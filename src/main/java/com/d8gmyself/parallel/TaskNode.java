package com.d8gmyself.parallel;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * <pre>
 * 并行任务节点，包括元信息和运行状态，每次执行前应该新建，禁止复用
 * 元数据通过Builder构建，构建后不可变；运行时状态由executor写入
 * 关于强弱依赖的表达，统一在节点上做表达，而不是边上，因为：
 *     C 强依赖 A
 *     B 弱依赖 A
 * 最终结果只要强依赖C，那A节点就是强依赖的，区分边级别的依赖意义不大，不如简单提现整个flow维度对当前TaskNode是强依赖还是弱依赖
 * 如果A是强依赖，B、C是弱依赖，B、C都弱依赖A，最终结果依赖B+C，这种情况下，只要最终结果不在依赖中明确依赖A，那理论上不会影响整个流程
 * 简单来说就是，表达在节点上的强弱依赖，在传递依赖和直接依赖两种情况下，含义不同
 * </pre>
 * @param <O> 节点返回值
 */
public class TaskNode<O> {

    @FunctionalInterface
    public interface ContextFunction<R> {
        R apply(FlowContext ctx) throws Exception;
    }

    @FunctionalInterface
    public interface FallbackFunction<R> {
        R apply(Throwable ex) throws Exception;
    }

    public enum DependencyType {
        REQUIRED,
        OPTIONAL
    }

    // ======================== Definition (immutable) ========================

    private final String name;
    private final ContextFunction<O> action;
    private final DependencyType dependencyType;
    private final long timeoutMs;
    private final int maxRetries;
    private final FallbackFunction<O> fallback;
    private final boolean hasTimeoutDefault;
    private final O timeoutDefaultValue;
    private final Collection<TaskNode<?>> dependencies;
    private final CircuitBreaker circuitBreaker;

    // ======================== Runtime state (written once by executor or timeout thread) ========================

    /**
     * 状态只能写入一次，通过该字段CAS控制，谁抢到，谁可以写
     */
    private final AtomicBoolean completed = new AtomicBoolean(false);
    /**
     * 用于保证状态可见性，默认false，task完成后，在更新完所有状态后，记得execute设置为true
     */
    private volatile boolean executed;
    private volatile boolean success;
    private volatile boolean fallbackUsed;
    private volatile boolean timedOut;
    private volatile int actualRetryCount;
    private volatile long durationMs;
    private volatile O resultValue;
    private volatile Throwable exception;

    private TaskNode(Builder<O> b) {
        this.name = b.name;
        this.action = b.action;
        this.dependencyType = b.dependencyType;
        this.timeoutMs = b.timeoutMs;
        this.maxRetries = b.maxRetries;
        this.fallback = b.fallback;
        this.hasTimeoutDefault = b.hasTimeoutDefault;
        this.timeoutDefaultValue = b.timeoutDefaultValue;
        this.dependencies = Collections.unmodifiableCollection(new HashSet<>(b.dependencies));
        this.circuitBreaker = b.circuitBreaker;
    }

    // ======================== Factory ========================

    /**
     * 快捷构建，等价于 {@code TaskNode.<O>builder(name, action).build()}
     */
    public static <O> TaskNode<O> of(String name, ContextFunction<O> action) {
        return TaskNode.<O>builder(name, action).build();
    }

    public static <O> Builder<O> builder(String name, ContextFunction<O> action) {
        if (name == null || action == null) {
            throw new IllegalArgumentException("name and action must not be null");
        }
        return new Builder<>(name, action);
    }

    // ======================== Builder ========================

    public static class Builder<O> {
        private final String name;
        private final ContextFunction<O> action;

        private DependencyType dependencyType = DependencyType.REQUIRED;
        private long timeoutMs;
        private int maxRetries;
        private FallbackFunction<O> fallback;
        private boolean hasTimeoutDefault;
        private O timeoutDefaultValue;
        private final Collection<TaskNode<?>> dependencies = new HashSet<>();
        private CircuitBreaker circuitBreaker;

        private Builder(String name, ContextFunction<O> action) {
            this.name = name;
            this.action = action;
        }

        /**
         * 设置taskNode的超时时间，这里的超时时间指taskNode内部逻辑执行的超时时间，而不是taskNode在整个Flow中的超时时间
         * Flow的超时时间由Flow自己的flowTimeoutMs控制
         */
        public Builder<O> timeout(long timeoutMs) {
            this.timeoutMs = Math.max(0, timeoutMs);
            return this;
        }

        public Builder<O> retry(int maxRetries) {
            this.maxRetries = Math.max(0, maxRetries);
            return this;
        }

        public Builder<O> fallback(FallbackFunction<O> fallback) {
            this.fallback = fallback;
            return this;
        }

        public Builder<O> timeoutDefault(O defaultValue) {
            this.hasTimeoutDefault = true;
            this.timeoutDefaultValue = defaultValue;
            return this;
        }

        public Builder<O> required() {
            this.dependencyType = DependencyType.REQUIRED;
            return this;
        }

        public Builder<O> optional() {
            return optional(null);
        }

        /**
         * <pre>
         * 标记为弱依赖
         * 在没有单独设置fallback，设置defaultValue为fallback的结果
         * 在没有单独设置timeoutDefault的情况下，设置defaultValue为timeoutDefaultValue
         * </pre>
         */
        public Builder<O> optional(O defaultValue) {
            this.dependencyType = DependencyType.OPTIONAL;
            if (this.fallback == null) {
                this.fallback = t -> defaultValue;
            }
            if (!this.hasTimeoutDefault) {
                this.hasTimeoutDefault = true;
                this.timeoutDefaultValue = defaultValue;
            }
            return this;
        }

        public Builder<O> dependsOn(TaskNode<?>... nodes) {
            for (TaskNode<?> node : nodes) {
                if (node == null) {
                    throw new IllegalArgumentException("dependency must not be null");
                }
                this.dependencies.add(node);
            }
            return this;
        }

        public Builder<O> circuitBreaker(CircuitBreaker circuitBreaker) {
            this.circuitBreaker = circuitBreaker;
            return this;
        }

        public TaskNode<O> build() {
            return new TaskNode<>(this);
        }
    }

    // ======================== Result access (public) ========================

    public O getResult() {
        if (!executed) {
            throw new ParallelFlowException("Task '" + name + "' has not been executed");
        }
        if (!success) {
            if (isOptional()) {
                return null;
            }
            if (exception instanceof ParallelFlowException) {
                throw (ParallelFlowException) exception;
            }
            throw new ParallelFlowException("Task '" + name + "' has not completed successfully", exception);
        }
        return resultValue;
    }

    public O getResultOrDefault(O defaultValue) {
        if (!executed || !success) {
            return defaultValue;
        }
        return resultValue != null ? resultValue : defaultValue;
    }

    public boolean isSuccess() {
        return executed && success;
    }

    // ======================== Completion methods (package-private, only called by TaskNodeFuture) ========================

    boolean isCompleted() {
        return completed.get();
    }

    boolean completeSuccess(O value, int retryCount, long durationMs) {
        if (!completed.compareAndSet(false, true)) {
            return false;
        }
        this.resultValue = value;
        this.success = true;
        this.actualRetryCount = retryCount;
        this.durationMs = durationMs;
        this.executed = true;
        return true;
    }

    boolean completeFallback(O value, int retryCount, long durationMs, Throwable exception) {
        if (!completed.compareAndSet(false, true)) {
            return false;
        }
        this.resultValue = value;
        this.success = true;
        this.fallbackUsed = true;
        this.actualRetryCount = retryCount;
        this.durationMs = durationMs;
        this.exception = exception;
        this.executed = true;
        return true;
    }

    boolean completeFailure(int retryCount, long durationMs, Throwable exception) {
        if (!completed.compareAndSet(false, true)) {
            return false;
        }
        this.success = false;
        this.actualRetryCount = retryCount;
        this.durationMs = durationMs;
        this.exception = exception;
        this.executed = true;
        return true;
    }

    boolean completeTimeout(long durationMs, Throwable exception) {
        if (!completed.compareAndSet(false, true)) {
            return false;
        }
        this.success = false;
        this.timedOut = true;
        this.durationMs = durationMs;
        this.exception = exception;
        this.executed = true;
        return true;
    }

    boolean completeTimeoutDefault(long durationMs, Throwable exception) {
        if (!completed.compareAndSet(false, true)) {
            return false;
        }
        this.resultValue = this.timeoutDefaultValue;
        this.success = true;
        this.fallbackUsed = true;
        this.timedOut = true;
        this.durationMs = durationMs;
        this.exception = exception;
        this.executed = true;
        return true;
    }

    // ======================== Definition getters ========================

    public String getName() {
        return name;
    }

    ContextFunction<O> getAction() {
        return action;
    }

    DependencyType getDependencyType() {
        return dependencyType;
    }

    boolean isOptional() {
        return dependencyType == DependencyType.OPTIONAL;
    }

    long getTimeoutMs() {
        return timeoutMs;
    }

    int getMaxRetries() {
        return maxRetries;
    }

    FallbackFunction<O> getFallback() {
        return fallback;
    }

    boolean hasTimeoutDefault() {
        return hasTimeoutDefault;
    }

    O getTimeoutDefaultValue() {
        return timeoutDefaultValue;
    }

    Collection<TaskNode<?>> getDependencies() {
        return dependencies;
    }

    CircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    NodeState snapshotState() {
        return new NodeState(name, executed, success, isOptional(),
                fallbackUsed, timedOut, actualRetryCount, durationMs, exception);
    }

    @Override
    public String toString() {
        return "TaskNode{" + name + "}";
    }

}
