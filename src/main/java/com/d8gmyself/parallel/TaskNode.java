package com.d8gmyself.parallel;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * <pre>
 * 并行任务节点，包括元信息和运行状态，每次执行前应该新建，禁止复用
 * 元数据通过Builder构建，构建后不可变；运行时状态由executor写入
 * 状态数据仅可写入一次，比如先超时，后完成，那完成的结果也直接丢弃，最终按超时记
 * 如果一个TaskNode同时存在dependencies和weakDependencies中，那优先认为是强依赖
 * 如果action或者fallback中有长IO操作，自行控制IO的超时逻辑，taskNode的timeout机制不会进行interrupt，避免误打断
 *
 * 关于成功的判定：仅在抛出异常的时候才算失败，核心其实就两种失败情况
 *   - 超时 & 无超时默认值
 *   - action失败 & (无fallback | fallback失败)
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

    // ======================== Definition (immutable) ========================

    private final String name;
    private final ContextFunction<O> action;
    private final long timeoutMs;
    private final int maxRetries;
    private final FallbackFunction<O> fallback;
    private final boolean hasTimeoutDefault;
    private final O timeoutDefaultValue;
    private final Collection<TaskNode<?>> dependencies;
    private final Collection<TaskNode<?>> weakDependencies;
    private final Collection<TaskNode<?>> allDependencies;
    private final CircuitBreaker circuitBreaker;

    // ======================== Runtime state (written once by executor or timeout thread) ========================

    /**
     * 新建，未绑定到任何flow中
     */
    private static final int INIT_FLAG = 0;
    /**
     * 已经被某个flow流程扫描绑定
     */
    private static final int USED_FLAG = 1;
    /**
     * 已经执行完，正在写入结果状态
     */
    private static final int RESULT_WRITING_FLAG = 11;
    /**
     * 已完成
     */
    private static final int COMPLETED_FLAG = 12;
    /**
     * 用于保证状态可见性+禁止复用，CAS直接控制
     */
    private final AtomicInteger stateFlag = new AtomicInteger(INIT_FLAG);
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
        this.timeoutMs = b.timeoutMs;
        this.maxRetries = b.maxRetries;
        this.fallback = b.fallback;
        this.hasTimeoutDefault = b.hasTimeoutDefault;
        this.timeoutDefaultValue = b.timeoutDefaultValue;
        HashSet<TaskNode<?>> strongDeps = new HashSet<>(b.dependencies);
        HashSet<TaskNode<?>> weakDeps = new HashSet<>(b.weakDependencies);
        weakDeps.removeAll(strongDeps);
        this.dependencies = Collections.unmodifiableCollection(strongDeps);
        this.weakDependencies = Collections.unmodifiableCollection(weakDeps);
        HashSet<TaskNode<?>> allDependencies = new HashSet<>(strongDeps);
        allDependencies.addAll(weakDeps);
        this.allDependencies = Collections.unmodifiableCollection(allDependencies);
        this.circuitBreaker = b.circuitBreaker;
    }

    // ======================== Factory ========================

    /**
     * 快捷构建，等价于 {@code TaskNode.<O>builder(name, action).build()}
     */
    public static <O> TaskNode<O> of(String name, ContextFunction<O> action) {
        return TaskNode.builder(name, action).build();
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

        private long timeoutMs;
        private int maxRetries;
        private FallbackFunction<O> fallback;
        private boolean hasTimeoutDefault;
        private O timeoutDefaultValue;
        private final Collection<TaskNode<?>> dependencies = new HashSet<>();
        private final Collection<TaskNode<?>> weakDependencies = new HashSet<>();
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

        public Builder<O> dependsOn(TaskNode<?>... nodes) {
            for (TaskNode<?> node : nodes) {
                if (node == null) {
                    throw new IllegalArgumentException("dependency must not be null");
                }
                this.dependencies.add(node);
            }
            return this;
        }

        public Builder<O> weakDependsOn(TaskNode<?>... nodes) {
            for (TaskNode<?> node : nodes) {
                if (node == null) {
                    throw new IllegalArgumentException("dependency must not be null");
                }
                this.weakDependencies.add(node);
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

    /**
     * 强依赖时获取结果
     */
    public O get() {
        if (!isCompleted()) {
            throw new ParallelFlowException("Task '" + name + "' has not been executed");
        }
        if (!success) {
            if (exception instanceof ParallelFlowException) {
                throw (ParallelFlowException) exception;
            }
            throw new ParallelFlowException("Task '" + name + "' has not completed successfully", exception);
        }
        return resultValue;
    }

    /**
     * 弱依赖时获取结果
     */
    public O orElse(O defaultValue) {
        if (!isCompleted() || !success) {
            return defaultValue;
        }
        return resultValue;
    }

    public boolean isSuccess() {
        return isCompleted() && success;
    }

    // ======================== stateFlag 状态机管理，就这几个状态，不再复杂化 ========================

    boolean transfer2Used() {
        return stateFlag.compareAndSet(INIT_FLAG, USED_FLAG);
    }

    boolean transfer2ResultWriting() {
        return stateFlag.compareAndSet(USED_FLAG, RESULT_WRITING_FLAG);
    }

    boolean transfer2Completed() {
        return stateFlag.compareAndSet(RESULT_WRITING_FLAG, COMPLETED_FLAG);
    }

    boolean isCompleted() {
        return stateFlag.get() == COMPLETED_FLAG;
    }

    // ======================== Completion methods (package-private, only called by TaskNodeFuture) ========================

    boolean completionClaimed() {
        return stateFlag.get() >= RESULT_WRITING_FLAG;
    }

    boolean completeSuccess(O value, int retryCount, long durationMs) {
        if (!stateFlag.compareAndSet(USED_FLAG, RESULT_WRITING_FLAG)) {
            return false;
        }
        this.resultValue = value;
        this.success = true;
        this.actualRetryCount = retryCount;
        this.durationMs = durationMs;
        this.stateFlag.set(COMPLETED_FLAG);
        return true;
    }

    boolean completeFallback(O value, int retryCount, long durationMs, Throwable exception) {
        if (!transfer2ResultWriting()) {
            return false;
        }
        this.resultValue = value;
        this.success = true;
        this.fallbackUsed = true;
        this.actualRetryCount = retryCount;
        this.durationMs = durationMs;
        this.exception = exception;
        return transfer2Completed();
    }

    boolean completeFailure(int retryCount, long durationMs, Throwable exception) {
        if (!transfer2ResultWriting()) {
            return false;
        }
        this.success = false;
        this.actualRetryCount = retryCount;
        this.durationMs = durationMs;
        this.exception = exception;
        return transfer2Completed();
    }

    boolean completeTimeout(long durationMs, Throwable exception) {
        if (!transfer2ResultWriting()) {
            return false;
        }
        this.success = false;
        this.timedOut = true;
        this.durationMs = durationMs;
        this.exception = exception;
        return transfer2Completed();
    }

    boolean completeTimeoutDefault(long durationMs, Throwable exception) {
        if (!transfer2ResultWriting()) {
            return false;
        }
        this.resultValue = this.timeoutDefaultValue;
        this.success = true;
        this.fallbackUsed = true;
        this.timedOut = true;
        this.durationMs = durationMs;
        this.exception = exception;
        return transfer2Completed();
    }

    // ======================== Definition getters ========================

    public String getName() {
        return name;
    }

    ContextFunction<O> getAction() {
        return action;
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

    Collection<TaskNode<?>> getWeakDependencies() {
        return weakDependencies;
    }

    Collection<TaskNode<?>> getAllDependencies() {
        return allDependencies;
    }

    boolean isRequired(TaskNode<?> depNode) {
        return dependencies.contains(depNode);
    }

    CircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    NodeState snapshotState() {
        return new NodeState(name, success, fallbackUsed, timedOut, actualRetryCount, durationMs, exception);
    }

    @Override
    public String toString() {
        return "TaskNode{" + name + "}";
    }

}
