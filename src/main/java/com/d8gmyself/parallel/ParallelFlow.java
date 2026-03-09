package com.d8gmyself.parallel;

import java.util.*;
import java.util.concurrent.*;

/**
 * <pre>
 * 异步编排，写操作慎用，更多的是用于数据的Fan-in/Fan-out
 *
 * 重点关注：
 * <li>关注TaskNode的行为中是否嵌套了ParallelFlow或者其它异步逻辑，避免线程池死锁</li>
 * <li>每个taskNode的默认超时时间在不指定的情况下为10秒</li>
 * <li>flow的默认超时时间在不指定的情况下为30秒</li>
 * <li>关于executor的选择，重点要关注RejectedExecutionHandler，如果选用DiscardPolicy这一类的，会导致TaskNode有可能被直接丢弃，完全靠flow超时兜底</li>
 * </pre>
 */
public class ParallelFlow {

    //静态方法默认使用
    private static final ParallelFlow DEFAULT = new ParallelFlow(ForkJoinPool.commonPool(), null, 0, 0);

    private final Executor executor;
    private final long flowTimeoutMs;
    private final long defaultTaskTimeoutMs;
    private final TaskLifecycleListener taskLifecycleListener;

    private ParallelFlow(Executor executor, TaskLifecycleListener listener, long defaultTaskTimeoutMs, long flowTimeoutMs) {
        if (defaultTaskTimeoutMs <= 0) {
            defaultTaskTimeoutMs = TimeUnit.SECONDS.toMillis(10);
        }
        if (flowTimeoutMs <= 0) {
            flowTimeoutMs = TimeUnit.SECONDS.toMillis(30);
        }
        if (executor == null) {
            executor = ForkJoinPool.commonPool();
        }
        this.defaultTaskTimeoutMs = defaultTaskTimeoutMs;
        this.flowTimeoutMs = flowTimeoutMs;
        this.executor = executor;
        this.taskLifecycleListener = listener;
    }

    // ======================== Static simple API ========================

    public static <O> O start(TaskNode<O> target) {
        return DEFAULT.run(target);
    }

    public static <O> O start(TaskNode<O> target, FlowContext ctx) {
        return DEFAULT.run(target, ctx);
    }

    // ======================== Static result API ========================

    public static <O> FlowResult<O> tryStart(TaskNode<O> target) {
        return DEFAULT.tryRun(target);
    }

    public static <O> FlowResult<O> tryStart(TaskNode<O> target, FlowContext ctx) {
        return DEFAULT.tryRun(target, ctx);
    }

    // ======================== Builder ========================

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Executor executor;
        private TaskLifecycleListener listener;
        private long defaultTaskTimeoutMs;
        private long flowTimeoutMs;

        public Builder executor(Executor executor) {
            this.executor = executor;
            return this;
        }

        public Builder listener(TaskLifecycleListener listener) {
            this.listener = listener;
            return this;
        }

        public Builder defaultTaskTimeoutMs(long defaultTaskTimeoutMs) {
            this.defaultTaskTimeoutMs = defaultTaskTimeoutMs;
            return this;
        }

        public Builder flowTimeout(long flowTimeoutMs) {
            this.flowTimeoutMs = flowTimeoutMs;
            return this;
        }

        public ParallelFlow build() {
            return new ParallelFlow(executor, listener, defaultTaskTimeoutMs, flowTimeoutMs);
        }
    }

    // ======================== Instance execute ========================

    public <O> O run(TaskNode<O> target) {
        return this.doRun(target, new FlowContext()).get();
    }

    public <O> O run(TaskNode<O> target, FlowContext ctx) {
        return this.doRun(target, ctx).get();
    }

    public <O> FlowResult<O> tryRun(TaskNode<O> target) {
        return this.doRun(target, new FlowContext());
    }

    public <O> FlowResult<O> tryRun(TaskNode<O> target, FlowContext ctx) {
        return this.doRun(target, ctx);
    }

    // ======================== Core engine ========================

    @SuppressWarnings("unchecked")
    private <O> FlowResult<O> doRun(TaskNode<O> target, FlowContext ctx) {

        // 前置校验，为了更语义化的NPE
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(ctx, "ctx");

        long startTime = System.currentTimeMillis();

        // 1. Collect all reachable nodes
        List<TaskNode<?>> allNodes = collectNodes(target);

        // 2. Topological sort (Kahn's algorithm)
        List<TaskNode<?>> sorted = topologicalSort(allNodes);

        // 3. Build TaskNodeFuture DAG
        Map<String, TaskNodeFuture<?>> futures = new LinkedHashMap<>();

        for (TaskNode<?> node : sorted) {
            TaskNodeFuture<?> future = buildFuture(node, ctx, futures);
            futures.put(node.getName(), future);
        }

        // 4. Wait for the target node
        O resultValue = null;
        Throwable exception = null;
        boolean success = false;

        CompletableFuture<?> targetFuture = futures.get(target.getName()).getFuture();
        try {
            resultValue = (O) targetFuture.get(flowTimeoutMs, TimeUnit.MILLISECONDS);
            success = true;
        } catch (TimeoutException e) {
            exception = new ParallelFlowException("Flow timed out after " + flowTimeoutMs + "ms");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof ParallelFlowException) {
                exception = cause;
            } else {
                exception = new ParallelFlowException(
                        "Task '" + target.getName() + "' failed", cause);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            exception = new ParallelFlowException("Execution interrupted", e);
        } catch (Throwable t) {
            if (t instanceof ParallelFlowException) {
                exception = t;
            } else if (t.getCause() instanceof ParallelFlowException) {
                exception = t.getCause();
            } else {
                exception = new ParallelFlowException("Task '" + target.getName() + "' failed", t);
            }
        }

        //如果target已经失败完成了，那所有futures都可以清除掉了，避免还未进入调度的node继续正常执行
        if (!success) {
            ParallelFlowException cancelEx = new ParallelFlowException(
                    "Cancelled: flow did not complete successfully", exception);
            for (TaskNodeFuture<?> taskNodeFuture : futures.values()) {
                taskNodeFuture.completeException(cancelEx);
            }
        }

        // 5. Snapshot all node states
        Map<String, NodeState> nodeStates = new LinkedHashMap<>();
        for (TaskNode<?> node : allNodes) {
            nodeStates.put(node.getName(), node.snapshotState());
        }

        // 6. Build mermaid
        String mermaid = buildMermaid(allNodes);

        long durationMs = System.currentTimeMillis() - startTime;
        return new FlowResult<>(success, resultValue, exception, nodeStates, mermaid, durationMs);
    }

    // ======================== Mermaid ========================

    private static String buildMermaid(List<TaskNode<?>> allNodes) {
        StringBuilder sb = new StringBuilder("graph BT\n");

        Set<String> failedNames = new LinkedHashSet<>();

        // 声明节点并分类
        for (TaskNode<?> node : allNodes) {
            String name = node.getName();
            sb.append("    ").append(name).append('\n');
            if (!node.isSuccess()) {
                failedNames.add(name);
            }
        }

        // 画边: dependency --> dependent
        for (TaskNode<?> node : allNodes) {
            for (TaskNode<?> dep : node.getDependencies()) {
                sb.append("    ").append(node.getName()).append(" --> ").append(dep.getName()).append('\n');
            }
            for (TaskNode<?> dep : node.getWeakDependencies()) {
                sb.append("    ").append(node.getName()).append(" -.-> ").append(dep.getName()).append('\n');
            }
        }

        boolean hasFailed = !failedNames.isEmpty();

        if (hasFailed) {
            sb.append("    classDef failed stroke:#ff0000\n");
        }
        for (String name : failedNames) {
            sb.append("    class ").append(name).append(" failed\n");
        }
        return sb.toString();
    }

    // ======================== Node collection ========================

    private static List<TaskNode<?>> collectNodes(TaskNode<?> target) {
        Map<String, TaskNode<?>> visited = new HashMap<>();
        List<TaskNode<?>> result = new ArrayList<>();
        collectDfs(target, visited, result);
        return result;
    }

    private static void collectDfs(TaskNode<?> node, Map<String, TaskNode<?>> visited, List<TaskNode<?>> result) {
        TaskNode<?> existing = visited.get(node.getName());
        if (existing != null) {
            if (existing != node) {
                throw new ParallelFlowException(
                        "Duplicate task name '" + node.getName() + "': two different TaskNode instances share the same name");
            }
            return;
        }
        visited.put(node.getName(), node);
        for (TaskNode<?> dep : node.getAllDependencies()) {
            collectDfs(dep, visited, result);
        }
        //TaskNode禁止复用
        if (!node.transfer2Used()) {
            throw new ParallelFlowException("Task '" + node.getName() + "' is already used, create new TaskNode per flow");
        }
        result.add(node);
    }

    // ======================== Topological sort ========================

    /**
     * DAG拓扑排序，确保某个node的dependencies一定在它前面
     * 带循环依赖检测
     */
    private static List<TaskNode<?>> topologicalSort(List<TaskNode<?>> nodes) {
        Map<String, TaskNode<?>> nodeMap = new HashMap<>();
        // inDegree: node -> number of unresolved prerequisites
        Map<String, Integer> inDegree = new HashMap<>();
        // successors: node -> list of nodes that depend on it
        Map<String, List<String>> successors = new HashMap<>();

        for (TaskNode<?> node : nodes) {
            String name = node.getName();
            nodeMap.put(name, node);
            inDegree.put(name, node.getAllDependencies().size());
            successors.computeIfAbsent(name, k -> new ArrayList<>());
            for (TaskNode<?> dep : node.getAllDependencies()) {
                successors.computeIfAbsent(dep.getName(), k -> new ArrayList<>()).add(name);
            }
        }

        // Kahn's BFS — O(V + E)
        Queue<String> queue = new ArrayDeque<>();
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                queue.add(entry.getKey());
            }
        }

        List<TaskNode<?>> sorted = new ArrayList<>();
        while (!queue.isEmpty()) {
            String name = queue.poll();
            sorted.add(nodeMap.get(name));

            for (String succ : successors.get(name)) {
                int remaining = inDegree.get(succ) - 1;
                inDegree.put(succ, remaining);
                if (remaining == 0) {
                    queue.add(succ);
                }
            }
        }

        if (sorted.size() != nodes.size()) {
            throw new ParallelFlowException("Circular dependency detected in task graph");
        }
        return sorted;
    }

    // ======================== Future building ========================

    @SuppressWarnings({"unchecked", "rawtypes"})
    private TaskNodeFuture<?> buildFuture(
            TaskNode<?> node, FlowContext ctx,
            Map<String, TaskNodeFuture<?>> futures) {

        Collection<TaskNode<?>> deps = node.getAllDependencies();

        TaskNodeFuture<?> taskNodeFuture = new TaskNodeFuture((TaskNode) node, defaultTaskTimeoutMs, taskLifecycleListener);
        if (deps.isEmpty()) {
            try {
                executor.execute(() -> taskNodeFuture.execute(ctx));
            } catch (Throwable throwable) {
                //线程池拒绝兜底
                taskNodeFuture.completeException(throwable);
            }
            return taskNodeFuture;
        }

        List<CompletableFuture<?>> depFutures = new ArrayList<>();
        for (TaskNode<?> dep : deps) {
            CompletableFuture<?> depFuture = futures.get(dep.getName()).getFuture();
            if (!node.isRequired(dep)) {
                //如果是弱依赖，那忽略异常
                depFuture = depFuture.handle((result, ex) -> result);
            } else {
                //如果是强依赖失败，直接complete taskNodeFuture
                //如果A强依赖B弱依赖C，那只要B抛异常，那A直接按异常完成，不等C
                depFuture = depFuture.exceptionally(throwable -> {
                    taskNodeFuture.completeByRequiredFail(dep.getName(), throwable);
                    return null;
                });
            }
            depFutures.add(depFuture);
        }

        CompletableFuture<?>[] depArray = depFutures.toArray(new CompletableFuture<?>[0]);
        CompletableFuture<Void> allDeps = CompletableFuture.allOf(depArray);
        allDeps.thenRunAsync(() -> taskNodeFuture.execute(ctx), executor)
                .exceptionally(throwable -> {
                    taskNodeFuture.completeFailure(0, 0, throwable, 0);
                    return null;
                });
        return taskNodeFuture;
    }
}
