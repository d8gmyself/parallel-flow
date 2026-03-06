package com.d8gmyself.parallel;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * <pre>
 * 异步编排，写操作慎用，更多的是用于数据的Fan-in/Fan-out
 *
 * 重点关注：
 * <li>关注TaskNode的行为中是否嵌套了ParallelFlow或者其它异步逻辑，避免线程池死锁</li>
 * </pre>
 */
public class ParallelFlow {

    private final TaskNodeExecutor nodeExecutor;
    private final Executor executor;
    private final long flowTimeoutMs;

    private ParallelFlow(Executor executor, TaskLifecycleListener listener, long defaultTaskTimeoutMs, long flowTimeoutMs) {
        this.nodeExecutor = new TaskNodeExecutor(listener, defaultTaskTimeoutMs);
        this.executor = executor;
        this.flowTimeoutMs = flowTimeoutMs;
    }

    // ======================== Static simple API ========================

    public static <O> O execute(TaskNode<O> target) {
        return execute(target, null, null, 0, null);
    }

    public static <O> O execute(TaskNode<O> target, FlowContext ctx) {
        return execute(target, ctx, null, 0, null);
    }

    public static <O> O execute(TaskNode<O> target, Executor executor, long defaultTaskTimeoutMs) {
        return execute(target, null, executor, defaultTaskTimeoutMs, null);
    }

    public static <O> O execute(TaskNode<O> target, Executor executor, TaskLifecycleListener listener) {
        return execute(target, null, executor, 0, listener);
    }

    public static <O> O execute(TaskNode<O> target, FlowContext ctx, Executor executor, long defaultTaskTimeoutMs, TaskLifecycleListener listener) {
        return executeForResult(target, ctx, executor, defaultTaskTimeoutMs, listener).get();
    }

    // ======================== Static result API ========================

    public static <O> FlowResult<O> executeForResult(TaskNode<O> target) {
        return executeForResult(target, null, null, 0, null);
    }

    public static <O> FlowResult<O> executeForResult(TaskNode<O> target, FlowContext ctx) {
        return executeForResult(target, ctx, null, 0, null);
    }

    public static <O> FlowResult<O> executeForResult(TaskNode<O> target, Executor executor, long defaultTaskTimeoutMs) {
        return executeForResult(target, null, executor, defaultTaskTimeoutMs, null);
    }

    public static <O> FlowResult<O> executeForResult(TaskNode<O> target, Executor executor, TaskLifecycleListener listener) {
        return executeForResult(target, null, executor, 0, listener);
    }

    public static <O> FlowResult<O> executeForResult(TaskNode<O> target, FlowContext ctx, Executor executor, long defaultTaskTimeoutMs, TaskLifecycleListener listener) {
        if (target == null) {
            return new FlowResult<>(true, null, null, Collections.emptyMap(), "", 0);
        }
        executor = executor == null ? ForkJoinPool.commonPool() : executor;
        defaultTaskTimeoutMs = Math.max(0, defaultTaskTimeoutMs);
        ParallelFlow flow = new ParallelFlow(executor, listener, defaultTaskTimeoutMs, 0);
        ctx = ctx == null ? new FlowContext() : ctx;
        return flow.doRun(target, ctx);
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
            Executor exec = executor != null ? executor : ForkJoinPool.commonPool();
            return new ParallelFlow(exec, listener, defaultTaskTimeoutMs, flowTimeoutMs);
        }
    }

    // ======================== Instance execute ========================

    public <O> O run(TaskNode<O> target) {
        return this.doRun(target, new FlowContext()).get();
    }

    public <O> O run(TaskNode<O> target, FlowContext ctx) {
        return this.doRun(target, ctx).get();
    }

    public <O> FlowResult<O> runForResult(TaskNode<O> target) {
        return this.doRun(target, new FlowContext());
    }

    public <O> FlowResult<O> runForResult(TaskNode<O> target, FlowContext ctx) {
        return this.doRun(target, ctx);
    }

    // ======================== Core engine ========================

    @SuppressWarnings("unchecked")
    private <O> FlowResult<O> doRun(TaskNode<O> target, FlowContext ctx) {
        long startTime = System.currentTimeMillis();

        // 1. Collect all reachable nodes
        List<TaskNode<?>> allNodes = collectNodes(target);

        // 2. Topological sort (Kahn's algorithm)
        List<TaskNode<?>> sorted = topologicalSort(allNodes);

        // 3. Build CompletableFuture DAG
        Map<String, CompletableFuture<?>> futures = new LinkedHashMap<>();

        for (TaskNode<?> node : sorted) {
            CompletableFuture<?> future = buildFuture(node, ctx, futures);
            futures.put(node.getName(), future);
        }

        // 4. Wait for the target node
        O resultValue = null;
        Throwable exception = null;
        boolean success = false;

        CompletableFuture<?> targetFuture = futures.get(target.getName());
        try {
            if (flowTimeoutMs > 0) {
                resultValue = (O) targetFuture.get(flowTimeoutMs, TimeUnit.MILLISECONDS);
            } else {
                resultValue = (O) targetFuture.get();
            }
            success = true;
        } catch (TimeoutException e) {
            exception = new ParallelFlowException("Flow timed out after " + flowTimeoutMs + "ms");
            for (TaskNode<?> node : allNodes) {
                node.completeTimeout(0, exception);
            }
        } catch (java.util.concurrent.ExecutionException e) {
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
        StringBuilder sb = new StringBuilder("graph TD\n");

        Set<String> optionalNames = new LinkedHashSet<>();
        Set<String> failedNames = new LinkedHashSet<>();
        Set<String> optionalFailedNames = new LinkedHashSet<>();

        // 声明节点并分类
        for (TaskNode<?> node : allNodes) {
            String name = node.getName();
            sb.append("    ").append(name).append('\n');

            boolean optional = node.isOptional();
            boolean failed = !node.isSuccess();

            if (optional && failed) {
                optionalFailedNames.add(name);
            } else if (optional) {
                optionalNames.add(name);
            } else if (failed) {
                failedNames.add(name);
            }
        }

        // 画边: dependency --> dependent
        for (TaskNode<?> node : allNodes) {
            for (TaskNode<?> dep : node.getDependencies()) {
                sb.append("    ").append(dep.getName()).append(" --> ").append(node.getName()).append('\n');
            }
        }

        // classDef 和 class 分配
        boolean hasOptional = !optionalNames.isEmpty() || !optionalFailedNames.isEmpty();
        boolean hasFailed = !failedNames.isEmpty() || !optionalFailedNames.isEmpty();

        if (hasOptional) {
            sb.append("    classDef optional stroke-dasharray: 5 5\n");
        }
        if (hasFailed) {
            sb.append("    classDef failed stroke:#ff0000\n");
        }
        if (!optionalFailedNames.isEmpty()) {
            sb.append("    classDef optionalFailed stroke:#ff0000,stroke-dasharray: 5 5\n");
        }

        for (String name : optionalNames) {
            sb.append("    class ").append(name).append(" optional\n");
        }
        for (String name : failedNames) {
            sb.append("    class ").append(name).append(" failed\n");
        }
        for (String name : optionalFailedNames) {
            sb.append("    class ").append(name).append(" optionalFailed\n");
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
        //TaskNode禁止复用
        if (node.isCompleted()) {
            throw new ParallelFlowException("Task '" + node.getName() + "' is completed, create new TaskNode per flow");
        }
        TaskNode<?> existing = visited.get(node.getName());
        if (existing != null) {
            if (existing != node) {
                throw new ParallelFlowException(
                        "Duplicate task name '" + node.getName() + "': two different TaskNode instances share the same name");
            }
            return;
        }
        visited.put(node.getName(), node);
        for (TaskNode<?> dep : node.getDependencies()) {
            collectDfs(dep, visited, result);
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
            inDegree.put(name, node.getDependencies().size());
            successors.computeIfAbsent(name, k -> new ArrayList<>());
            for (TaskNode<?> dep : node.getDependencies()) {
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
    private CompletableFuture<?> buildFuture(
            TaskNode<?> node, FlowContext ctx,
            Map<String, CompletableFuture<?>> futures) {

        Collection<TaskNode<?>> deps = node.getDependencies();

        CompletableFuture<?> resultFuture = new CompletableFuture<>();

        if (deps.isEmpty()) {
            executor.execute(() -> nodeExecutor.execute((TaskNode) node, ctx, resultFuture));
            return resultFuture;
        }

        List<CompletableFuture<?>> depFutures = new ArrayList<>();
        for (TaskNode<?> dep : deps) {
            CompletableFuture<?> depFuture = futures.get(dep.getName());
            if (dep.isOptional()) {
                //如果是弱依赖，那忽略异常
                depFuture = depFuture.handle((result, ex) -> result);
            }
            depFutures.add(depFuture);
        }

        CompletableFuture<?>[] depArray = depFutures.toArray(new CompletableFuture<?>[0]);
        CompletableFuture<Void> allDeps = CompletableFuture.allOf(depArray);
        //如果allDeps中存在没有fallback的强依赖抛出异常，为了确保resultFuture可以被complete，不阻断后续流程，这里需要处理exceptionally逻辑
        allDeps.exceptionally(throwable -> {
                    if (node.completeFailure(0, 0, throwable)) {
                        resultFuture.completeExceptionally(throwable);
                    }
                    return null;
                })
                .thenRunAsync(() -> nodeExecutor.execute((TaskNode) node, ctx, resultFuture), executor);
        return resultFuture;
    }
}
