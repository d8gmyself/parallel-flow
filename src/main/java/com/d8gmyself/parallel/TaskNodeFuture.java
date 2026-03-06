package com.d8gmyself.parallel;

import java.util.concurrent.*;

class TaskNodeFuture<O> {

    private static final ScheduledExecutorService SCHEDULER =
            Executors.newScheduledThreadPool(1, r -> {
                Thread t = new Thread(r, "parallel-flow-timeout");
                t.setDaemon(true);
                return t;
            });

    private final TaskLifecycleListener listener;
    private final long defaultTimeoutMs;
    private final CompletableFuture<O> future;
    private final TaskNode<O> taskNode;

    TaskNodeFuture(TaskNode<O> taskNode, long defaultTimeoutMs, TaskLifecycleListener listener) {
        this.taskNode = taskNode;
        this.listener = listener;
        this.defaultTimeoutMs = defaultTimeoutMs;
        this.future = new CompletableFuture<>();
    }

    // ======================== future execute helpers ========================

    void execute(FlowContext ctx) {
        //提前被异常链路阻断，没必要继续执行了，典型的比如当前future的强依赖发生exception，那当前future已经提前被completeExceptionally了
        if (future.isDone()) {
            return;
        }
        registerTimeout();
        executeWithRetry(ctx);
    }

    private void registerTimeout() {
        // Timeout handling: node-level timeout > flow-level defaultTaskTimeoutMs
        long effectiveTimeout = taskNode.getTimeoutMs() > 0 ? taskNode.getTimeoutMs() : defaultTimeoutMs;
        if (effectiveTimeout > 0) {
            final ScheduledFuture<?> timer = SCHEDULER.schedule(() -> {
                if (taskNode.isCompleted()) {
                    return;
                }
                ParallelFlowException timeoutEx = new ParallelFlowException(
                        "Task '" + taskNode.getName() + "' timed out after "
                                + effectiveTimeout + "ms",
                        new TimeoutException());

                if (taskNode.hasTimeoutDefault()) {
                    completeTimeoutDefault(effectiveTimeout, timeoutEx);
                    return;
                }
                completeTimeout(effectiveTimeout, timeoutEx);
            }, effectiveTimeout, TimeUnit.MILLISECONDS);
            future.whenComplete((r, e) -> timer.cancel(false));
        }
    }

    private void executeWithRetry(FlowContext ctx) {
        if (isCompleted()) {
            return;
        }
        long taskStart = System.currentTimeMillis();
        int maxAttempts = taskNode.getMaxRetries() + 1;
        int retryCount = 0;
        int attemptsMade = 0;
        Throwable lastException = null;
        CircuitBreaker cb = taskNode.getCircuitBreaker();

        fireEvent(taskNode.getName(), 0, 0, null, null, EventType.START);

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            if (isCompleted()) {
                return;
            }

            retryCount = attempt - 1;
            attemptsMade = attempt;

            // Check circuit breaker

            if (cb != null && !cb.allowRequest(taskNode.getName())) {
                lastException = new ParallelFlowException(
                        "Circuit breaker is open for task '" + taskNode.getName() + "'");
                break;
            }

            try {
                O result = taskNode.getAction().apply(ctx);

                if (cb != null) {
                    cb.recordSuccess(taskNode.getName());
                }

                long duration = System.currentTimeMillis() - taskStart;
                completeSuccess(result, retryCount, duration, attempt);
                return;
            } catch (Throwable t) {
                lastException = t;

                if (cb != null) {
                    cb.recordFailure(taskNode.getName());
                }

                if (attempt < maxAttempts && !taskNode.isCompleted()) {
                    long duration = System.currentTimeMillis() - taskStart;
                    fireEvent(taskNode.getName(), duration, attempt, t, null, EventType.RETRY);
                }
            }
        }

        // All attempts exhausted — try fallback
        if (taskNode.getFallback() != null) {
            try {
                O fallbackResult = taskNode.getFallback().apply(lastException);

                long duration = System.currentTimeMillis() - taskStart;
                completeFallback(fallbackResult, retryCount, duration, lastException, attemptsMade);
                return;
            } catch (Throwable fallbackEx) {
                if (lastException != null && lastException != fallbackEx) {
                    fallbackEx.addSuppressed(lastException);
                }
                lastException = fallbackEx;
            }
        }

        // Final failure
        long duration = System.currentTimeMillis() - taskStart;
        completeFailure(retryCount, duration, lastException, attemptsMade);
    }

    // ======================== future helpers ========================

    boolean isCompleted() {
        return taskNode.isCompleted();
    }

    boolean completeSuccess(O value, int retryCount, long durationMs, int attempt) {
        if (taskNode.completeSuccess(value, retryCount, durationMs)) {
            future.complete(value);
            fireEvent(taskNode.getName(), durationMs, attempt, null, value, EventType.SUCCESS);
            return true;
        }
        return false;
    }

    boolean completeFallback(O value, int retryCount, long durationMs, Throwable exception, int attempt) {
        if (taskNode.completeFallback(value, retryCount, durationMs, exception)) {
            future.complete(value);
            fireEvent(taskNode.getName(), durationMs, attempt, exception, value, EventType.FALLBACK);
            return true;
        }
        return false;
    }

    boolean completeFailure(int retryCount, long durationMs, Throwable exception, int attempt) {
        if (taskNode.completeFailure(retryCount, durationMs, exception)) {
            ParallelFlowException failEx = new ParallelFlowException(
                    "Task '" + taskNode.getName() + "' failed after "
                            + retryCount + " retries", exception);
            future.completeExceptionally(failEx);
            fireEvent(taskNode.getName(), durationMs, attempt, exception, null, EventType.FAILURE);
            return true;
        }
        return false;
    }

    boolean completeTimeout(long durationMs, Throwable exception) {
        if (taskNode.completeTimeout(durationMs, exception)) {
            future.completeExceptionally(exception);
            fireEvent(taskNode.getName(), durationMs, 0, exception, null, EventType.FAILURE);
            return true;
        }
        return false;
    }

    boolean completeTimeoutDefault(long durationMs, Throwable exception) {
        if (taskNode.completeTimeoutDefault(durationMs, exception)) {
            future.complete(taskNode.getTimeoutDefaultValue());
            fireEvent(taskNode.getName(), durationMs, 0, exception, taskNode.getTimeoutDefaultValue(), EventType.FALLBACK);
            return true;
        }
        return false;
    }

    /**
     * 仅用在Flow维度超时以及线程池饱和策略下的complete
     */
    boolean completeException(Throwable exception) {
        if (taskNode.hasTimeoutDefault()) {
            return completeTimeoutDefault(0, exception);
        }
        return completeFailure(0, 0, exception, 0);
    }

    CompletableFuture<O> getFuture() {
        return future;
    }

    // ======================== Listener helpers ========================

    enum EventType { START, SUCCESS, FAILURE, RETRY, FALLBACK }

    private void fireEvent(String taskName, long durationMs, int attempt,
                           Throwable exception, Object result, EventType type) {
        if (listener == null) {
            return;
        }
        TaskLifecycleListener.TaskEvent event =
                new TaskLifecycleListener.TaskEvent(taskName, durationMs, attempt, exception, result);
        try {
            switch (type) {
                case START:
                    listener.onStart(event);
                    break;
                case SUCCESS:
                    listener.onSuccess(event);
                    break;
                case FAILURE:
                    listener.onFailure(event);
                    break;
                case RETRY:
                    listener.onRetry(event);
                    break;
                case FALLBACK:
                    listener.onFallback(event);
                    break;
            }
        } catch (Throwable ignored) {
            // Listener errors must not break execution
        }
    }
}
