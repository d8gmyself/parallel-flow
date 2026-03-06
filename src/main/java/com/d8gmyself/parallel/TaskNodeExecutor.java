package com.d8gmyself.parallel;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class TaskNodeExecutor {

    private static final ScheduledExecutorService SCHEDULER =
            Executors.newScheduledThreadPool(1, r -> {
                Thread t = new Thread(r, "parallel-flow-timeout");
                t.setDaemon(true);
                return t;
            });

    private final TaskLifecycleListener listener;
    private final long defaultTimeoutMs;

    TaskNodeExecutor(TaskLifecycleListener listener, long defaultTimeoutMs) {
        this.listener = listener;
        this.defaultTimeoutMs = defaultTimeoutMs;
    }

    <O> void execute(TaskNode<O> node, FlowContext ctx, CompletableFuture<O> future) {
        //提前被异常链路阻断，没必要继续执行了，典型的比如当前future的强依赖发生exception，那当前future已经提前被completeExceptionally了
        if (future.isDone()) {
            return;
        }
        registerTimeout(node, future);
        executeWithRetry(node, ctx, future);
    }

    private <O> void registerTimeout(TaskNode<O> node, CompletableFuture<O> future) {
        // Timeout handling: node-level timeout > flow-level defaultTaskTimeoutMs
        long effectiveTimeout = node.getTimeoutMs() > 0 ? node.getTimeoutMs() : defaultTimeoutMs;
        if (effectiveTimeout > 0) {
            final ScheduledFuture<?> timer = SCHEDULER.schedule(() -> {
                if (node.isCompleted()) {
                    return;
                }
                ParallelFlowException timeoutEx = new ParallelFlowException(
                        "Task '" + node.getName() + "' timed out after "
                                + effectiveTimeout + "ms",
                        new TimeoutException());

                if (node.hasTimeoutDefault()) {
                    O defaultResult = node.getTimeoutDefaultValue();
                    if (node.completeTimeoutDefault(defaultResult, effectiveTimeout, timeoutEx)) {
                        future.complete(defaultResult);
                        fireEvent(node.getName(), effectiveTimeout, 0,
                                timeoutEx, defaultResult, EventType.FALLBACK);
                    }
                    return;
                }

                if (node.completeTimeout(effectiveTimeout, timeoutEx)) {
                    future.completeExceptionally(timeoutEx);
                    fireEvent(node.getName(), effectiveTimeout, 0,
                            timeoutEx, null, EventType.FAILURE);
                }
            }, effectiveTimeout, TimeUnit.MILLISECONDS);
            future.whenComplete((r, e) -> timer.cancel(false));
        }
    }

    private <O> void executeWithRetry(TaskNode<O> node, FlowContext ctx, CompletableFuture<O> future) {

        if (node.isCompleted()) {
            return;
        }

        long taskStart = System.currentTimeMillis();
        int maxAttempts = node.getMaxRetries() + 1;
        int retryCount = 0;
        int attemptsMade = 0;
        Throwable lastException = null;
        CircuitBreaker cb = node.getCircuitBreaker();

        fireEvent(node.getName(), 0, 0, null, null, EventType.START);

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            if (node.isCompleted()) {
                return;
            }

            retryCount = attempt - 1;
            attemptsMade = attempt;

            // Check circuit breaker

            if (cb != null && !cb.allowRequest(node.getName())) {
                lastException = new ParallelFlowException(
                        "Circuit breaker is open for task '" + node.getName() + "'");
                break;
            }

            try {
                O result = node.getAction().apply(ctx);

                if (cb != null) {
                    cb.recordSuccess(node.getName());
                }

                long duration = System.currentTimeMillis() - taskStart;
                if (node.completeSuccess(result, retryCount, duration)) {
                    future.complete(result);
                    fireEvent(node.getName(), duration, attempt, null, result, EventType.SUCCESS);
                }
                return;
            } catch (Throwable t) {
                lastException = t;

                if (cb != null) {
                    cb.recordFailure(node.getName());
                }

                if (attempt < maxAttempts && !node.isCompleted()) {
                    long duration = System.currentTimeMillis() - taskStart;
                    fireEvent(node.getName(), duration, attempt, t, null, EventType.RETRY);
                }
            }
        }

        // All attempts exhausted — try fallback
        if (node.getFallback() != null) {
            try {
                O fallbackResult = node.getFallback().apply(lastException);

                long duration = System.currentTimeMillis() - taskStart;
                if (node.completeFallback(fallbackResult, retryCount, duration, lastException)) {
                    future.complete(fallbackResult);
                    fireEvent(node.getName(), duration, attemptsMade,
                            lastException, fallbackResult, EventType.FALLBACK);
                }
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
        if (node.completeFailure(retryCount, duration, lastException)) {
            ParallelFlowException failEx = new ParallelFlowException(
                    "Task '" + node.getName() + "' failed after "
                            + retryCount + " retries", lastException);
            future.completeExceptionally(failEx);
            fireEvent(node.getName(), duration, attemptsMade,
                    lastException, null, EventType.FAILURE);
        }
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
