package com.d8gmyself.parallel;

/**
 * 单个TaskNode执行后的不可变状态快照
 */
public class NodeState {

    private final String name;
    private final boolean executed;
    private final boolean success;
    private final boolean optional;
    private final boolean fallbackUsed;
    private final boolean timedOut;
    private final int actualRetryCount;
    private final long durationMs;
    private final Throwable exception;

    NodeState(String name, boolean executed, boolean success, boolean optional,
              boolean fallbackUsed, boolean timedOut, int actualRetryCount,
              long durationMs, Throwable exception) {
        this.name = name;
        this.executed = executed;
        this.success = success;
        this.optional = optional;
        this.fallbackUsed = fallbackUsed;
        this.timedOut = timedOut;
        this.actualRetryCount = actualRetryCount;
        this.durationMs = durationMs;
        this.exception = exception;
    }

    public String getName() {
        return name;
    }

    public boolean isExecuted() {
        return executed;
    }

    public boolean isSuccess() {
        return executed && success;
    }

    public boolean isOptional() {
        return optional;
    }

    public boolean isFallbackUsed() {
        return fallbackUsed;
    }

    public boolean isTimedOut() {
        return timedOut;
    }

    public int getActualRetryCount() {
        return actualRetryCount;
    }

    public long getDurationMs() {
        return durationMs;
    }

    public Throwable getException() {
        return exception;
    }

    @Override
    public String toString() {
        return "NodeState{name='" + name + "', success=" + isSuccess()
                + ", duration=" + durationMs + "ms}";
    }
}
