package com.d8gmyself.parallel;

/**
 * 单个TaskNode执行后的不可变状态快照
 */
public class NodeState {

    private final String name;
    private final boolean success;
    private final boolean fallbackUsed;
    private final boolean timedOut;
    private final int actualRetryCount;
    private final long durationMs;
    private final Throwable exception;

    NodeState(String name, boolean success,
              boolean fallbackUsed, boolean timedOut, int actualRetryCount,
              long durationMs, Throwable exception) {
        this.name = name;
        this.success = success;
        this.fallbackUsed = fallbackUsed;
        this.timedOut = timedOut;
        this.actualRetryCount = actualRetryCount;
        this.durationMs = durationMs;
        this.exception = exception;
    }

    public String getName() {
        return name;
    }

    public boolean isSuccess() {
        return success;
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
