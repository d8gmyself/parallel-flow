package com.d8gmyself.parallel;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 简易熔断逻辑
 */
public class DefaultCircuitBreaker implements CircuitBreaker {

    private enum State {
        CLOSED, OPEN, HALF_OPEN
    }

    private static class CircuitState {
        final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
        final AtomicInteger consecutiveFailures = new AtomicInteger(0);
        final AtomicLong lastFailureTime = new AtomicLong(0);
    }

    private final int failureThreshold;
    private final long resetTimeoutMs;
    private final ConcurrentHashMap<String, CircuitState> states = new ConcurrentHashMap<String, CircuitState>();

    public DefaultCircuitBreaker() {
        this(5, 10000);
    }

    public DefaultCircuitBreaker(int failureThreshold, long resetTimeoutMs) {
        this.failureThreshold = failureThreshold;
        this.resetTimeoutMs = resetTimeoutMs;
    }

    private CircuitState getState(String taskName) {
        return states.computeIfAbsent(taskName, k -> new CircuitState());
    }

    @Override
    public boolean allowRequest(String taskName) {
        CircuitState cs = getState(taskName);
        switch (cs.state.get()) {
            case CLOSED:
                return true;
            case OPEN:
                long elapsed = System.currentTimeMillis() - cs.lastFailureTime.get();
                if (elapsed >= resetTimeoutMs) {
                    if (cs.state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                        return true; // probe request
                    }
                }
                return false;
            case HALF_OPEN:
                return false; // probe in progress
            default:
                return true;
        }
    }

    @Override
    public void recordSuccess(String taskName) {
        CircuitState cs = getState(taskName);
        cs.consecutiveFailures.set(0);
        cs.state.set(State.CLOSED);
    }

    @Override
    public void recordFailure(String taskName) {
        CircuitState cs = getState(taskName);
        int failures = cs.consecutiveFailures.incrementAndGet();
        cs.lastFailureTime.set(System.currentTimeMillis());
        if (failures >= failureThreshold) {
            cs.state.set(State.OPEN);
        }
    }
}
