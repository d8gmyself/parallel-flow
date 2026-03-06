package com.d8gmyself.parallel;

public interface CircuitBreaker {

    boolean allowRequest(String taskName);

    void recordSuccess(String taskName);

    void recordFailure(String taskName);
}
