package com.d8gmyself.parallel;

public class ParallelFlowException extends RuntimeException {

    public ParallelFlowException(String message) {
        super(message);
    }

    public ParallelFlowException(String message, Throwable cause) {
        super(message, cause);
    }
}
