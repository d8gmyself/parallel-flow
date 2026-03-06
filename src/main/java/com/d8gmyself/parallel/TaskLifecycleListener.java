package com.d8gmyself.parallel;

public interface TaskLifecycleListener {

    class TaskEvent {
        private final String taskName;
        private final long durationMs;
        private final int attempt;
        private final Throwable exception;
        private final Object result;

        public TaskEvent(String taskName, long durationMs, int attempt, Throwable exception, Object result) {
            this.taskName = taskName;
            this.durationMs = durationMs;
            this.attempt = attempt;
            this.exception = exception;
            this.result = result;
        }

        public String getTaskName() {
            return taskName;
        }

        public long getDurationMs() {
            return durationMs;
        }

        public int getAttempt() {
            return attempt;
        }

        public Throwable getException() {
            return exception;
        }

        public Object getResult() {
            return result;
        }

        @Override
        public String toString() {
            return "TaskEvent{task='" + taskName + "', duration=" + durationMs
                    + "ms, attempt=" + attempt + "}";
        }
    }

    default void onStart(TaskEvent event) {}

    default void onSuccess(TaskEvent event) {}

    default void onFailure(TaskEvent event) {}

    default void onRetry(TaskEvent event) {}

    default void onFallback(TaskEvent event) {}
}
