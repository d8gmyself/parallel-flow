package com.d8gmyself.parallel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Slf4jTaskLifecycleListener implements TaskLifecycleListener {

    private static final Logger logger = LoggerFactory.getLogger(Slf4jTaskLifecycleListener.class);

    public static final TaskLifecycleListener INSTANCE = new Slf4jTaskLifecycleListener();

    @Override
    public void onFailure(TaskEvent event) {
        logger.error("TaskNode failure, {}", event, event.getException());
    }

    @Override
    public void onRetry(TaskEvent event) {
        logger.warn("TaskNode retry, {}", event, event.getException());
    }

    @Override
    public void onFallback(TaskEvent event) {
        logger.warn("TaskNode fallback, {}", event, event.getException());
    }

}
