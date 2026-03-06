package com.d8gmyself.parallel;

import java.util.concurrent.ConcurrentHashMap;

public class FlowContext {

    private final ConcurrentHashMap<String, Object> params = new ConcurrentHashMap<>();

    public void put(String key, Object value) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        if (value != null) {
            params.put(key, value);
        } else {
            params.remove(key);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        return (T) params.get(key);
    }

    @SuppressWarnings("unchecked")
    public <T> T getOrDefault(String key, T defaultValue) {
        return (T) params.getOrDefault(key, defaultValue);
    }
}
