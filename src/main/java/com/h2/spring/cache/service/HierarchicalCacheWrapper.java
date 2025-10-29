package com.h2.spring.cache.service;

import org.springframework.cache.Cache;
import org.springframework.lang.Nullable;

import java.util.Collections;
import java.util.concurrent.Callable;

public class HierarchicalCacheWrapper implements Cache {

    private final String name;
    private final NearNFarHierarchicalCacheService cacheService;
    private final String cacheLevel;

    public HierarchicalCacheWrapper(String name, NearNFarHierarchicalCacheService cacheService) {
        this(name, cacheService, "default");
    }

    public HierarchicalCacheWrapper(String name, NearNFarHierarchicalCacheService cacheService, String cacheLevel) {
        this.name = name;
        this.cacheService = cacheService;
        this.cacheLevel = cacheLevel;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object getNativeCache() {
        return cacheService;
    }

    @Override
    @Nullable
    public ValueWrapper get(Object key) {
        if (key == null) return null;

        return cacheService.get(key.toString(), Object.class, cacheLevel)
                .map(SimpleValueWrapper::new)
                .orElse(null);
    }

    @Override
    @Nullable
    public <T> T get(Object key, @Nullable Class<T> type) {
        if (key == null) return null;

        Class<T> targetType = type != null ? type : (Class<T>) Object.class;
        return cacheService.get(key.toString(), targetType, cacheLevel)
                .orElse(null);
    }

    @Override
    @Nullable
    public <T> T get(Object key, Callable<T> valueLoader) {
        if (key == null) return null;

        return cacheService.getOrCompute(key.toString(), (Class<T>) Object.class, () -> {
            try {
                return valueLoader.call();
            } catch (Exception e) {
                throw new RuntimeException("Value loader failed", e);
            }
        }, cacheLevel);
    }

    @Override
    public void put(Object key, @Nullable Object value) {
        if (key != null && value != null) {
            cacheService.put(key.toString(), Collections.emptyList(), value, cacheLevel);
        }
    }

    @Override
    @Nullable
    public ValueWrapper putIfAbsent(Object key, @Nullable Object value) {
        if (key == null) return null;

        ValueWrapper existing = get(key);
        if (existing == null && value != null) {
            put(key, value);
        }
        return existing;
    }

    @Override
    public void evict(Object key) {
        if (key != null) {
            cacheService.invalidate(key.toString(), cacheLevel);
        }
    }

    @Override
    public boolean evictIfPresent(Object key) {
        if (key != null) {
            boolean existed = get(key) != null;
            evict(key);
            return existed;
        }
        return false;
    }

    @Override
    public void clear() {
        // Clear only this specific cache level
        cacheService.invalidateAll(); // You might want to implement level-specific clearing
    }

    private static class SimpleValueWrapper implements ValueWrapper {
        private final Object value;

        public SimpleValueWrapper(Object value) {
            this.value = value;
        }

        @Override
        @Nullable
        public Object get() {
            return value;
        }
    }
}