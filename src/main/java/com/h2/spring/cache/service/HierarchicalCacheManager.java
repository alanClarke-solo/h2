// HierarchicalCacheManager.java (Enhanced to support standard Spring Cache annotations)
package com.h2.spring.cache.service;

import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.lang.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class HierarchicalCacheManager implements CacheManager {

    private final NearNFarHierarchicalCacheService cacheService;
    private final ConcurrentMap<String, Cache> caches = new ConcurrentHashMap<>();

    public HierarchicalCacheManager(NearNFarHierarchicalCacheService cacheService) {
        this.cacheService = cacheService;
    }

    @Override
    @Nullable
    public Cache getCache(String name) {
        return caches.computeIfAbsent(name, cacheName ->
                new HierarchicalCacheWrapper(cacheName, cacheService));
    }

    @Override
    public Collection<String> getCacheNames() {
        return Collections.unmodifiableSet(caches.keySet());
    }

    public void clearCache(String name) {
        Cache cache = caches.get(name);
        if (cache != null) {
            cache.clear();
        }
    }

    public void clearAllCaches() {
        caches.values().forEach(Cache::clear);
    }

    // Additional method to support dynamic cache creation
    public Cache createCache(String name, String cacheLevel) {
        return caches.computeIfAbsent(name, cacheName ->
                new HierarchicalCacheWrapper(cacheName, cacheService, cacheLevel));
    }
}