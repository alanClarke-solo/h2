
package com.example.cache.service;

import ac.h2.HierarchicalCacheService;
import com.example.cache.model.CacheConfiguration;
import com.example.cache.model.CachedItem;
import com.example.cache.model.SearchParameter;
import com.example.cache.stats.CacheStatistics;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;


@Service
public class TransparentHierarchicalCacheService<T> {
    private static final Logger logger = LoggerFactory.getLogger(TransparentHierarchicalCacheService.class);

    // Local caches (L1)
    private final Cache<String, CachedItem<T>> localPrimaryCache;
    private final Cache<Long, String> localLongKeyCache;
    private final Cache<String, Set<String>> localParamCache;

    // Remote cache service (L2)
    private final HierarchicalCacheService redisCache;

    // Configuration
    private final CacheConfiguration config;
    private final CacheStatistics statistics;

    // Internal state
    private final Map<String, Set<String>> parameterPatterns = new ConcurrentHashMap<>();

    public TransparentHierarchicalCacheService(
            HierarchicalCacheService redisCache,
            CacheConfiguration config) {

        this.redisCache = redisCache;
        this.config = config;
        this.statistics = new CacheStatistics();

        // Initialize local caches with removal listener
        RemovalListener<String, CachedItem<T>> removalListener = (key, value, cause) -> {
            logger.debug("Local cache eviction - Key: {}, Cause: {}", key, cause);
            statistics.incrementL1Evictions();
            if (value != null && config.isWriteThroughEnabled()) {
                writeToRemoteAsync(value);
            }
        };

        this.localPrimaryCache = Caffeine.newBuilder()
                .maximumSize(config.getMaxLocalCacheSize())
                .expireAfterWrite(Duration.ofMillis(config.getLocalCacheTtlMillis()))
                .removalListener(removalListener)
                .recordStats()
                .build();

        this.localLongKeyCache = Caffeine.newBuilder()
                .maximumSize(config.getMaxLocalCacheSize())
                .expireAfterWrite(Duration.ofMillis(config.getLocalCacheTtlMillis()))
                .recordStats()
                .build();

        this.localParamCache = Caffeine.newBuilder()
                .maximumSize(config.getMaxLocalCacheSize() * 10)
                .expireAfterWrite(Duration.ofMillis(config.getLocalCacheTtlMillis()))
                .recordStats()
                .build();
    }

    // ==================== PUT OPERATIONS ====================

    public void put(String key, List<SearchParameter> parameters, T value) {
        put(key, null, parameters, value, config.getRemoteCacheTtlMillis());
    }

    public void put(String key, List<SearchParameter> parameters, T value, long ttlMillis) {
        put(key, null, parameters, value, ttlMillis);
    }

    public void put(String key, Long id, List<SearchParameter> parameters, T value) {
        put(key, id, parameters, value, config.getRemoteCacheTtlMillis());
    }

    public void put(String key, Long id, List<SearchParameter> parameters, T value, long ttlMillis) {
        if (key == null || parameters == null || value == null) {
            throw new IllegalArgumentException("Key, parameters, and value cannot be null");
        }

        CachedItem<T> cachedItem = new CachedItem<>(key, id, value, parameters, ttlMillis);
        String uniqueId = cachedItem.generateUniqueId();

        // Store in local cache (L1)
        if (config.isLocalCacheEnabled()) {
            localPrimaryCache.put(uniqueId, cachedItem);
            statistics.incrementL1Puts();

            if (id != null) {
                localLongKeyCache.put(id, uniqueId);
            }

            // Update parameter patterns
            updateLocalParameterPatterns(parameters, uniqueId);
        }

        // Store in remote cache (L2)
        if (config.isRemoteCacheEnabled()) {
            try {
                redisCache.put(key, id, parameters, value, ttlMillis);
                statistics.incrementL2Puts();
            } catch (Exception e) {
                logger.warn("Failed to store in remote cache", e);
                statistics.incrementL2Errors();
            }
        }

        statistics.incrementValues();
        statistics.incrementKeys();
    }

    private void updateLocalParameterPatterns(List<SearchParameter> parameters, String uniqueId) {
        Set<String> patterns = generateHierarchicalPatterns(parameters);
        for (String pattern : patterns) {
            localParamCache.asMap().compute(pattern, (k, v) -> {
                Set<String> ids = v != null ? new HashSet<>(v) : new HashSet<>();
                ids.add(uniqueId);
                return ids;
            });
        }
    }

    private Set<String> generateHierarchicalPatterns(List<SearchParameter> parameters) {
        Set<String> patterns = new HashSet<>();
        List<SearchParameter> sortedParams = new ArrayList<>(parameters);
        sortedParams.sort(Comparator.comparingInt(SearchParameter::getLevel));

        for (int i = 1; i <= sortedParams.size(); i++) {
            StringBuilder pattern = new StringBuilder();
            for (int j = 0; j < i; j++) {
                if (j > 0) pattern.append(":");
                SearchParameter param = sortedParams.get(j);
                pattern.append(param.getName()).append("=").append(param.getValue());
            }
            patterns.add(pattern.toString());
        }

        return patterns;
    }

    // ==================== GET OPERATIONS ====================

    public Optional<T> get(String key, Class<T> valueType) {
        statistics.incrementRequests();

        // Try local cache first (L1)
        if (config.isLocalCacheEnabled()) {
            Optional<T> localResult = getFromLocal(key, null, null, valueType);
            if (localResult.isPresent()) {
                statistics.incrementL1Hits();
                return localResult;
            } else {
                statistics.incrementL1Misses();
            }
        }

        // Try remote cache (L2)
        if (config.isRemoteCacheEnabled()) {
            Optional<T> remoteResult = getFromRemote(key, null, null, valueType);
            if (remoteResult.isPresent()) {
                statistics.incrementL2Hits();
                // Cache in local cache for future access
                if (config.isLocalCacheEnabled()) {
                    CachedItem<T> item = new CachedItem<>(key, null, remoteResult.get(),
                            Collections.emptyList(), config.getLocalCacheTtlMillis());
                    localPrimaryCache.put(item.generateUniqueId(), item);
                    statistics.incrementL1Puts();
                }
                return remoteResult;
            } else {
                statistics.incrementL2Misses();
            }
        }

        statistics.incrementMisses();
        return Optional.empty();
    }

    public Optional<T> get(String key, Long id, Class<T> valueType) {
        statistics.incrementRequests();

        // Try local cache first (L1)
        if (config.isLocalCacheEnabled()) {
            Optional<T> localResult = getFromLocal(key, id, null, valueType);
            if (localResult.isPresent()) {
                statistics.incrementL1Hits();
                return localResult;
            } else {
                statistics.incrementL1Misses();
            }
        }

        // Try remote cache (L2)
        if (config.isRemoteCacheEnabled()) {
            Optional<T> remoteResult = getFromRemote(key, id, null, valueType);
            if (remoteResult.isPresent()) {
                statistics.incrementL2Hits();
                // Cache in local cache for future access
                if (config.isLocalCacheEnabled()) {
                    CachedItem<T> item = new CachedItem<>(key, id, remoteResult.get(),
                            Collections.emptyList(), config.getLocalCacheTtlMillis());
                    String uniqueId = item.generateUniqueId();
                    localPrimaryCache.put(uniqueId, item);
                    if (id != null) {
                        localLongKeyCache.put(id, uniqueId);
                    }
                    statistics.incrementL1Puts();
                }
                return remoteResult;
            } else {
                statistics.incrementL2Misses();
            }
        }

        statistics.incrementMisses();
        return Optional.empty();
    }

    public List<T> get(List<SearchParameter> parameters, Class<T> valueType) {
        statistics.incrementRequests();

        List<T> results = new ArrayList<>();

        // Try local cache first (L1)
        if (config.isLocalCacheEnabled()) {
            results = getFromLocalByParameters(parameters, valueType);
            if (!results.isEmpty()) {
                statistics.incrementL1Hits();
                return results;
            } else {
                statistics.incrementL1Misses();
            }
        }

        // Try remote cache (L2)
        if (config.isRemoteCacheEnabled()) {
            results = getFromRemoteByParameters(parameters, valueType);
            if (!results.isEmpty()) {
                statistics.incrementL2Hits();
                return results;
            } else {
                statistics.incrementL2Misses();
            }
        }

        statistics.incrementMisses();
        return results;
    }

    private Optional<T> getFromLocal(String key, Long id, List<SearchParameter> parameters, Class<T> valueType) {
        // Implementation for local cache lookup
        if (key != null) {
            CachedItem<T> item = new CachedItem<>(key, id, null, parameters != null ? parameters : Collections.emptyList(), 0);
            String uniqueId = item.generateUniqueId();
            CachedItem<T> cached = localPrimaryCache.getIfPresent(uniqueId);
            if (cached != null && !cached.isExpired()) {
                return Optional.of(cached.getValue());
            }
        }

        if (id != null) {
            String uniqueId = localLongKeyCache.getIfPresent(id);
            if (uniqueId != null) {
                CachedItem<T> cached = localPrimaryCache.getIfPresent(uniqueId);
                if (cached != null && !cached.isExpired()) {
                    return Optional.of(cached.getValue());
                }
            }
        }

        return Optional.empty();
    }

    private Optional<T> getFromRemote(String key, Long id, List<SearchParameter> parameters, Class<T> valueType) {
        try {
            if (key != null && id != null) {
                return redisCache.get(key, id, valueType);
            } else if (key != null) {
                return redisCache.get(key, valueType);
            } else if (id != null) {
                return redisCache.get(id, valueType);
            }
        } catch (Exception e) {
            logger.warn("Error accessing remote cache", e);
            statistics.incrementL2Errors();
        }
        return Optional.empty();
    }

    private List<T> getFromLocalByParameters(List<SearchParameter> parameters, Class<T> valueType) {
        List<T> results = new ArrayList<>();
        Set<String> patterns = generateHierarchicalPatterns(parameters);

        for (String pattern : patterns) {
            Set<String> uniqueIds = localParamCache.getIfPresent(pattern);
            if (uniqueIds != null) {
                for (String uniqueId : uniqueIds) {
                    CachedItem<T> cached = localPrimaryCache.getIfPresent(uniqueId);
                    if (cached != null && !cached.isExpired()) {
                        results.add(cached.getValue());
                    }
                }
                if (!results.isEmpty()) {
                    break; // Return first matching level
                }
            }
        }

        return results;
    }

    private List<T> getFromRemoteByParameters(List<SearchParameter> parameters, Class<T> valueType) {
        try {
            return redisCache.get(parameters, valueType);
        } catch (Exception e) {
            logger.warn("Error accessing remote cache by parameters", e);
            statistics.incrementL2Errors();
            return Collections.emptyList();
        }
    }

    // ==================== GET OR COMPUTE OPERATIONS ====================

    public T getOrCompute(String key, Class<T> valueType, Supplier<T> supplier) {
        Optional<T> cached = get(key, valueType);
        if (cached.isPresent()) {
            return cached.get();
        }

        T computed = supplier.get();
        if (computed != null) {
            put(key, Collections.emptyList(), computed);
        }
        return computed;
    }

    public T getOrCompute(String key, Long id, Class<T> valueType, Supplier<T> supplier) {
        Optional<T> cached = get(key, id, valueType);
        if (cached.isPresent()) {
            return cached.get();
        }

        T computed = supplier.get();
        if (computed != null) {
            put(key, id, Collections.emptyList(), computed);
        }
        return computed;
    }

    public List<T> getOrCompute(List<SearchParameter> parameters, Class<T> valueType, Supplier<List<T>> supplier) {
        List<T> cached = get(parameters, valueType);
        if (!cached.isEmpty()) {
            return cached;
        }

        List<T> computed = supplier.get();
        if (computed != null && !computed.isEmpty()) {
            // Store each computed item
            for (T item : computed) {
                put("computed_" + System.nanoTime(), null, parameters, item);
            }
        }
        return computed != null ? computed : Collections.emptyList();
    }

    // ==================== INVALIDATION OPERATIONS ====================

    public void invalidate(String key) {
        if (config.isLocalCacheEnabled()) {
            invalidateFromLocal(key, null, null);
        }

        if (config.isRemoteCacheEnabled()) {
            try {
                redisCache.invalidate(key);
            } catch (Exception e) {
                logger.warn("Error invalidating remote cache", e);
                statistics.incrementL2Errors();
            }
        }
    }

    public void invalidate(String key, Long id) {
        if (config.isLocalCacheEnabled()) {
            invalidateFromLocal(key, id, null);
        }

        if (config.isRemoteCacheEnabled()) {
            try {
                redisCache.invalidate(key, id);
            } catch (Exception e) {
                logger.warn("Error invalidating remote cache", e);
                statistics.incrementL2Errors();
            }
        }
    }

    public void invalidateByPattern(List<SearchParameter> parameters) {
        if (config.isLocalCacheEnabled()) {
            invalidateFromLocal(null, null, parameters);
        }

        if (config.isRemoteCacheEnabled()) {
            try {
                redisCache.invalidateByPattern(parameters);
            } catch (Exception e) {
                logger.warn("Error invalidating remote cache by pattern", e);
                statistics.incrementL2Errors();
            }
        }
    }

    private void invalidateFromLocal(String key, Long id, List<SearchParameter> parameters) {
        if (key != null || id != null) {
            CachedItem<T> item = new CachedItem<>(key, id, null,
                    parameters != null ? parameters : Collections.emptyList(), 0);
            String uniqueId = item.generateUniqueId();
            localPrimaryCache.invalidate(uniqueId);

            if (id != null) {
                localLongKeyCache.invalidate(id);
            }
        }

        if (parameters != null) {
            Set<String> patterns = generateHierarchicalPatterns(parameters);
            for (String pattern : patterns) {
                Set<String> uniqueIds = localParamCache.getIfPresent(pattern);
                if (uniqueIds != null) {
                    for (String uniqueId : uniqueIds) {
                        localPrimaryCache.invalidate(uniqueId);
                    }
                    localParamCache.invalidate(pattern);
                }
            }
        }
    }

    public void invalidateAll() {
        if (config.isLocalCacheEnabled()) {
            localPrimaryCache.invalidateAll();
            localLongKeyCache.invalidateAll();
            localParamCache.invalidateAll();
        }

        if (config.isRemoteCacheEnabled()) {
            try {
                redisCache.invalidateAll();
            } catch (Exception e) {
                logger.warn("Error invalidating all remote cache", e);
                statistics.incrementL2Errors();
            }
        }
    }

    // ==================== UTILITY METHODS ====================

    private void writeToRemoteAsync(CachedItem<T> item) {
        CompletableFuture.runAsync(() -> {
            try {
                redisCache.put(item.getPrimaryKey(), item.getLongKey(),
                        item.getParameters(), item.getValue(), item.getTtlMillis());
                statistics.incrementL2Puts();
            } catch (Exception e) {
                logger.warn("Failed to write to remote cache asynchronously", e);
                statistics.incrementL2Errors();
            }
        });
    }

    public CacheStatistics getStatistics() {
        return statistics;
    }

    public void clearStatistics() {
        statistics.reset();
    }

    public String getDetailedStatistics() {
        return statistics.getDetailedReport();
    }

    public CacheStatistics.CacheStatisticsSnapshot getStatisticsSnapshot() {
        return statistics.getSnapshot();
    }
}
