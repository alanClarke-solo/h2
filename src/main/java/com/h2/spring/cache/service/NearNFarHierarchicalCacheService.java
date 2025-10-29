package com.h2.spring.cache.service;

import ac.h2.CachedItem;
import ac.h2.CacheStatistics;
import ac.h2.SearchParameter;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.h2.spring.cache.config.HierarchicalCacheProperties;
import jakarta.annotation.PreDestroy;
import org.redisson.api.*;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Service
public class NearNFarHierarchicalCacheService {
    private static final String PRIMARY_KEY_PREFIX = "hcache:primary:";
    private static final String LONG_KEY_PREFIX = "hcache:longkey:";
    private static final String PARAM_PREFIX = "hcache:param:";
    private static final String VALUE_PREFIX = "hcache:value:";

    private final RedissonClient redissonClient;
    private final HierarchicalCacheProperties properties;
    private final Map<String, Cache<String, Object>> caffeineCache;
    private final CacheStatistics statistics;

    public NearNFarHierarchicalCacheService(RedissonClient redissonClient,
                                            HierarchicalCacheProperties properties) {
        this.redissonClient = redissonClient;
        this.properties = properties;
        this.caffeineCache = new ConcurrentHashMap<>();
        this.statistics = new CacheStatistics();
        
        initializeCaffeineCache();
    }

    private void initializeCaffeineCache() {
        // Default L1 cache
        caffeineCache.put("default", createCaffeineCache(
                properties.getCaffeine().getMaximumSize(),
                properties.getCaffeine().getExpireAfterWrite().toMillis(),
                properties.getCaffeine().getExpireAfterAccess().toMillis()
        ));

        // Level-specific L1 caches
        properties.getLevels().forEach((levelName, config) -> {
            if (config.isEnableL1()) {
                caffeineCache.put(levelName, createCaffeineCache(
                        config.getMaximumSize(),
                        config.getTtl().toMillis(),
                        config.getTtl().toMillis()
                ));
            }
        });
    }

    private Cache<String, Object> createCaffeineCache(long maxSize, long expireAfterWriteMs, long expireAfterAccessMs) {
        return Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(expireAfterWriteMs, TimeUnit.MILLISECONDS)
                .expireAfterAccess(expireAfterAccessMs, TimeUnit.MILLISECONDS)
                .recordStats()
                .build();
    }

    // Enhanced PUT operations with L1/L2 coordination
    public <T> void put(String key, List<SearchParameter> parameters, T value) {
        put(key, null, parameters, value, getCacheLevelName(parameters), null);
    }

    public <T> void put(String key, List<SearchParameter> parameters, T value, String cacheLevel) {
        put(key, null, parameters, value, cacheLevel, null);
    }

    public <T> void put(String key, Long id, List<SearchParameter> parameters, T value, 
                       String cacheLevel, Duration ttl) {
        if (key == null || parameters == null || value == null) {
            throw new IllegalArgumentException("Key, parameters, and value cannot be null");
        }

        CachedItem<T> cachedItem = new CachedItem<>(key, id, value, parameters,
                ttl != null ? ttl.toMillis() : getDefaultTtl(cacheLevel));
        String uniqueId = cachedItem.generateUniqueId();

        // Store in L1 cache (Caffeine) if enabled
        if (isL1Enabled(cacheLevel)) {
            Cache<String, Object> l1Cache = getL1Cache(cacheLevel);
            l1Cache.put(uniqueId, cachedItem);
        }

        // Store in L2 cache (Redis) if enabled
        if (isL2Enabled(cacheLevel)) {
            storeInRedis(cachedItem, uniqueId);
        }

        statistics.incrementValues();
        statistics.incrementKeys();
    }

    private <T> void storeInRedis(CachedItem<T> cachedItem, String uniqueId) {
        RBatch batch = redissonClient.createBatch();
        long ttlMillis = cachedItem.getTtl();

        // Store the actual value
        RBucketAsync<Object> valueBucket = batch.getBucket(VALUE_PREFIX + uniqueId);
        if (ttlMillis > 0) {
            valueBucket.setAsync(cachedItem, ttlMillis, TimeUnit.MILLISECONDS);
        } else {
            valueBucket.setAsync(cachedItem);
        }

        // Create primary key reference
        if (cachedItem.getStringKey() != null) {
            RBucketAsync<Object> primaryBucket = batch.getBucket(PRIMARY_KEY_PREFIX + cachedItem.getStringKey());
            if (ttlMillis > 0) {
                primaryBucket.setAsync(uniqueId, ttlMillis, TimeUnit.MILLISECONDS);
            } else {
                primaryBucket.setAsync(uniqueId);
            }
        }

        // Create long key reference if provided
        if (cachedItem.getLongKey() != null) {
            RBucketAsync<Object> longKeyBucket = batch.getBucket(LONG_KEY_PREFIX + cachedItem.getLongKey());
            if (ttlMillis > 0) {
                longKeyBucket.setAsync(uniqueId, ttlMillis, TimeUnit.MILLISECONDS);
            } else {
                longKeyBucket.setAsync(uniqueId);
            }
        }

        // Create hierarchical parameter references
        createParameterReferences(batch, cachedItem.getParameters(), uniqueId, ttlMillis);

        batch.execute();
    }

    // Enhanced GET operations with L1/L2 coordination
    public <T> Optional<T> get(String key, Class<T> valueType) {
        return get(key, valueType, "default");
    }

    public <T> Optional<T> get(String key, Class<T> valueType, String cacheLevel) {
        if (key == null) return Optional.empty();

        // Try L1 cache first
        if (isL1Enabled(cacheLevel)) {
            Cache<String, Object> l1Cache = getL1Cache(cacheLevel);
            String primaryKey = PRIMARY_KEY_PREFIX + key;
            String uniqueId = (String) l1Cache.getIfPresent(primaryKey);
            
            if (uniqueId != null) {
                @SuppressWarnings("unchecked")
                CachedItem<T> cachedItem = (CachedItem<T>) l1Cache.getIfPresent(uniqueId);
                if (cachedItem != null && !cachedItem.isExpired()) {
                    statistics.incrementHits();
                    return Optional.of(cachedItem.getValue());
                }
            }
        }

        // Try L2 cache (Redis)
        if (isL2Enabled(cacheLevel)) {
            RBucket<String> primaryBucket = redissonClient.getBucket(PRIMARY_KEY_PREFIX + key);
            String uniqueId = primaryBucket.get();
            
            if (uniqueId != null) {
                Optional<T> result = getFromRedis(uniqueId, valueType);
                if (result.isPresent()) {
                    // Populate L1 cache for next time
                    if (isL1Enabled(cacheLevel)) {
                        RBucket<CachedItem<T>> valueBucket = redissonClient.getBucket(VALUE_PREFIX + uniqueId);
                        CachedItem<T> cachedItem = valueBucket.get();
                        if (cachedItem != null) {
                            Cache<String, Object> l1Cache = getL1Cache(cacheLevel);
                            l1Cache.put(PRIMARY_KEY_PREFIX + key, uniqueId);
                            l1Cache.put(uniqueId, cachedItem);
                        }
                    }
                    statistics.incrementHits();
                    return result;
                }
            }
        }

        statistics.incrementMisses();
        return Optional.empty();
    }

    public <T> List<T> get(List<SearchParameter> parameters, Class<T> valueType) {
        return get(parameters, valueType, getCacheLevelName(parameters));
    }

    public <T> List<T> get(List<SearchParameter> parameters, Class<T> valueType, String cacheLevel) {
        if (parameters == null || parameters.isEmpty()) {
            statistics.incrementMisses();
            return Collections.emptyList();
        }

        return getByParameters(parameters, valueType, cacheLevel);
    }

    private <T> Optional<T> getFromRedis(String uniqueId, Class<T> valueType) {
        RBucket<CachedItem<T>> valueBucket = redissonClient.getBucket(VALUE_PREFIX + uniqueId);
        CachedItem<T> cachedItem = valueBucket.get();
        
        if (cachedItem == null || cachedItem.isExpired()) {
            if (cachedItem != null && cachedItem.isExpired()) {
                invalidateByUniqueId(uniqueId);
            }
            return Optional.empty();
        }
        
        return Optional.of(cachedItem.getValue());
    }

    // Enhanced getOrCompute with hierarchical support
    public <T> T getOrCompute(String key, Class<T> valueType, Supplier<T> supplier, String cacheLevel) {
        Optional<T> cached = get(key, valueType, cacheLevel);
        if (cached.isPresent()) {
            return cached.get();
        }
        
        T computed = supplier.get();
        if (computed != null) {
            put(key, Collections.emptyList(), computed, cacheLevel);
        }
        return computed;
    }

    // Enhanced invalidation with L1/L2 coordination
    public void invalidate(String key) {
        invalidate(key, "default");
    }

    public void invalidate(String key, String cacheLevel) {
        if (key == null) return;

        // Remove from L1 cache
        if (isL1Enabled(cacheLevel)) {
            Cache<String, Object> l1Cache = getL1Cache(cacheLevel);
            String primaryKey = PRIMARY_KEY_PREFIX + key;
            String uniqueId = (String) l1Cache.getIfPresent(primaryKey);
            if (uniqueId != null) {
                l1Cache.invalidate(primaryKey);
                l1Cache.invalidate(uniqueId);
            }
        }

        // Remove from L2 cache
        if (isL2Enabled(cacheLevel)) {
            RBucket<String> primaryBucket = redissonClient.getBucket(PRIMARY_KEY_PREFIX + key);
            String uniqueId = primaryBucket.get();
            if (uniqueId != null) {
                invalidateFromRedis(uniqueId);
            }
        }
    }

    public void invalidateAll() {
        // Clear all L1 caches
        caffeineCache.values().forEach(Cache::invalidateAll);
        
        // Clear L2 cache
        RKeys keys = redissonClient.getKeys();
        keys.deleteByPattern(PRIMARY_KEY_PREFIX + "*");
        keys.deleteByPattern(LONG_KEY_PREFIX + "*");
        keys.deleteByPattern(PARAM_PREFIX + "*");
        keys.deleteByPattern(VALUE_PREFIX + "*");
        
        statistics.reset();
    }

    // Helper methods
    private String getCacheLevelName(List<SearchParameter> parameters) {
        return parameters.stream()
                .min(Comparator.comparingInt(SearchParameter::getLevel))
                .map(param -> "level_" + param.getLevel())
                .orElse("default");
    }

    private boolean isL1Enabled(String cacheLevel) {
        if ("default".equals(cacheLevel)) return true;
        HierarchicalCacheProperties.CacheLevel config = properties.getLevels().get(cacheLevel);
        return config == null || config.isEnableL1();
    }

    private boolean isL2Enabled(String cacheLevel) {
        if ("default".equals(cacheLevel)) return true;
        HierarchicalCacheProperties.CacheLevel config = properties.getLevels().get(cacheLevel);
        return config == null || config.isEnableL2();
    }

    private Cache<String, Object> getL1Cache(String cacheLevel) {
        return caffeineCache.getOrDefault(cacheLevel, caffeineCache.get("default"));
    }

    private long getDefaultTtl(String cacheLevel) {
        if ("default".equals(cacheLevel)) {
            return properties.getRedis().getDefaultTtl().toMillis();
        }
        HierarchicalCacheProperties.CacheLevel config = properties.getLevels().get(cacheLevel);
        return config != null ? config.getTtl().toMillis() : properties.getRedis().getDefaultTtl().toMillis();
    }

    // Implementation of remaining methods from original class...
    private void createParameterReferences(RBatch batch, List<SearchParameter> parameters, String uniqueId, long ttlMillis) {
        List<SearchParameter> sortedParams = parameters.stream()
                .sorted(Comparator.comparingInt(SearchParameter::getLevel))
                .collect(Collectors.toList());

        Set<String> patterns = generateHierarchicalPatterns(sortedParams);
        
        for (String pattern : patterns) {
            RSetAsync<Object> paramSet = batch.getSet(PARAM_PREFIX + pattern);
            paramSet.addAsync(uniqueId);
            if (ttlMillis > 0) {
                paramSet.expireAsync(ttlMillis, TimeUnit.MILLISECONDS);
            }
        }
    }

    private Set<String> generateHierarchicalPatterns(List<SearchParameter> sortedParams) {
        Set<String> patterns = new HashSet<>();
        
        for (int i = 0; i < sortedParams.size(); i++) {
            for (int j = i; j < sortedParams.size(); j++) {
                List<SearchParameter> subList = sortedParams.subList(i, j + 1);
                String pattern = subList.stream()
                        .map(SearchParameter::toKey)
                        .collect(Collectors.joining(">"));
                patterns.add(pattern);
            }
        }
        
        for (SearchParameter param : sortedParams) {
            patterns.add(param.toKey());
        }
        
        return patterns;
    }

    private <T> List<T> getByParameters(List<SearchParameter> parameters, Class<T> valueType, String cacheLevel) {
        List<SearchParameter> sortedParams = parameters.stream()
                .sorted(Comparator.comparingInt(SearchParameter::getLevel))
                .collect(Collectors.toList());

        String exactPattern = sortedParams.stream()
                .map(SearchParameter::toKey)
                .collect(Collectors.joining(">"));

        Set<String> uniqueIds = getUniqueIdsByPattern(exactPattern);
        
        if (uniqueIds.isEmpty()) {
            uniqueIds = findItemsWithHierarchicalDegradation(sortedParams);
        }

        List<T> results = new ArrayList<>();
        for (String uniqueId : uniqueIds) {
            Optional<T> item = getFromRedis(uniqueId, valueType);
            item.ifPresent(results::add);
        }

        if (results.isEmpty()) {
            statistics.incrementMisses();
        } else {
            statistics.incrementHits();
        }

        return results;
    }

    private Set<String> getUniqueIdsByPattern(String pattern) {
        RSet<String> paramSet = redissonClient.getSet(PARAM_PREFIX + pattern);
        return new HashSet<>(paramSet.readAll());
    }

    private Set<String> findItemsWithHierarchicalDegradation(List<SearchParameter> searchParams) {
        Set<String> allUniqueIds = new HashSet<>();
        
        RKeys keys = redissonClient.getKeys();
        Iterable<String> paramKeys = keys.getKeysByPattern(PARAM_PREFIX + "*");
        
        for (String key : paramKeys) {
            String pattern = key.substring(PARAM_PREFIX.length());
            if (patternMatches(pattern, searchParams)) {
                RSet<String> paramSet = redissonClient.getSet(key);
                allUniqueIds.addAll(paramSet.readAll());
            }
        }
        
        return allUniqueIds;
    }

    private boolean patternMatches(String pattern, List<SearchParameter> searchParams) {
        String[] patternParts = pattern.split(">");
        Set<String> searchParamKeys = searchParams.stream()
                .map(SearchParameter::toKey)
                .collect(Collectors.toSet());
        
        for (String part : patternParts) {
            if (searchParamKeys.contains(part)) {
                searchParamKeys.remove(part);
            }
        }
        
        return searchParamKeys.isEmpty();
    }

    private void invalidateByUniqueId(String uniqueId) {
        // Remove from all L1 caches
        caffeineCache.values().forEach(cache -> cache.invalidate(uniqueId));
        
        // Remove from L2 cache
        invalidateFromRedis(uniqueId);
    }

    private void invalidateFromRedis(String uniqueId) {
        RBucket<CachedItem<?>> valueBucket = redissonClient.getBucket(VALUE_PREFIX + uniqueId);
        CachedItem<?> cachedItem = valueBucket.get();

        if (cachedItem == null) return;

        RBatch batch = redissonClient.createBatch();

        RBucketAsync<Object> valueBucketBatch = batch.getBucket(VALUE_PREFIX + uniqueId);
        valueBucketBatch.deleteAsync();

        if (cachedItem.getStringKey() != null) {
            RBucketAsync<Object> primaryBucket = batch.getBucket(PRIMARY_KEY_PREFIX + cachedItem.getStringKey());
            primaryBucket.deleteAsync();
        }

        if (cachedItem.getLongKey() != null) {
            RBucketAsync<Object> longKeyBucket = batch.getBucket(LONG_KEY_PREFIX + cachedItem.getLongKey());
            longKeyBucket.deleteAsync();
        }

        Set<String> patterns = generateHierarchicalPatterns(cachedItem.getParameters().stream()
                .sorted(Comparator.comparingInt(SearchParameter::getLevel))
                .collect(Collectors.toList()));

        for (String pattern : patterns) {
            RSetAsync<Object> paramSet = batch.getSet(PARAM_PREFIX + pattern);
            paramSet.removeAsync(uniqueId);
        }

        batch.execute();
        statistics.decrementValues();
        statistics.decrementKeys();
    }

    public CacheStatistics getStatistics() {
        return statistics;
    }

    @PreDestroy
    public void shutdown() {
        caffeineCache.clear();
        redissonClient.shutdown();
    }

// Example of how to use the enhanced statistics in HierarchicalCacheService
// Add this method to HierarchicalCacheService class

    public void resetStatistics() {
        statistics.reset();
    }

    public void hardResetStatistics() {
        statistics.hardReset();
    }

    public CacheStatistics.CacheStatisticsSnapshot getStatisticsSnapshot() {
        return statistics.getSnapshot();
    }
}
