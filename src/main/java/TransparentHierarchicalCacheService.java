// TransparentHierarchicalCacheService.java - The main transparent cache adapter
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class TransparentHierarchicalCacheService<T> {
    private static final Logger logger = LoggerFactory.getLogger(TransparentHierarchicalCacheService.class);
    
    // Local caches
    private final Cache<String, CachedItem<T>> localPrimaryCache;
    private final Cache<Long, String> localLongKeyCache;
    private final Cache<String, Set<String>> localParamCache;
    
    // Remote providers
    private final HierarchicalCacheService<T> redisCache;
    private final DatabaseCacheProvider<T> databaseCache;
    
    // Configuration
    private final CacheConfiguration config;
    private final CacheStatistics statistics;
    
    // Internal state
    private final Map<String, Set<String>> parameterPatterns = new ConcurrentHashMap<>();
    
    public TransparentHierarchicalCacheService(
            HierarchicalCacheService<T> redisCache,
            DatabaseCacheProvider<T> databaseCache,
            CacheConfiguration config) {
        
        this.redisCache = redisCache;
        this.databaseCache = databaseCache;
        this.config = config;
        this.statistics = new CacheStatistics();
        
        // Initialize local caches
        RemovalListener<String, CachedItem<T>> removalListener = (key, value, cause) -> {
            logger.debug("Local cache eviction - Key: {}, Cause: {}", key, cause);
            if (value != null && config.isWriteThroughEnabled()) {
                // Optionally write back to remote cache on eviction
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
                .maximumSize(config.getMaxLocalCacheSize() * 10) // More parameter patterns
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
        
        CacheContext context = CacheContext.get();
        long effectiveTtl = (context != null && context.getCustomTtl() != null) 
            ? context.getCustomTtl() : ttlMillis;
        
        CachedItem<T> cachedItem = new CachedItem<>(key, id, value, parameters, effectiveTtl);
        String uniqueId = cachedItem.generateUniqueId();
        
        // Store in local cache
        if (config.isLocalCacheEnabled() && 
            (context == null || !context.isSkipLocalCache())) {
            localPrimaryCache.put(uniqueId, cachedItem);
            
            // Update local indexes
            if (id != null) {
                localLongKeyCache.put(id, uniqueId);
            }
            updateLocalParameterIndexes(parameters, uniqueId);
        }
        
        // Write through to remote caches
        if (config.isWriteThroughEnabled()) {
            writeToRemote(key, id, parameters, value, effectiveTtl);
        }
        
        statistics.incrementValues();
        statistics.incrementKeys();
    }
    
    // ==================== GET OPERATIONS ====================
    
    public Optional<T> get(String key, Class<T> valueType) {
        if (key == null) return Optional.empty();
        
        CacheContext context = CacheContext.get();
        boolean forceRefresh = context != null && context.isForceRefresh();
        boolean skipLocal = context != null && context.isSkipLocalCache();
        
        // Try local cache first (unless force refresh or skip local)
        if (config.isLocalCacheEnabled() && !forceRefresh && !skipLocal) {
            CachedItem<T> cachedItem = findInLocalCache(key, null);
            if (cachedItem != null && !cachedItem.isExpired()) {
                statistics.incrementHits();
                return Optional.of(cachedItem.getValue());
            }
        }
        
        // Cache miss - try remote sources
        statistics.incrementMisses();
        return getFromRemote(key, null, null, valueType);
    }
    
    public Optional<T> get(String key, Long id, Class<T> valueType) {
        if (key == null || id == null) return Optional.empty();
        
        CacheContext context = CacheContext.get();
        boolean forceRefresh = context != null && context.isForceRefresh();
        boolean skipLocal = context != null && context.isSkipLocalCache();
        
        // Try local cache first
        if (config.isLocalCacheEnabled() && !forceRefresh && !skipLocal) {
            CachedItem<T> cachedItem = findInLocalCache(key, id);
            if (cachedItem != null && !cachedItem.isExpired()) {
                statistics.incrementHits();
                return Optional.of(cachedItem.getValue());
            }
        }
        
        statistics.incrementMisses();
        return getFromRemote(key, id, null, valueType);
    }
    
    public Optional<T> get(Long id, Class<T> valueType) {
        if (id == null) return Optional.empty();
        
        CacheContext context = CacheContext.get();
        boolean forceRefresh = context != null && context.isForceRefresh();
        boolean skipLocal = context != null && context.isSkipLocalCache();
        
        // Try local cache first
        if (config.isLocalCacheEnabled() && !forceRefresh && !skipLocal) {
            String uniqueId = localLongKeyCache.getIfPresent(id);
            if (uniqueId != null) {
                CachedItem<T> cachedItem = localPrimaryCache.getIfPresent(uniqueId);
                if (cachedItem != null && !cachedItem.isExpired()) {
                    statistics.incrementHits();
                    return Optional.of(cachedItem.getValue());
                }
            }
        }
        
        statistics.incrementMisses();
        return getFromRemote(null, id, null, valueType);
    }
    
    public List<T> get(List<SearchParameter> parameters, Class<T> valueType) {
        if (parameters == null || parameters.isEmpty()) {
            statistics.incrementMisses();
            return Collections.emptyList();
        }
        
        CacheContext context = CacheContext.get();
        boolean forceRefresh = context != null && context.isForceRefresh();
        boolean skipLocal = context != null && context.isSkipLocalCache();
        
        // Try local cache first
        if (config.isLocalCacheEnabled() && !forceRefresh && !skipLocal) {
            List<T> localResults = searchInLocalCache(parameters);
            if (!localResults.isEmpty()) {
                statistics.incrementHits();
                return localResults;
            }
        }
        
        statistics.incrementMisses();
        return searchInRemote(parameters, valueType);
    }
    
    public List<T> get(String key, List<SearchParameter> parameters, Class<T> valueType) {
        if (parameters == null || parameters.isEmpty()) {
            Optional<T> single = get(key, valueType);
            return single.map(Collections::singletonList).orElse(Collections.emptyList());
        }
        return get(parameters, valueType);
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
    
    public T getOrCompute(Long id, Class<T> valueType, Supplier<T> supplier) {
        Optional<T> cached = get(id, valueType);
        if (cached.isPresent()) {
            return cached.get();
        }
        
        T computed = supplier.get();
        // Cannot cache with only ID - need string key
        return computed;
    }
    
    public List<T> getOrCompute(List<SearchParameter> parameters, Class<T> valueType, Supplier<List<T>> supplier) {
        List<T> cached = get(parameters, valueType);
        if (!cached.isEmpty()) {
            return cached;
        }
        
        List<T> computed = supplier.get();
        return computed != null ? computed : Collections.emptyList();
    }
    
    // ==================== LINK OPERATIONS ====================
    
    public void link(String key, Long id) {
        if (key == null || id == null) {
            throw new IllegalArgumentException("Key and id cannot be null");
        }
        
        // Update local cache
        CachedItem<T> cachedItem = findInLocalCache(key, null);
        if (cachedItem != null) {
            CachedItem<T> newCachedItem = new CachedItem<>(key, id, cachedItem.getValue(), 
                cachedItem.getParameters(), cachedItem.getTtl());
            String newUniqueId = newCachedItem.generateUniqueId();
            
            localPrimaryCache.invalidate(cachedItem.generateUniqueId());
            localPrimaryCache.put(newUniqueId, newCachedItem);
            localLongKeyCache.put(id, newUniqueId);
        }
        
        // Update remote caches
        if (redisCache != null) {
            redisCache.link(key, id);
        }
    }
    
    public void link(String key, List<SearchParameter> parameters) {
        if (key == null || parameters == null || parameters.isEmpty()) {
            throw new IllegalArgumentException("Key and parameters cannot be null or empty");
        }
        
        // Update local cache
        CachedItem<T> cachedItem = findInLocalCache(key, null);
        if (cachedItem != null) {
            List<SearchParameter> allParams = new ArrayList<>(cachedItem.getParameters());
            allParams.addAll(parameters);
            
            CachedItem<T> newCachedItem = new CachedItem<>(key, cachedItem.getLongKey(), 
                cachedItem.getValue(), allParams, cachedItem.getTtl());
            
            localPrimaryCache.put(cachedItem.generateUniqueId(), newCachedItem);
            updateLocalParameterIndexes(parameters, cachedItem.generateUniqueId());
        }
        
        // Update remote caches
        if (redisCache != null) {
            redisCache.link(key, parameters);
        }
    }
    
    public void link(Long id, List<SearchParameter> parameters) {
        if (id == null || parameters == null || parameters.isEmpty()) {
            throw new IllegalArgumentException("ID and parameters cannot be null or empty");
        }
        
        // Update local cache
        String uniqueId = localLongKeyCache.getIfPresent(id);
        if (uniqueId != null) {
            CachedItem<T> cachedItem = localPrimaryCache.getIfPresent(uniqueId);
            if (cachedItem != null) {
                List<SearchParameter> allParams = new ArrayList<>(cachedItem.getParameters());
                allParams.addAll(parameters);
                
                CachedItem<T> newCachedItem = new CachedItem<>(cachedItem.getStringKey(), id, 
                    cachedItem.getValue(), allParams, cachedItem.getTtl());
                
                localPrimaryCache.put(uniqueId, newCachedItem);
                updateLocalParameterIndexes(parameters, uniqueId);
            }
        }
        
        // Update remote caches
        if (redisCache != null) {
            redisCache.link(id, parameters);
        }
    }
    
    // ==================== INVALIDATION OPERATIONS ====================

    public void invalidate(String key) {
        if (key == null) return;

        // Find and invalidate local cache first - need to find ALL entries with this key
        CachedItem<T> cachedItem = findInLocalCache(key, null);
        String uniqueId = null;
        Long associatedId = null;
        List<SearchParameter> parameters = null;

        if (cachedItem != null) {
            uniqueId = cachedItem.generateUniqueId();
            associatedId = cachedItem.getLongKey();
            parameters = cachedItem.getParameters();
        } else {
            // If not found in local cache, try to find in all entries by iterating
            for (Map.Entry<String, CachedItem<T>> entry : localPrimaryCache.asMap().entrySet()) {
                CachedItem<T> item = entry.getValue();
                if (key.equals(item.getStringKey())) {
                    cachedItem = item;
                    uniqueId = entry.getKey(); // This is the actual key used in the cache
                    associatedId = item.getLongKey();
                    parameters = item.getParameters();
                    break;
                }
            }
        }

        // Remove from all local caches using cascading approach
        if (cachedItem != null) {
            invalidateLocalCaches(uniqueId, key, associatedId, parameters);
        }

        // Invalidate remote caches with cascading - always call this even if not found locally
        // because the item might exist in remote caches only
        invalidateRemoteCaches(key, associatedId);

        statistics.decrementValues();
    }

    public void invalidate(Long id) {
        if (id == null) return;

        // Find the cached item first to get complete information
        String uniqueId = localLongKeyCache.getIfPresent(id);
        CachedItem<T> cachedItem = null;
        String associatedKey = null;
        List<SearchParameter> parameters = null;

        if (uniqueId != null) {
            cachedItem = localPrimaryCache.getIfPresent(uniqueId);
            if (cachedItem != null) {
                associatedKey = cachedItem.getStringKey();
                parameters = cachedItem.getParameters();
            }
        }

        // If not found in local cache, try to get information from remote caches
        if (cachedItem == null && redisCache != null) {
            try {
                Optional<T> remoteItem = redisCache.get(id, (Class<T>) Object.class);
                if (remoteItem.isPresent()) {
                    // We found it in remote cache, but we need the full cached item
                    // This is a limitation - we should enhance the remote cache to return metadata
                    // For now, we'll proceed with what we have
                }
            } catch (Exception e) {
                logger.warn("Error checking remote cache during ID invalidation", e);
            }
        }

        // Remove from all local caches using cascading approach
        // Pass the found information to ensure complete cleanup
        invalidateLocalCaches(uniqueId, associatedKey, id, parameters);

        // Invalidate remote caches with cascading
        invalidateRemoteCaches(associatedKey, id);

        statistics.decrementValues();
    }

    public void invalidate(String key, Long id) {
        if (key == null && id == null) return;

        // Find cached item to get complete information
        CachedItem<T> cachedItem = findInLocalCache(key, id);
        String uniqueId = null;
        List<SearchParameter> parameters = null;

        if (cachedItem != null) {
            uniqueId = cachedItem.generateUniqueId();
            parameters = cachedItem.getParameters();
        }

        // Remove from all local caches using cascading approach
        invalidateLocalCaches(uniqueId, key, id, parameters);

        // Invalidate remote caches with cascading
        invalidateRemoteCaches(key, id);

        statistics.decrementValues();
    }
    private void invalidateLocalCaches(String uniqueId, String key, Long id, List<SearchParameter> parameters) {
        // Remove from primary cache
        if (uniqueId != null) {
            localPrimaryCache.invalidate(uniqueId);
        }

        // Remove direct key mapping
        if (key != null) {
            localPrimaryCache.invalidate(key);
        }

        // Remove from long key cache
        if (id != null) {
            localLongKeyCache.invalidate(id);
        }

        // Clean up parameter indexes completely
        if (parameters != null && uniqueId != null) {
            removeFromParameterIndexes(parameters, uniqueId);

            // Also remove from parameter cache
            for (SearchParameter param : parameters) {
                String paramKey = param.toKey();
                Set<String> paramSet = localParamCache.getIfPresent(paramKey);
                if (paramSet != null) {
                    paramSet.remove(uniqueId);
                    if (paramSet.isEmpty()) {
                        localParamCache.invalidate(paramKey);
                    } else {
                        localParamCache.put(paramKey, paramSet);
                    }
                }
            }
        }
    }

    private void invalidateRemoteCaches(String key, Long id) {
        // Invalidate Redis cache with cascading
        if (redisCache != null) {
            if (key != null) {
                redisCache.invalidate(key);
            }
            if (id != null) {
                redisCache.invalidate(id);
            }
            // Also try combined invalidation if both are available
            if (key != null && id != null) {
                redisCache.invalidate(key, id);
            }
        }

        // Invalidate Database cache with cascading
        if (databaseCache != null) {
            if (key != null) {
                databaseCache.invalidate(key);
            }
            if (id != null) {
                databaseCache.invalidate(id);
            }
        }
    }

    public void invalidateAll() {
        // Clear local caches
        localPrimaryCache.invalidateAll();
        localLongKeyCache.invalidateAll();
        localParamCache.invalidateAll();
        parameterPatterns.clear();
        
        // Clear remote caches
        if (redisCache != null) {
            redisCache.invalidateAll();
        }
        if (databaseCache != null) {
            databaseCache.invalidateAll();
        }
    }
    
    // ==================== STATISTICS AND MANAGEMENT ====================
    
    public CacheStatistics getStatistics() {
        return statistics;
    }
    
    public void shutdown() {
        if (redisCache != null) {
            redisCache.shutdown();
        }
        if (databaseCache != null) {
            databaseCache.shutdown();
        }
    }
    
    // ==================== PRIVATE HELPER METHODS ====================

    private CachedItem<T> findInLocalCache(String key, Long id) {
        CachedItem<T> cachedItem = null;

        // If we have both key and id, try unique ID first
        if (key != null && id != null) {
            String uniqueId = key + ":" + id;
            cachedItem = localPrimaryCache.getIfPresent(uniqueId);
        }

        // If we have just a key, search for it
        if (cachedItem == null && key != null) {
            // Try direct key lookup first (for items stored without ID)
            cachedItem = localPrimaryCache.getIfPresent(key);

            // If not found, search through all cache entries to find one with matching string key
            if (cachedItem == null) {
                for (CachedItem<T> item : localPrimaryCache.asMap().values()) {
                    if (key.equals(item.getStringKey())) {
                        cachedItem = item;
                        break;
                    }
                }
            }
        }

        // If we have just an ID, try ID-based lookup
        if (cachedItem == null && id != null) {
            String uniqueId = localLongKeyCache.getIfPresent(id);
            if (uniqueId != null) {
                cachedItem = localPrimaryCache.getIfPresent(uniqueId);
            }
        }

        return cachedItem;
    }
    private Optional<T> getFromRemote(String key, Long id, List<SearchParameter> parameters, Class<T> valueType) {
        CacheConfiguration.FallbackStrategy strategy = getFallbackStrategy();
        
        switch (strategy) {
            case REDIS_ONLY:
                return getFromRedis(key, id, parameters, valueType);
            case DATABASE_ONLY:
                return getFromDatabase(key, id, parameters, valueType);
            case REDIS_THEN_DATABASE:
                Optional<T> redisResult = getFromRedis(key, id, parameters, valueType);
                if (redisResult.isPresent()) {
                    cacheLocallyIfEnabled(key, id, parameters, redisResult.get());
                    return redisResult;
                }
                Optional<T> dbResult = getFromDatabase(key, id, parameters, valueType);
                if (dbResult.isPresent()) {
                    cacheLocallyIfEnabled(key, id, parameters, dbResult.get());
                }
                return dbResult;
            case DATABASE_THEN_REDIS:
                Optional<T> dbFirst = getFromDatabase(key, id, parameters, valueType);
                if (dbFirst.isPresent()) {
                    cacheLocallyIfEnabled(key, id, parameters, dbFirst.get());
                    return dbFirst;
                }
                Optional<T> redisSecond = getFromRedis(key, id, parameters, valueType);
                if (redisSecond.isPresent()) {
                    cacheLocallyIfEnabled(key, id, parameters, redisSecond.get());
                }
                return redisSecond;
            default:
                return Optional.empty();
        }
    }
    
    private Optional<T> getFromRedis(String key, Long id, List<SearchParameter> parameters, Class<T> valueType) {
        if (redisCache == null) return Optional.empty();
        
        try {
            if (key != null && id != null) {
                return redisCache.get(key, id, valueType);
            } else if (key != null) {
                return redisCache.get(key, valueType);
            } else if (id != null) {
                return redisCache.get(id, valueType);
            }
        } catch (Exception e) {
            logger.warn("Error accessing Redis cache", e);
        }
        return Optional.empty();
    }
    
    private Optional<T> getFromDatabase(String key, Long id, List<SearchParameter> parameters, Class<T> valueType) {
        if (databaseCache == null) return Optional.empty();
        
        try {
            if (key != null && id != null) {
                return databaseCache.get(key, id, valueType);
            } else if (key != null) {
                return databaseCache.get(key, valueType);
            } else if (id != null) {
                return databaseCache.get(id, valueType);
            }
        } catch (Exception e) {
            logger.warn("Error accessing database cache", e);
        }
        return Optional.empty();
    }
    
    private List<T> searchInLocalCache(List<SearchParameter> parameters) {
        Set<String> patterns = generateHierarchicalPatterns(parameters);
        Set<String> uniqueIds = new HashSet<>();
        
        for (String pattern : patterns) {
            Set<String> patternIds = localParamCache.getIfPresent(pattern);
            if (patternIds != null) {
                uniqueIds.addAll(patternIds);
            }
        }
        
        List<T> results = new ArrayList<>();
        for (String uniqueId : uniqueIds) {
            CachedItem<T> cachedItem = localPrimaryCache.getIfPresent(uniqueId);
            if (cachedItem != null && !cachedItem.isExpired()) {
                results.add(cachedItem.getValue());
            }
        }
        
        return results;
    }
    
    private List<T> searchInRemote(List<SearchParameter> parameters, Class<T> valueType) {
        CacheConfiguration.FallbackStrategy strategy = getFallbackStrategy();
        
        switch (strategy) {
            case REDIS_ONLY:
                return searchInRedis(parameters, valueType);
            case DATABASE_ONLY:
                return searchInDatabase(parameters, valueType);
            case REDIS_THEN_DATABASE:
                List<T> redisResults = searchInRedis(parameters, valueType);
                if (!redisResults.isEmpty()) {
                    cacheSearchResultsLocally(parameters, redisResults);
                    return redisResults;
                }
                List<T> dbResults = searchInDatabase(parameters, valueType);
                if (!dbResults.isEmpty()) {
                    cacheSearchResultsLocally(parameters, dbResults);
                }
                return dbResults;
            case DATABASE_THEN_REDIS:
                List<T> dbFirst = searchInDatabase(parameters, valueType);
                if (!dbFirst.isEmpty()) {
                    cacheSearchResultsLocally(parameters, dbFirst);
                    return dbFirst;
                }
                List<T> redisSecond = searchInRedis(parameters, valueType);
                if (!redisSecond.isEmpty()) {
                    cacheSearchResultsLocally(parameters, redisSecond);
                }
                return redisSecond;
            default:
                return Collections.emptyList();
        }
    }
    
    private List<T> searchInRedis(List<SearchParameter> parameters, Class<T> valueType) {
        if (redisCache == null) return Collections.emptyList();
        
        try {
            return redisCache.get(parameters, valueType);
        } catch (Exception e) {
            logger.warn("Error searching in Redis cache", e);
            return Collections.emptyList();
        }
    }
    
    private List<T> searchInDatabase(List<SearchParameter> parameters, Class<T> valueType) {
        if (databaseCache == null) return Collections.emptyList();
        
        try {
            return databaseCache.get(parameters, valueType);
        } catch (Exception e) {
            logger.warn("Error searching in database cache", e);
            return Collections.emptyList();
        }
    }
    
    private void writeToRemote(String key, Long id, List<SearchParameter> parameters, T value, long ttlMillis) {
        CacheConfiguration.FallbackStrategy strategy = getFallbackStrategy();
        
        try {
            switch (strategy) {
                case REDIS_ONLY:
                case REDIS_THEN_DATABASE:
                    if (redisCache != null) {
                        if (id != null) {
                            redisCache.put(key, id, parameters, value, ttlMillis);
                        } else {
                            redisCache.put(key, parameters, value, ttlMillis);
                        }
                    }
                    break;
                case DATABASE_ONLY:
                case DATABASE_THEN_REDIS:
                    if (databaseCache != null) {
                        databaseCache.put(key, id, parameters, value, ttlMillis, (Class<T>) value.getClass());
                    }
                    break;
            }
        } catch (Exception e) {
            logger.warn("Error writing to remote cache", e);
        }
    }
    
    private void writeToRemoteAsync(CachedItem<T> cachedItem) {
        CompletableFuture.runAsync(() -> {
            try {
                writeToRemote(cachedItem.getStringKey(), cachedItem.getLongKey(), 
                    cachedItem.getParameters(), cachedItem.getValue(), cachedItem.getTtl());
            } catch (Exception e) {
                logger.warn("Error in async write to remote cache", e);
            }
        });
    }
    
    private void cacheLocallyIfEnabled(String key, Long id, List<SearchParameter> parameters, T value) {
        if (config.isLocalCacheEnabled()) {
            CachedItem<T> cachedItem = new CachedItem<>(key, id, value, 
                parameters != null ? parameters : Collections.emptyList(), 
                config.getLocalCacheTtlMillis());
            
            String uniqueId = cachedItem.generateUniqueId();
            localPrimaryCache.put(uniqueId, cachedItem);
            
            if (id != null) {
                localLongKeyCache.put(id, uniqueId);
            }
            
            if (parameters != null && !parameters.isEmpty()) {
                updateLocalParameterIndexes(parameters, uniqueId);
            }
        }
    }
    
    private void cacheSearchResultsLocally(List<SearchParameter> parameters, List<T> results) {
        // This is a simplified approach - in a real implementation, you'd need to store
        // the individual items with their keys and parameters
        logger.debug("Caching {} search results locally", results.size());
    }
    
    private void updateLocalParameterIndexes(List<SearchParameter> parameters, String uniqueId) {
        Set<String> patterns = generateHierarchicalPatterns(parameters);
        
        for (String pattern : patterns) {
            localParamCache.asMap().compute(pattern, (k, v) -> {
                if (v == null) {
                    v = ConcurrentHashMap.newKeySet();
                }
                v.add(uniqueId);
                return v;
            });
        }
    }

    private void removeFromParameterIndexes(List<SearchParameter> parameters, String uniqueId) {
        if (parameters == null || uniqueId == null) return;

        // Remove from internal parameter patterns map
        for (SearchParameter param : parameters) {
            String paramKey = param.toKey();
            Set<String> patterns = parameterPatterns.get(paramKey);
            if (patterns != null) {
                patterns.remove(uniqueId);
                if (patterns.isEmpty()) {
                    parameterPatterns.remove(paramKey);
                }
            }
        }

        // Generate and remove all hierarchical patterns for this item
        List<SearchParameter> sortedParams = new ArrayList<>(parameters);
        sortedParams.sort(Comparator.comparingInt(SearchParameter::getLevel));

        Set<String> hierarchicalPatterns = generateHierarchicalPatterns(sortedParams);
        for (String pattern : hierarchicalPatterns) {
            Set<String> patternSet = parameterPatterns.get(pattern);
            if (patternSet != null) {
                patternSet.remove(uniqueId);
                if (patternSet.isEmpty()) {
                    parameterPatterns.remove(pattern);
                }
            }
        }
    }

    private Set<String> generateHierarchicalPatterns(List<SearchParameter> sortedParams) {
        Set<String> patterns = new HashSet<>();

        // Generate patterns for each level and combination
        for (int i = 0; i < sortedParams.size(); i++) {
            StringBuilder pattern = new StringBuilder();
            for (int j = 0; j <= i; j++) {
                if (j > 0) pattern.append(":");
                pattern.append(sortedParams.get(j).toKey());
            }
            patterns.add(pattern.toString());
        }

        return patterns;
    }
    
    private CacheConfiguration.FallbackStrategy getFallbackStrategy() {
        CacheContext context = CacheContext.get();
        return (context != null && context.getFallbackStrategy() != null) 
            ? context.getFallbackStrategy() 
            : config.getGlobalFallbackStrategy();
    }
}
