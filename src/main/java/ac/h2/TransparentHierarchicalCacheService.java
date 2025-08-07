package ac.h2;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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

        // Initialize local caches if enabled
        if (config.isLocalCacheEnabled()) {
            this.localPrimaryCache = Caffeine.newBuilder()
                    .maximumSize(config.getMaxLocalCacheSize())
                    .expireAfterWrite(config.getLocalCacheTtlMillis(), TimeUnit.MILLISECONDS)
                    .build();

            this.localLongKeyCache = Caffeine.newBuilder()
                    .maximumSize(config.getMaxLocalCacheSize())
                    .expireAfterWrite(config.getLocalCacheTtlMillis(), TimeUnit.MILLISECONDS)
                    .build();

            this.localParamCache = Caffeine.newBuilder()
                    .maximumSize(config.getMaxLocalCacheSize() * 2) // Parameter cache can be larger
                    .expireAfterWrite(config.getRemoteCacheTtlMillis(), TimeUnit.MILLISECONDS)
                    .build();
        } else {
            this.localPrimaryCache = null;
            this.localLongKeyCache = null;
            this.localParamCache = null;
        }
    }

    // ==================== PUT OPERATIONS ====================

    public void put(String key, List<SearchParameter> parameters, T value) {
        put(key, null, parameters != null ? parameters : Collections.emptyList(), value, config.getRemoteCacheTtlMillis());
    }

    public void put(String key, List<SearchParameter> parameters, T value, long ttlMillis) {
        put(key, null, parameters != null ? parameters : Collections.emptyList(), value, ttlMillis);
    }

    public void put(String key, Long id, List<SearchParameter> parameters, T value) {
        put(key, id, parameters != null ? parameters : Collections.emptyList(), value, config.getRemoteCacheTtlMillis());
    }

    public void put(String key, Long id, List<SearchParameter> parameters, T value, long ttlMillis) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }

        // Normalize parameters
        List<SearchParameter> normalizedParams = parameters != null ?
                parameters : Collections.emptyList();

        // Cache locally if enabled
        cacheLocallyIfEnabled(key, id, normalizedParams, value);

        // Write to remote cache asynchronously if enabled
        writeToRemote(key, id, normalizedParams, value, ttlMillis);

        // Update statistics
        statistics.incrementValues();
    }

    // ==================== GET OPERATIONS ====================

    public Optional<T> get(String key, Class<T> valueType) {
        if (key == null) return Optional.empty();

//        statistics.incrementRequests();

        CacheContext context = CacheContext.get();
        boolean skipLocalCache = context != null && context.isSkipLocalCache();
        boolean forceRefresh = context != null && context.isForceRefresh();

        // Check local cache first unless skipped
        CachedItem<T> cachedItem = null;
        if (!skipLocalCache && !forceRefresh && config.isLocalCacheEnabled()) {
            cachedItem = findInLocalCache(key, null);
            if (cachedItem != null && !cachedItem.isExpired()) {
                statistics.incrementL1Hits();
                return Optional.of(cachedItem.getValue());
            } else {
                statistics.incrementL1Misses();
            }
        }

        // If not in local cache or skipping it, check remote
        Optional<T> remoteResult = getFromRemote(key, null, Collections.emptyList(), valueType);

        // If found in remote, update local cache
        if (remoteResult.isPresent() && config.isLocalCacheEnabled() && !skipLocalCache) {
            cacheLocallyIfEnabled(key, null, Collections.emptyList(), remoteResult.get());
        }

        return remoteResult;
    }

    public Optional<T> get(String key, Long id, Class<T> valueType) {
        if (key == null || id == null) return Optional.empty();

//        statistics.incrementRequests();

        CacheContext context = CacheContext.get();
        boolean skipLocalCache = context != null && context.isSkipLocalCache();
        boolean forceRefresh = context != null && context.isForceRefresh();

        // Check local cache first unless skipped
        CachedItem<T> cachedItem = null;
        if (!skipLocalCache && !forceRefresh && config.isLocalCacheEnabled()) {
            cachedItem = findInLocalCache(key, id);
            if (cachedItem != null && !cachedItem.isExpired()) {
                statistics.incrementL1Hits();
                return Optional.of(cachedItem.getValue());
            } else {
                statistics.incrementL1Misses();
            }
        }

        // If not in local cache or skipping it, check remote
        Optional<T> remoteResult = getFromRemote(key, id, Collections.emptyList(), valueType);

        // If found in remote, update local cache
        if (remoteResult.isPresent() && config.isLocalCacheEnabled() && !skipLocalCache) {
            cacheLocallyIfEnabled(key, id, Collections.emptyList(), remoteResult.get());
        }

        return remoteResult;
    }

    public Optional<T> get(Long id, Class<T> valueType) {
        if (id == null) return Optional.empty();

//        statistics.incrementRequests();

        CacheContext context = CacheContext.get();
        boolean skipLocalCache = context != null && context.isSkipLocalCache();
        boolean forceRefresh = context != null && context.isForceRefresh();

        // Check local cache first unless skipped
        if (!skipLocalCache && !forceRefresh && config.isLocalCacheEnabled()) {
            String uniqueId = localLongKeyCache.getIfPresent(id);
            if (uniqueId != null) {
                CachedItem<T> cachedItem = localPrimaryCache.getIfPresent(uniqueId);
                if (cachedItem != null && !cachedItem.isExpired()) {
                    statistics.incrementL1Hits();
                    return Optional.of(cachedItem.getValue());
                }
            }
            statistics.incrementL1Misses();
        }

        // If not in local cache or skipping it, check remote
        Optional<T> remoteResult = getFromRemote(null, id, Collections.emptyList(), valueType);

        // If found in remote, update local cache
        if (remoteResult.isPresent() && config.isLocalCacheEnabled() && !skipLocalCache) {
            // We need to find the key from the remote result
            // For now, just cache with ID
            // TODO: Improve to get the key from remote if possible
            cacheLocallyIfEnabled(null, id, Collections.emptyList(), remoteResult.get());
        }

        return remoteResult;
    }

    public List<T> get(List<SearchParameter> parameters, Class<T> valueType) {
        if (parameters == null || parameters.isEmpty()) {
            return Collections.emptyList();
        }

//        statistics.incrementRequests();

        CacheContext context = CacheContext.get();
        boolean skipLocalCache = context != null && context.isSkipLocalCache();
        boolean forceRefresh = context != null && context.isForceRefresh();

        // Check local cache first unless skipped
        List<T> localResults = Collections.emptyList();
        if (!skipLocalCache && !forceRefresh && config.isLocalCacheEnabled()) {
            localResults = searchInLocalCache(parameters);
            if (!localResults.isEmpty()) {
                statistics.incrementL1Hits();
                return localResults;
            } else {
                statistics.incrementL1Misses();
            }
        }

        // If not in local cache or skipping it, check remote
        List<T> remoteResults = searchInRemote(parameters, valueType);

        // If found in remote, update local cache
        if (!remoteResults.isEmpty() && config.isLocalCacheEnabled() && !skipLocalCache) {
            cacheSearchResultsLocally(parameters, remoteResults);
        }

        return remoteResults;
    }

    public List<T> get(String key, List<SearchParameter> parameters, Class<T> valueType) {
        if (key == null) {
            return Collections.emptyList();
        }

        // Combine the key with parameters for a more specific search
        List<SearchParameter> combinedParams = new ArrayList<>();
        if (parameters != null) {
            combinedParams.addAll(parameters);
        }
        combinedParams.add(new SearchParameter("_key", key, 0));

        return get(combinedParams, valueType);
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
        if (computed != null) {
            // This is tricky since we only have an ID but no key
            // We'd need a way to determine the key from the computed value
            // For now, we'll store it with a generated key based on the ID
            put("id:" + id, id, Collections.emptyList(), computed);
        }
        return computed;
    }

    public List<T> getOrCompute(List<SearchParameter> parameters, Class<T> valueType, Supplier<List<T>> supplier) {
        List<T> cached = get(parameters, valueType);
        if (!cached.isEmpty()) {
            return cached;
        }

        List<T> computed = supplier.get();
        // Can't easily cache these results since we don't have keys
        return computed != null ? computed : Collections.emptyList();
    }

    // ==================== LINK OPERATIONS ====================

    public void link(String key, Long id) {
        if (key == null || id == null) {
            throw new IllegalArgumentException("Key and ID cannot be null");
        }

        // Link in remote caches
        if (config.isRemoteCacheEnabled()) {
            redisCache.link(key, id);
        }

        if (config.isDatabaseCacheEnabled()) {
            databaseCache.link(key, id);
        }

        // Update local cache mapping
        if (config.isLocalCacheEnabled()) {
            // Find the cached item by key
            CachedItem<T> cachedItem = findInLocalCache(key, null);
            if (cachedItem != null) {
                // Create a new cached item with the ID
                CachedItem<T> updatedItem = new CachedItem<>(
                        key, id, cachedItem.getValue(),
                        cachedItem.getParameters(), cachedItem.getTtl());

                // Update the primary cache
                String uniqueId = updatedItem.generateUniqueId();
                localPrimaryCache.put(uniqueId, updatedItem);

                // Update the ID mapping
                localLongKeyCache.put(id, uniqueId);
            }
        }
    }

    public void link(String key, List<SearchParameter> parameters) {
        if (key == null || parameters == null || parameters.isEmpty()) {
            throw new IllegalArgumentException("Key and parameters cannot be null or empty");
        }

        // Link in remote caches
        if (config.isRemoteCacheEnabled()) {
            redisCache.link(key, parameters);
        }

        if (config.isDatabaseCacheEnabled()) {
            databaseCache.link(key, parameters);
        }

        // Update local cache parameter mapping
        if (config.isLocalCacheEnabled()) {
            // Find the cached item by key
            CachedItem<T> cachedItem = findInLocalCache(key, null);
            if (cachedItem != null) {
                // Update parameter indexes
                updateLocalParameterIndexes(parameters, cachedItem.generateUniqueId());
            }
        }
    }

    public void link(Long id, List<SearchParameter> parameters) {
        if (id == null || parameters == null || parameters.isEmpty()) {
            throw new IllegalArgumentException("ID and parameters cannot be null or empty");
        }

        // Link in remote caches
        if (config.isRemoteCacheEnabled()) {
            redisCache.link(id, parameters);
        }

        if (config.isDatabaseCacheEnabled()) {
            databaseCache.link(id, parameters);
        }

        // Update local cache parameter mapping
        if (config.isLocalCacheEnabled()) {
            // Find the cached item by ID
            String uniqueId = localLongKeyCache.getIfPresent(id);
            if (uniqueId != null) {
                // Update parameter indexes
                updateLocalParameterIndexes(parameters, uniqueId);
            }
        }
    }

    // ==================== INVALIDATION OPERATIONS ====================

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

    public void invalidate(String key) {
        if (key == null) return;

        try {
            // First, find the cached item in local cache to get complete information
            CachedItem<T> cachedItem = localPrimaryCache != null ? localPrimaryCache.getIfPresent(key) : null;
            Long associatedId = null;
            List<SearchParameter> parameters = null;

            if (cachedItem != null) {
                associatedId = cachedItem.getLongKey();
                parameters = cachedItem.getParameters();

                // Remove from local caches
                String uniqueId = cachedItem.generateUniqueId();
                invalidateLocalCaches(uniqueId, key, associatedId, parameters);
            } else if (localPrimaryCache != null) {
                // If not found directly by key, need to search through cached items
                // Find any entry with matching string key
                for (Map.Entry<String, CachedItem<T>> entry : localPrimaryCache.asMap().entrySet()) {
                    CachedItem<T> item = entry.getValue();
                    if (key.equals(item.getStringKey())) {
                        associatedId = item.getLongKey();
                        parameters = item.getParameters();
                        invalidateLocalCaches(entry.getKey(), key, associatedId, parameters);
                        break;
                    }
                }

                // Also check if there's an ID mapping for this key
                if (localLongKeyCache != null) {
                    for (Map.Entry<Long, String> entry : localLongKeyCache.asMap().entrySet()) {
                        if (entry.getValue().equals(key) || entry.getValue().startsWith(key + ":")) {
                            associatedId = entry.getKey();
                            localLongKeyCache.invalidate(associatedId);
                            break;
                        }
                    }
                }
            }

            // Invalidate in remote caches
            redisCache.invalidate(key);
            if (associatedId != null) {
                redisCache.invalidate(associatedId);
            }

            // Invalidate in database
            databaseCache.invalidate(key);
            if (associatedId != null) {
                databaseCache.invalidate(associatedId);
            }

            statistics.decrementValues();
        } catch (Exception e) {
            logger.warn("Error during invalidation of key: {}", key, e);
        }
    }

    public void invalidate(Long id) {
        if (id == null) return;

        try {
            // Find the key associated with this ID from local cache
            String key = null;
            String uniqueId = null;
            List<SearchParameter> parameters = null;

            if (localLongKeyCache != null) {
                uniqueId = localLongKeyCache.getIfPresent(id);
                if (uniqueId != null) {
                    CachedItem<T> cachedItem = localPrimaryCache.getIfPresent(uniqueId);
                    if (cachedItem != null) {
                        key = cachedItem.getStringKey();
                        parameters = cachedItem.getParameters();
                    }
                }
            }

            // Invalidate local caches
            invalidateLocalCaches(uniqueId, key, id, parameters);

            // Invalidate remote caches
            redisCache.invalidate(id);
            if (key != null) {
                redisCache.invalidate(key);
            }

            // Invalidate database
            databaseCache.invalidate(id);
            if (key != null) {
                databaseCache.invalidate(key);
            }

            statistics.decrementValues();
        } catch (Exception e) {
            logger.warn("Error during invalidation of ID: {}", id, e);
        }
    }

    public void invalidateLocalCaches(String uniqueId, String key, Long id, List<SearchParameter> parameters) {
        if (!config.isLocalCacheEnabled()) return;

        // Invalidate by uniqueId if provided
        if (uniqueId != null && localPrimaryCache != null) {
            localPrimaryCache.invalidate(uniqueId);
        }

        // Invalidate by key if provided
        if (key != null && localPrimaryCache != null) {
            localPrimaryCache.invalidate(key);
        }

        // Invalidate by ID if provided
        if (id != null && localLongKeyCache != null) {
            localLongKeyCache.invalidate(id);
        }

        // Remove from parameter indexes
        if (parameters != null && !parameters.isEmpty() && localParamCache != null) {
            removeFromParameterIndexes(parameters, uniqueId != null ? uniqueId : (key != null ? key : id.toString()));
        }
    }

    private void invalidateRemoteCaches(String key, Long id) {
        // Invalidate in Redis if enabled
        if (config.isRemoteCacheEnabled()) {
            if (key != null) {
                redisCache.invalidate(key);
            }
            if (id != null) {
                redisCache.invalidate(id);
            }
            if (key != null && id != null) {
                redisCache.invalidate(key, id);
            }
        }

        // Invalidate in database if enabled
        if (config.isDatabaseCacheEnabled()) {
            if (key != null) {
                databaseCache.invalidate(key);
            }
            if (id != null) {
                databaseCache.invalidate(id);
            }
            if (key != null && id != null) {
                databaseCache.invalidate(key, id);
            }
        }
    }

    public void invalidateAll() {
        // Clear local caches
        if (config.isLocalCacheEnabled()) {
            if (localPrimaryCache != null) localPrimaryCache.invalidateAll();
            if (localLongKeyCache != null) localLongKeyCache.invalidateAll();
            if (localParamCache != null) localParamCache.invalidateAll();
        }

        // Clear remote caches
        if (config.isRemoteCacheEnabled()) {
            redisCache.invalidateAll();
        }

        if (config.isDatabaseCacheEnabled()) {
            databaseCache.invalidateAll();
        }

        // Reset statistics
        statistics.reset();
    }

    // ==================== STATISTICS AND MANAGEMENT ====================

    public CacheStatistics getStatistics() {
        return statistics;
    }

    public void shutdown() {
        invalidateAll();

        if (config.isRemoteCacheEnabled() && redisCache != null) {
            redisCache.shutdown();
        }

        if (config.isDatabaseCacheEnabled() && databaseCache != null) {
            databaseCache.shutdown();
        }
    }

    // ==================== PRIVATE HELPER METHODS ====================

    private CachedItem<T> findInLocalCache(String key, Long id) {
        if (!config.isLocalCacheEnabled() || localPrimaryCache == null) {
            return null;
        }

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
        if (cachedItem == null && id != null && localLongKeyCache != null) {
            String uniqueId = localLongKeyCache.getIfPresent(id);
            if (uniqueId != null) {
                cachedItem = localPrimaryCache.getIfPresent(uniqueId);
            }
        }

        return cachedItem;
    }

    private Optional<T> getFromRemote(String key, Long id, List<SearchParameter> parameters, Class<T> valueType) {
        CacheContext context = CacheContext.get();
        CacheConfiguration.FallbackStrategy strategy = getFallbackStrategy();

        switch (strategy) {
            case REDIS_THEN_DATABASE:
                // Try Redis first, then database
                if (config.isRemoteCacheEnabled()) {
                    Optional<T> redisResult = getFromRedis(key, id, parameters, valueType);
                    if (redisResult.isPresent()) {
                        return redisResult;
                    }
                }

                // If not in Redis or Redis disabled, try database
                if (config.isDatabaseCacheEnabled()) {
                    return getFromDatabase(key, id, parameters, valueType);
                }
                break;

            case DATABASE_THEN_REDIS:
                // Try database first, then Redis
                if (config.isDatabaseCacheEnabled()) {
                    Optional<T> dbResult = getFromDatabase(key, id, parameters, valueType);
                    if (dbResult.isPresent()) {
                        return dbResult;
                    }
                }

                // If not in database or database disabled, try Redis
                if (config.isRemoteCacheEnabled()) {
                    return getFromRedis(key, id, parameters, valueType);
                }
                break;

            case REDIS_ONLY:
                // Only try Redis
                if (config.isRemoteCacheEnabled()) {
                    return getFromRedis(key, id, parameters, valueType);
                }
                break;

            case DATABASE_ONLY:
                // Only try database
                if (config.isDatabaseCacheEnabled()) {
                    return getFromDatabase(key, id, parameters, valueType);
                }
                break;
        }

        return Optional.empty();
    }

    private Optional<T> getFromRedis(String key, Long id, List<SearchParameter> parameters, Class<T> valueType) {
        try {
            Optional<T> result;

            if (key != null && id != null) {
                result = redisCache.get(key, id, valueType);
            } else if (key != null) {
                result = redisCache.get(key, valueType);
            } else if (id != null) {
                result = redisCache.get(id, valueType);
            } else {
                result = Optional.empty();
            }

            if (result.isPresent()) {
                statistics.incrementL2Hits();
            } else {
                statistics.incrementL2Misses();
            }

            return result;
        } catch (Exception e) {
            logger.warn("Error accessing Redis cache: {}", e.getMessage());
            statistics.incrementL2Errors();
            return Optional.empty();
        }
    }

    private Optional<T> getFromDatabase(String key, Long id, List<SearchParameter> parameters, Class<T> valueType) {
        try {
            Optional<T> result;

            if (key != null && id != null) {
                result = databaseCache.get(key, id, valueType);
            } else if (key != null) {
                result = databaseCache.get(key, valueType);
            } else if (id != null) {
                result = databaseCache.get(id, valueType);
            } else {
                result = Optional.empty();
            }

            if (result.isPresent()) {
                statistics.incrementL3Hits();

                // Write back to Redis for future cache hits if enabled
                if (config.isRemoteCacheEnabled() && result.isPresent()) {
                    if (key != null && id != null) {
                        redisCache.put(key, id, parameters, result.get());
                    } else if (key != null) {
                        redisCache.put(key, parameters, result.get());
                    }
                    // Note: can't write back with just an ID
                }
            } else {
                statistics.incrementL3Misses();
            }

            return result;
        } catch (Exception e) {
            logger.warn("Error accessing database cache: {}", e.getMessage());
            statistics.incrementL3Errors();
            return Optional.empty();
        }
    }

    private List<T> searchInLocalCache(List<SearchParameter> parameters) {
        if (!config.isLocalCacheEnabled() || localParamCache == null) {
            return Collections.emptyList();
        }

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
            case REDIS_THEN_DATABASE:
                // Try Redis first, then database
                if (config.isRemoteCacheEnabled()) {
                    List<T> redisResults = searchInRedis(parameters, valueType);
                    if (!redisResults.isEmpty()) {
                        return redisResults;
                    }
                }

                // If nothing in Redis or Redis disabled, try database
                if (config.isDatabaseCacheEnabled()) {
                    return searchInDatabase(parameters, valueType);
                }
                break;

            case DATABASE_THEN_REDIS:
                // Try database first, then Redis
                if (config.isDatabaseCacheEnabled()) {
                    List<T> dbResults = searchInDatabase(parameters, valueType);
                    if (!dbResults.isEmpty()) {
                        return dbResults;
                    }
                }

                // If nothing in database or database disabled, try Redis
                if (config.isRemoteCacheEnabled()) {
                    return searchInRedis(parameters, valueType);
                }
                break;

            case REDIS_ONLY:
                // Only try Redis
                if (config.isRemoteCacheEnabled()) {
                    return searchInRedis(parameters, valueType);
                }
                break;

            case DATABASE_ONLY:
                // Only try database
                if (config.isDatabaseCacheEnabled()) {
                    return searchInDatabase(parameters, valueType);
                }
                break;
        }

        return Collections.emptyList();
    }

    private List<T> searchInRedis(List<SearchParameter> parameters, Class<T> valueType) {
        try {
            List<T> results = redisCache.get(parameters, valueType);

            if (!results.isEmpty()) {
                statistics.incrementL2Hits();
            } else {
                statistics.incrementL2Misses();
            }

            return results;
        } catch (Exception e) {
            logger.warn("Error searching Redis cache: {}", e.getMessage());
            statistics.incrementL2Errors();
            return Collections.emptyList();
        }
    }

    private List<T> searchInDatabase(List<SearchParameter> parameters, Class<T> valueType) {
        try {
            List<T> results = databaseCache.get(parameters, valueType);

            if (!results.isEmpty()) {
                statistics.incrementL3Hits();

                // Write back to Redis for future cache hits if enabled
                if (config.isRemoteCacheEnabled()) {
                    // This is tricky because we don't have keys for these results
                    // For now, we skip write-back for parameter searches
                }
            } else {
                statistics.incrementL3Misses();
            }

            return results;
        } catch (Exception e) {
            logger.warn("Error searching database cache: {}", e.getMessage());
            statistics.incrementL3Errors();
            return Collections.emptyList();
        }
    }

    private void writeToRemote(String key, Long id, List<SearchParameter> parameters, T value, long ttlMillis) {
        // Write to Redis if enabled
        if (config.isRemoteCacheEnabled()) {
            try {
                if (id != null) {
                    redisCache.put(key, id, parameters, value, ttlMillis);
                } else {
                    redisCache.put(key, parameters, value, ttlMillis);
                }
                statistics.incrementL2Puts();
            } catch (Exception e) {
                logger.warn("Error writing to Redis cache: {}", e.getMessage());
                statistics.incrementL2Errors();
            }
        }

        // Write to database if enabled
        if (config.isDatabaseCacheEnabled()) {
            try {
                if (id != null) {
                    databaseCache.put(key, id, parameters, value, ttlMillis);
                } else {
                    databaseCache.put(key, parameters, value, ttlMillis);
                }
                statistics.incrementL3Puts();
            } catch (Exception e) {
                logger.warn("Error writing to database cache: {}", e.getMessage());
                statistics.incrementL3Errors();
            }
        }
    }

    private void writeToRemoteAsync(CachedItem<T> cachedItem) {
        // Could use CompletableFuture.runAsync() here for truly async behavior
        writeToRemote(
                cachedItem.getStringKey(),
                cachedItem.getLongKey(),
                cachedItem.getParameters(),
                cachedItem.getValue(),
                cachedItem.getTtl()
        );
    }

    private void cacheLocallyIfEnabled(String key, Long id, List<SearchParameter> parameters, T value) {
        if (!config.isLocalCacheEnabled() || localPrimaryCache == null) {
            return;
        }

        // Create cached item
        CachedItem<T> cachedItem = new CachedItem<>(key, id, value, parameters, config.getLocalCacheTtlMillis());
        String uniqueId = cachedItem.generateUniqueId();

        // Store in primary cache
        localPrimaryCache.put(uniqueId, cachedItem);
        statistics.incrementL1Puts();

        // If we have a key but no ID, also store it by key for direct lookups
        if (key != null && id == null) {
            localPrimaryCache.put(key, cachedItem);
        }

        // If we have an ID, store the mapping
        if (id != null && localLongKeyCache != null) {
            localLongKeyCache.put(id, uniqueId);
        }

        // Update parameter indexes
        updateLocalParameterIndexes(parameters, uniqueId);
    }

    private void cacheSearchResultsLocally(List<SearchParameter> parameters, List<T> results) {
        // For search results, we don't have keys/IDs, so we can't easily cache them
        // We could cache them with generated keys based on hash of parameters + value
        // but that's not implemented yet
    }

    private void updateLocalParameterIndexes(List<SearchParameter> parameters, String uniqueId) {
        if (parameters == null || parameters.isEmpty() || localParamCache == null) {
            return;
        }

        Set<String> patterns = generateHierarchicalPatterns(parameters);

        for (String pattern : patterns) {
            Set<String> ids = localParamCache.getIfPresent(pattern);
            if (ids == null) {
                ids = new HashSet<>();
            } else {
                // Make a copy since the cached set might be immutable
                ids = new HashSet<>(ids);
            }
            ids.add(uniqueId);
            localParamCache.put(pattern, ids);
        }

        // Store pattern mapping for cleanup
        parameterPatterns.computeIfAbsent(uniqueId, k -> new HashSet<>()).addAll(patterns);
    }

    private void removeFromParameterIndexes(List<SearchParameter> parameters, String uniqueId) {
        if (parameters == null || parameters.isEmpty() || localParamCache == null) {
            return;
        }

        // Get patterns associated with this unique ID
        Set<String> patterns = parameterPatterns.get(uniqueId);
        if (patterns == null) {
            // If we don't have a record, generate them from parameters
            patterns = generateHierarchicalPatterns(parameters);
        }

        // Remove uniqueId from all pattern indexes
        for (String pattern : patterns) {
            Set<String> ids = localParamCache.getIfPresent(pattern);
            if (ids != null) {
                // Make a copy since the cached set might be immutable
                Set<String> updatedIds = new HashSet<>(ids);
                updatedIds.remove(uniqueId);

                if (updatedIds.isEmpty()) {
                    // If no more IDs, remove the pattern entirely
                    localParamCache.invalidate(pattern);
                } else {
                    // Otherwise update with the remaining IDs
                    localParamCache.put(pattern, updatedIds);
                }
            }
        }

        // Remove pattern mapping
        parameterPatterns.remove(uniqueId);
    }

    private Set<String> generateHierarchicalPatterns(List<SearchParameter> parameters) {
        if (parameters == null || parameters.isEmpty()) {
            return Collections.emptySet();
        }

        // Sort parameters by level
        List<SearchParameter> sortedParams = parameters.stream()
                .sorted(Comparator.comparingInt(SearchParameter::getLevel))
                .collect(Collectors.toList());

        Set<String> patterns = new HashSet<>();

        // Generate all possible combinations maintaining hierarchy
        for (int i = 0; i < sortedParams.size(); i++) {
            for (int j = i; j < sortedParams.size(); j++) {
                List<SearchParameter> subList = sortedParams.subList(i, j + 1);
                String pattern = subList.stream()
                        .map(SearchParameter::toKey)
                        .collect(Collectors.joining(">"));
                patterns.add(pattern);
            }
        }

        // Add individual parameter patterns
        for (SearchParameter param : sortedParams) {
            patterns.add(param.toKey());
        }

        return patterns;
    }

    private CacheConfiguration.FallbackStrategy getFallbackStrategy() {
        CacheContext context = CacheContext.get();
        if (context != null && context.getFallbackStrategy() != null) {
            return context.getFallbackStrategy();
        }
        return config.getGlobalFallbackStrategy();
    }
}