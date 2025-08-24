package ac.h2;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Thread-safe transparent hierarchical cache service with L1 (local) and L2/L3 (remote) caching layers.
 * Provides fine-grained locking for optimal performance under concurrent access.
 *
 * @param <T> The type of objects stored in the cache
 */
public class TransparentHCacheService<T> {
    private static final Logger logger = LoggerFactory.getLogger(TransparentHCacheService.class);

    // Thread safety locks
    private final ReadWriteLock cacheLock = new ReentrantReadWriteLock();
    private final ReadWriteLock parameterPatternLock = new ReentrantReadWriteLock();
    private final ReadWriteLock statisticsLock = new ReentrantReadWriteLock();

    // Shutdown state
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    // Local caches (thread-safe by design)
    private final Cache<String, CachedItem<T>> localPrimaryCache;
    private final Cache<Long, String> localLongKeyCache;
    private final Cache<String, Set<String>> localParamCache;

    // Remote providers (assumed thread-safe)
    private final HierarchicalCacheService<T> redisCache;
    private final DatabaseCacheProvider<T> databaseCache;

    // Configuration (immutable after construction)
    private final CacheConfiguration config;
    private final CacheStatistics statistics;

    // Internal state (thread-safe collections)
    private final Map<String, Set<String>> parameterPatterns = new ConcurrentHashMap<>();

    /**
     * Creates a new TransparentHierarchicalCacheService instance.
     *
     * @param redisCache Redis cache service (can be null if disabled in config)
     * @param databaseCache Database cache provider (can be null if disabled in config)
     * @param config Cache configuration (must not be null)
     * @throws IllegalArgumentException if configuration is invalid
     * @throws IllegalStateException if no cache layers are enabled
     */
    public TransparentHCacheService(
            HierarchicalCacheService<T> redisCache,
            DatabaseCacheProvider<T> databaseCache,
            CacheConfiguration config) {

        // Validate inputs
        validateConstructorInputs(redisCache, databaseCache, config);

        this.redisCache = redisCache;
        this.databaseCache = databaseCache;
        this.config = config;
        this.statistics = new CacheStatistics();

        // Initialize local caches if enabled
        if (config.isLocalCacheEnabled()) {
            this.localPrimaryCache = createLocalPrimaryCache();
            this.localLongKeyCache = createLocalLongKeyCache();
            this.localParamCache = createLocalParamCache();
        } else {
            this.localPrimaryCache = null;
            this.localLongKeyCache = null;
            this.localParamCache = null;
        }

        logger.info("TransparentHierarchicalCacheService initialized with local={}, remote={}, database={}",
                config.isLocalCacheEnabled(), config.isRemoteCacheEnabled(), config.isDatabaseCacheEnabled());
    }

    // ==================== VALIDATION METHODS ====================

    private void validateConstructorInputs(HierarchicalCacheService<T> redisCache,
                                           DatabaseCacheProvider<T> databaseCache,
                                           CacheConfiguration config) {
        if (config == null) {
            throw new IllegalArgumentException("Configuration cannot be null");
        }

        // Validate that at least one cache layer is enabled
        if (!config.isLocalCacheEnabled() && !config.isRemoteCacheEnabled() && !config.isDatabaseCacheEnabled()) {
            throw new IllegalStateException("At least one cache layer must be enabled");
        }

        // Validate TTL values
        if (config.getLocalCacheTtlMillis() < 0) {
            throw new IllegalArgumentException("Local cache TTL cannot be negative");
        }
        if (config.getRemoteCacheTtlMillis() < 0) {
            throw new IllegalArgumentException("Remote cache TTL cannot be negative");
        }

        // Validate cache size
        if (config.getMaxLocalCacheSize() <= 0) {
            throw new IllegalArgumentException("Local cache size must be positive");
        }

        // Validate dependencies
        if (config.isRemoteCacheEnabled() && redisCache == null) {
            throw new IllegalArgumentException("Redis cache service is required when remote cache is enabled");
        }
        if (config.isDatabaseCacheEnabled() && databaseCache == null) {
            throw new IllegalArgumentException("Database cache provider is required when database cache is enabled");
        }
    }

    private Cache<String, CachedItem<T>> createLocalPrimaryCache() {
        return Caffeine.newBuilder()
                .maximumSize(config.getMaxLocalCacheSize())
                .expireAfterWrite(config.getLocalCacheTtlMillis(), TimeUnit.MILLISECONDS)
                .recordStats()
                .build();
    }

    private Cache<Long, String> createLocalLongKeyCache() {
        return Caffeine.newBuilder()
                .maximumSize(config.getMaxLocalCacheSize())
                .expireAfterWrite(config.getLocalCacheTtlMillis(), TimeUnit.MILLISECONDS)
                .recordStats()
                .build();
    }

    private Cache<String, Set<String>> createLocalParamCache() {
        return Caffeine.newBuilder()
                .maximumSize(Math.min(config.getMaxLocalCacheSize() * 2, 100000L)) // Bounded parameter cache
                .expireAfterWrite(config.getRemoteCacheTtlMillis(), TimeUnit.MILLISECONDS)
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
        // Input validation
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        if (ttlMillis < 0) {
            throw new IllegalArgumentException("TTL cannot be negative");
        }

        checkNotShutdown();

        // Normalize parameters (defensive copy)
        List<SearchParameter> normalizedParams = parameters != null ?
                new ArrayList<>(parameters) : Collections.emptyList();

        try {
            // Cache locally if enabled (thread-safe)
            if (config.isLocalCacheEnabled()) {
                cacheLocallyIfEnabled(key, id, normalizedParams, value, ttlMillis);
            }

            // Write to remote caches (async, thread-safe)
            writeToRemoteCachesWithErrorHandling(key, id, normalizedParams, value, ttlMillis);

            // Update statistics atomically
            statistics.incrementValues();

            logger.debug("Successfully cached item with key={}, id={}, paramCount={}", key, id, normalizedParams.size());

        } catch (Exception e) {
            logger.error("Error caching item with key={}, id={}: {}", key, id, e.getMessage(), e);
            throw new RuntimeException("Failed to cache item", e);
        }
    }

    // ==================== GET OPERATIONS ====================

    public Optional<T> get(String key, Class<T> valueType) {
        if (key == null || valueType == null) {
            return Optional.empty();
        }

        checkNotShutdown();

        try {
            CacheContext context = CacheContext.get();
            boolean skipLocalCache = context != null && context.isSkipLocalCache();
            boolean forceRefresh = context != null && context.isForceRefresh();

            // Check local cache first unless skipped or force refresh
            if (!skipLocalCache && !forceRefresh && config.isLocalCacheEnabled()) {
                Optional<T> localResult = getFromLocalCache(key, null);
                if (localResult.isPresent()) {
                    statistics.incrementL1Hits();
                    return localResult;
                }
                statistics.incrementL1Misses();
            }

            // Check remote caches
            Optional<T> remoteResult = getFromRemoteCachesWithFallback(key, null, Collections.emptyList(), valueType);

            // Cache in local if found and local cache is enabled
            if (remoteResult.isPresent() && config.isLocalCacheEnabled() && !skipLocalCache) {
                cacheLocallyIfEnabled(key, null, Collections.emptyList(), remoteResult.get(), config.getLocalCacheTtlMillis());
            }

            return remoteResult;

        } catch (Exception e) {
            logger.warn("Error retrieving item with key={}: {}", key, e.getMessage());
            return Optional.empty();
        }
    }

    public Optional<T> get(String key, Long id, Class<T> valueType) {
        if (key == null || id == null || valueType == null) {
            return Optional.empty();
        }

        checkNotShutdown();

        try {
            CacheContext context = CacheContext.get();
            boolean skipLocalCache = context != null && context.isSkipLocalCache();
            boolean forceRefresh = context != null && context.isForceRefresh();

            // Check local cache first
            if (!skipLocalCache && !forceRefresh && config.isLocalCacheEnabled()) {
                Optional<T> localResult = getFromLocalCache(key, id);
                if (localResult.isPresent()) {
                    statistics.incrementL1Hits();
                    return localResult;
                }
                statistics.incrementL1Misses();
            }

            // Check remote caches
            Optional<T> remoteResult = getFromRemoteCachesWithFallback(key, id, Collections.emptyList(), valueType);

            // Cache locally if found
            if (remoteResult.isPresent() && config.isLocalCacheEnabled() && !skipLocalCache) {
                cacheLocallyIfEnabled(key, id, Collections.emptyList(), remoteResult.get(), config.getLocalCacheTtlMillis());
            }

            return remoteResult;

        } catch (Exception e) {
            logger.warn("Error retrieving item with key={}, id={}: {}", key, id, e.getMessage());
            return Optional.empty();
        }
    }

    public Optional<T> get(Long id, Class<T> valueType) {
        if (id == null || valueType == null) {
            return Optional.empty();
        }

        checkNotShutdown();

        try {
            CacheContext context = CacheContext.get();
            boolean skipLocalCache = context != null && context.isSkipLocalCache();
            boolean forceRefresh = context != null && context.isForceRefresh();

            // Check local cache first
            if (!skipLocalCache && !forceRefresh && config.isLocalCacheEnabled()) {
                Optional<T> localResult = getFromLocalCacheById(id);
                if (localResult.isPresent()) {
                    statistics.incrementL1Hits();
                    return localResult;
                }
                statistics.incrementL1Misses();
            }

            // Check remote caches
            Optional<T> remoteResult = getFromRemoteCachesWithFallback(null, id, Collections.emptyList(), valueType);

            // Note: Cannot easily cache with just ID in local cache without key
            return remoteResult;

        } catch (Exception e) {
            logger.warn("Error retrieving item with id={}: {}", id, e.getMessage());
            return Optional.empty();
        }
    }

    public List<T> get(List<SearchParameter> parameters, Class<T> valueType) {
        if (parameters == null || parameters.isEmpty() || valueType == null) {
            return Collections.emptyList();
        }

        checkNotShutdown();

        try {
            CacheContext context = CacheContext.get();
            boolean skipLocalCache = context != null && context.isSkipLocalCache();
            boolean forceRefresh = context != null && context.isForceRefresh();

            // Check local cache first
            if (!skipLocalCache && !forceRefresh && config.isLocalCacheEnabled()) {
                List<T> localResults = searchInLocalCacheThreadSafe(parameters);
                if (!localResults.isEmpty()) {
                    statistics.incrementL1Hits();
                    return localResults;
                }
                statistics.incrementL1Misses();
            }

            // Search in remote caches
            List<T> remoteResults = searchInRemoteCachesWithFallback(parameters, valueType);

            // Cache results locally if found (parameter-based caching is complex, skipped for now)
            return remoteResults;

        } catch (Exception e) {
            logger.warn("Error searching with parameters: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    public List<T> get(String key, List<SearchParameter> parameters, Class<T> valueType) {
        if (key == null || valueType == null) {
            return Collections.emptyList();
        }

        // Combine key with parameters for search
        List<SearchParameter> combinedParams = new ArrayList<>();
        if (parameters != null) {
            combinedParams.addAll(parameters);
        }
        combinedParams.add(new SearchParameter("_key", key, 0));

        return get(combinedParams, valueType);
    }

    // ==================== GET OR COMPUTE OPERATIONS ====================

    public T getOrCompute(String key, Class<T> valueType, Supplier<T> supplier) {
        if (key == null || valueType == null || supplier == null) {
            throw new IllegalArgumentException("Key, valueType, and supplier cannot be null");
        }

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
        if (key == null || id == null || valueType == null || supplier == null) {
            throw new IllegalArgumentException("Key, id, valueType, and supplier cannot be null");
        }

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
        if (id == null || valueType == null || supplier == null) {
            throw new IllegalArgumentException("Id, valueType, and supplier cannot be null");
        }

        Optional<T> cached = get(id, valueType);
        if (cached.isPresent()) {
            return cached.get();
        }

        T computed = supplier.get();
        if (computed != null) {
            // Generate a key based on ID for storage
            put("id:" + id, id, Collections.emptyList(), computed);
        }
        return computed;
    }

    public List<T> getOrCompute(List<SearchParameter> parameters, Class<T> valueType, Supplier<List<T>> supplier) {
        if (parameters == null || valueType == null || supplier == null) {
            throw new IllegalArgumentException("Parameters, valueType, and supplier cannot be null");
        }

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
            throw new IllegalArgumentException("Key and ID cannot be null");
        }

        checkNotShutdown();

        cacheLock.writeLock().lock();
        try {
            // Link in remote caches first
            linkInRemoteCaches(key, id);

            // Update local cache mapping if enabled
            if (config.isLocalCacheEnabled()) {
                updateLocalCacheLinking(key, id);
            }

            logger.debug("Successfully linked key={} with id={}", key, id);

        } catch (Exception e) {
            logger.error("Error linking key={} with id={}: {}", key, id, e.getMessage(), e);
            throw new RuntimeException("Failed to link key with ID", e);
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    public void link(String key, List<SearchParameter> parameters) {
        if (key == null || parameters == null || parameters.isEmpty()) {
            throw new IllegalArgumentException("Key and parameters cannot be null or empty");
        }

        checkNotShutdown();

        parameterPatternLock.writeLock().lock();
        try {
            // Link in remote caches
            linkInRemoteCaches(key, parameters);

            // Update local parameter mapping if enabled
            if (config.isLocalCacheEnabled()) {
                updateLocalParameterLinking(key, parameters);
            }

            logger.debug("Successfully linked key={} with {} parameters", key, parameters.size());

        } catch (Exception e) {
            logger.error("Error linking key={} with parameters: {}", key, e.getMessage(), e);
            throw new RuntimeException("Failed to link key with parameters", e);
        } finally {
            parameterPatternLock.writeLock().unlock();
        }
    }

    public void link(Long id, List<SearchParameter> parameters) {
        if (id == null || parameters == null || parameters.isEmpty()) {
            throw new IllegalArgumentException("ID and parameters cannot be null or empty");
        }

        checkNotShutdown();

        parameterPatternLock.writeLock().lock();
        try {
            // Link in remote caches
            linkInRemoteCaches(id, parameters);

            // Update local parameter mapping if enabled
            if (config.isLocalCacheEnabled()) {
                updateLocalParameterLinkingById(id, parameters);
            }

            logger.debug("Successfully linked id={} with {} parameters", id, parameters.size());

        } catch (Exception e) {
            logger.error("Error linking id={} with parameters: {}", id, e.getMessage(), e);
            throw new RuntimeException("Failed to link ID with parameters", e);
        } finally {
            parameterPatternLock.writeLock().unlock();
        }
    }

    // ==================== INVALIDATION OPERATIONS ====================

    public void invalidate(String key, Long id) {
        if (key == null && id == null) {
            return;
        }

        checkNotShutdown();

        cacheLock.writeLock().lock();
        try {
            // Find cached item info first
            CachedItemInfo itemInfo = findCachedItemInfo(key, id);

            // Invalidate local caches
            if (config.isLocalCacheEnabled()) {
                invalidateLocalCachesThreadSafe(itemInfo.uniqueId, key, id, itemInfo.parameters);
            }

            // Invalidate remote caches
            invalidateRemoteCachesWithErrorHandling(key, id);

            statistics.decrementValues();

            logger.debug("Successfully invalidated item with key={}, id={}", key, id);

        } catch (Exception e) {
            logger.warn("Error during invalidation of key={}, id={}: {}", key, id, e.getMessage());
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    public void invalidate(String key) {
        if (key == null) {
            return;
        }

        cacheLock.writeLock().lock();
        try {
            // Find associated information
            CachedItemInfo itemInfo = findCachedItemInfoByKey(key);

            // Invalidate local caches
            if (config.isLocalCacheEnabled()) {
                invalidateLocalCachesThreadSafe(itemInfo.uniqueId, key, itemInfo.associatedId, itemInfo.parameters);
            }

            // Invalidate remote caches
            invalidateRemoteCachesWithErrorHandling(key, itemInfo.associatedId);

            statistics.decrementValues();

            logger.debug("Successfully invalidated key={}", key);

        } catch (Exception e) {
            logger.warn("Error during invalidation of key={}: {}", key, e.getMessage());
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    public void invalidate(Long id) {
        if (id == null) {
            return;
        }

        cacheLock.writeLock().lock();
        try {
            // Find associated information
            CachedItemInfo itemInfo = findCachedItemInfoById(id);

            // Invalidate local caches
            if (config.isLocalCacheEnabled()) {
                invalidateLocalCachesThreadSafe(itemInfo.uniqueId, itemInfo.associatedKey, id, itemInfo.parameters);
            }

            // Invalidate remote caches
            invalidateRemoteCachesWithErrorHandling(itemInfo.associatedKey, id);

            statistics.decrementValues();

            logger.debug("Successfully invalidated id={}", id);

        } catch (Exception e) {
            logger.warn("Error during invalidation of id={}: {}", id, e.getMessage());
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    public void invalidateAll() {
        checkNotShutdown();

        cacheLock.writeLock().lock();
        parameterPatternLock.writeLock().lock();
        try {
            // Clear local caches
            if (config.isLocalCacheEnabled()) {
                clearLocalCaches();
            }

            // Clear remote caches
            clearRemoteCaches();

            // Reset statistics
            statistics.reset();

            logger.info("Successfully invalidated all cache entries");

        } catch (Exception e) {
            logger.error("Error during invalidate all operation: {}", e.getMessage(), e);
        } finally {
            parameterPatternLock.writeLock().unlock();
            cacheLock.writeLock().unlock();
        }
    }

    // ==================== STATISTICS AND MANAGEMENT ====================

    public CacheStatistics getStatistics() {
        return statistics;
    }

    public void shutdown() {
        if (isShutdown.compareAndSet(false, true)) {
            logger.info("Shutting down TransparentHierarchicalCacheService...");

            cacheLock.writeLock().lock();
            parameterPatternLock.writeLock().lock();
            try {
                // Clear all caches
                invalidateAll();

                // Shutdown remote services
                shutdownRemoteServices();

                // Clean up ThreadLocal variables
                cleanupThreadLocalVariables();

                // Clear internal state
                parameterPatterns.clear();

                logger.info("TransparentHierarchicalCacheService shutdown completed");

            } catch (Exception e) {
                logger.error("Error during shutdown: {}", e.getMessage(), e);
            } finally {
                parameterPatternLock.writeLock().unlock();
                cacheLock.writeLock().unlock();
            }
        }
    }

    // ==================== PRIVATE HELPER METHODS ====================

    private void checkNotShutdown() {
        if (isShutdown.get()) {
            throw new IllegalStateException("Cache service has been shut down");
        }
    }

    private Optional<T> getFromLocalCache(String key, Long id) {
        cacheLock.readLock().lock();
        try {
            CachedItem<T> cachedItem = findInLocalCacheThreadSafe(key, id);
            if (cachedItem != null && !cachedItem.isExpired()) {
                return Optional.of(cachedItem.getValue());
            }
        } finally {
            cacheLock.readLock().unlock();
        }
        return Optional.empty();
    }

    private Optional<T> getFromLocalCacheById(Long id) {
        cacheLock.readLock().lock();
        try {
            if (localLongKeyCache != null) {
                String uniqueId = localLongKeyCache.getIfPresent(id);
                if (uniqueId != null && localPrimaryCache != null) {
                    CachedItem<T> cachedItem = localPrimaryCache.getIfPresent(uniqueId);
                    if (cachedItem != null && !cachedItem.isExpired()) {
                        return Optional.of(cachedItem.getValue());
                    }
                }
            }
        } finally {
            cacheLock.readLock().unlock();
        }
        return Optional.empty();
    }

    private CachedItem<T> findInLocalCacheThreadSafe(String key, Long id) {
        if (!config.isLocalCacheEnabled() || localPrimaryCache == null) {
            return null;
        }

        // Try unique ID first if both key and ID are provided
        if (key != null && id != null) {
            String uniqueId = key + ":" + id;
            CachedItem<T> cachedItem = localPrimaryCache.getIfPresent(uniqueId);
            if (cachedItem != null) {
                return cachedItem;
            }
        }

        // Try by key only
        if (key != null) {
            CachedItem<T> cachedItem = localPrimaryCache.getIfPresent(key);
            if (cachedItem != null) {
                return cachedItem;
            }

            // Search through all entries for matching string key (expensive, but thorough)
            for (CachedItem<T> item : localPrimaryCache.asMap().values()) {
                if (key.equals(item.getStringKey())) {
                    return item;
                }
            }
        }

        // Try by ID via ID mapping
        if (id != null && localLongKeyCache != null) {
            String uniqueId = localLongKeyCache.getIfPresent(id);
            if (uniqueId != null) {
                return localPrimaryCache.getIfPresent(uniqueId);
            }
        }

        return null;
    }

    private List<T> searchInLocalCacheThreadSafe(List<SearchParameter> parameters) {
        cacheLock.readLock().lock();
        parameterPatternLock.readLock().lock();
        try {
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

        } finally {
            parameterPatternLock.readLock().unlock();
            cacheLock.readLock().unlock();
        }
    }

    private Optional<T> getFromRemoteCachesWithFallback(String key, Long id, List<SearchParameter> parameters, Class<T> valueType) {
        CacheConfiguration.FallbackStrategy strategy = getFallbackStrategy();

        switch (strategy) {
            case REDIS_THEN_DATABASE:
                return getFromRemoteWithRedisFirst(key, id, parameters, valueType);
            case DATABASE_THEN_REDIS:
                return getFromRemoteWithDatabaseFirst(key, id, parameters, valueType);
            case REDIS_ONLY:
                return getFromRedisOnly(key, id, parameters, valueType);
            case DATABASE_ONLY:
                return getFromDatabaseOnly(key, id, parameters, valueType);
            default:
                return Optional.empty();
        }
    }

    private Optional<T> getFromRemoteWithRedisFirst(String key, Long id, List<SearchParameter> parameters, Class<T> valueType) {
        if (config.isRemoteCacheEnabled()) {
            Optional<T> redisResult = getFromRedisWithErrorHandling(key, id, parameters, valueType);
            if (redisResult.isPresent()) {
                return redisResult;
            }
        }

        if (config.isDatabaseCacheEnabled()) {
            return getFromDatabaseWithErrorHandling(key, id, parameters, valueType);
        }

        return Optional.empty();
    }

    private Optional<T> getFromRemoteWithDatabaseFirst(String key, Long id, List<SearchParameter> parameters, Class<T> valueType) {
        if (config.isDatabaseCacheEnabled()) {
            Optional<T> dbResult = getFromDatabaseWithErrorHandling(key, id, parameters, valueType);
            if (dbResult.isPresent()) {
                // Write back to Redis for future hits
                if (config.isRemoteCacheEnabled()) {
                    writeToRedisWithErrorHandling(key, id, parameters, dbResult.get(), config.getRemoteCacheTtlMillis());
                }
                return dbResult;
            }
        }

        if (config.isRemoteCacheEnabled()) {
            return getFromRedisWithErrorHandling(key, id, parameters, valueType);
        }

        return Optional.empty();
    }

    private Optional<T> getFromRedisOnly(String key, Long id, List<SearchParameter> parameters, Class<T> valueType) {
        if (config.isRemoteCacheEnabled()) {
            return getFromRedisWithErrorHandling(key, id, parameters, valueType);
        }
        return Optional.empty();
    }

    private Optional<T> getFromDatabaseOnly(String key, Long id, List<SearchParameter> parameters, Class<T> valueType) {
        if (config.isDatabaseCacheEnabled()) {
            return getFromDatabaseWithErrorHandling(key, id, parameters, valueType);
        }
        return Optional.empty();
    }

    private Optional<T> getFromRedisWithErrorHandling(String key, Long id, List<SearchParameter> parameters, Class<T> valueType) {
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
            logger.warn("Error accessing Redis cache for key={}, id={}: {}", key, id, e.getMessage());
            statistics.incrementL2Errors();
            return Optional.empty();
        }
    }

    private Optional<T> getFromDatabaseWithErrorHandling(String key, Long id, List<SearchParameter> parameters, Class<T> valueType) {
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
            } else {
                statistics.incrementL3Misses();
            }

            return result;

        } catch (Exception e) {
            logger.warn("Error accessing database cache for key={}, id={}: {}", key, id, e.getMessage());
            statistics.incrementL3Errors();
            return Optional.empty();
        }
    }

    private List<T> searchInRemoteCachesWithFallback(List<SearchParameter> parameters, Class<T> valueType) {
        CacheConfiguration.FallbackStrategy strategy = getFallbackStrategy();

        switch (strategy) {
            case REDIS_THEN_DATABASE:
                return searchInRemoteWithRedisFirst(parameters, valueType);
            case DATABASE_THEN_REDIS:
                return searchInRemoteWithDatabaseFirst(parameters, valueType);
            case REDIS_ONLY:
                return searchInRedisOnly(parameters, valueType);
            case DATABASE_ONLY:
                return searchInDatabaseOnly(parameters, valueType);
            default:
                return Collections.emptyList();
        }
    }

    private List<T> searchInRemoteWithRedisFirst(List<SearchParameter> parameters, Class<T> valueType) {
        if (config.isRemoteCacheEnabled()) {
            List<T> redisResults = searchInRedisWithErrorHandling(parameters, valueType);
            if (!redisResults.isEmpty()) {
                return redisResults;
            }
        }

        if (config.isDatabaseCacheEnabled()) {
            return searchInDatabaseWithErrorHandling(parameters, valueType);
        }

        return Collections.emptyList();
    }

    private List<T> searchInRemoteWithDatabaseFirst(List<SearchParameter> parameters, Class<T> valueType) {
        if (config.isDatabaseCacheEnabled()) {
            List<T> dbResults = searchInDatabaseWithErrorHandling(parameters, valueType);
            if (!dbResults.isEmpty()) {
                return dbResults;
            }
        }

        if (config.isRemoteCacheEnabled()) {
            return searchInRedisWithErrorHandling(parameters, valueType);
        }

        return Collections.emptyList();
    }

    private List<T> searchInRedisOnly(List<SearchParameter> parameters, Class<T> valueType) {
        if (config.isRemoteCacheEnabled()) {
            return searchInRedisWithErrorHandling(parameters, valueType);
        }
        return Collections.emptyList();
    }

    private List<T> searchInDatabaseOnly(List<SearchParameter> parameters, Class<T> valueType) {
        if (config.isDatabaseCacheEnabled()) {
            return searchInDatabaseWithErrorHandling(parameters, valueType);
        }
        return Collections.emptyList();
    }

    private List<T> searchInRedisWithErrorHandling(List<SearchParameter> parameters, Class<T> valueType) {
        try {
            List<T> results = redisCache.get(parameters, valueType);
            if (!results.isEmpty()) {
                statistics.incrementL2Hits();
            } else {
                statistics.incrementL2Misses();
            }
            return results;

        } catch (Exception e) {
            logger.warn("Error searching Redis cache with parameters: {}", e.getMessage());
            statistics.incrementL2Errors();
            return Collections.emptyList();
        }
    }

    private List<T> searchInDatabaseWithErrorHandling(List<SearchParameter> parameters, Class<T> valueType) {
        try {
            List<T> results = databaseCache.get(parameters, valueType);
            if (!results.isEmpty()) {
                statistics.incrementL3Hits();
            } else {
                statistics.incrementL3Misses();
            }
            return results;

        } catch (Exception e) {
            logger.warn("Error searching database cache with parameters: {}", e.getMessage());
            statistics.incrementL3Errors();
            return Collections.emptyList();
        }
    }

    private void writeToRemoteCachesWithErrorHandling(String key, Long id, List<SearchParameter> parameters, T value, long ttlMillis) {
        // Write to Redis
        if (config.isRemoteCacheEnabled()) {
            writeToRedisWithErrorHandling(key, id, parameters, value, ttlMillis);
        }

        // Write to database
        if (config.isDatabaseCacheEnabled()) {
            writeToDatabaseWithErrorHandling(key, id, parameters, value, ttlMillis);
        }
    }

    private void writeToRedisWithErrorHandling(String key, Long id, List<SearchParameter> parameters, T value, long ttlMillis) {
        try {
            if (id != null) {
                redisCache.put(key, id, parameters, value, ttlMillis);
            } else {
                redisCache.put(key, parameters, value, ttlMillis);
            }
            statistics.incrementL2Puts();

        } catch (Exception e) {
            logger.warn("Error writing to Redis cache for key={}, id={}: {}", key, id, e.getMessage());
            statistics.incrementL2Errors();
        }
    }

    private void writeToDatabaseWithErrorHandling(String key, Long id, List<SearchParameter> parameters, T value, long ttlMillis) {
        try {
            if (id != null) {
                databaseCache.put(key, id, parameters, value, ttlMillis);
            } else {
                databaseCache.put(key, parameters, value, ttlMillis);
            }
            statistics.incrementL3Puts();

        } catch (Exception e) {
            logger.warn("Error writing to database cache for key={}, id={}: {}", key, id, e.getMessage());
            statistics.incrementL3Errors();
        }
    }

    private void cacheLocallyIfEnabled(String key, Long id, List<SearchParameter> parameters, T value, long ttlMillis) {
        if (!config.isLocalCacheEnabled() || localPrimaryCache == null) {
            return;
        }

        cacheLock.writeLock().lock();
        try {
            // Create cached item
            CachedItem<T> cachedItem = new CachedItem<>(key, id, value, parameters, ttlMillis);
            String uniqueId = cachedItem.generateUniqueId();

            // Store in primary cache
            localPrimaryCache.put(uniqueId, cachedItem);
            statistics.incrementL1Puts();

            // Store by key for direct lookups if no ID
            if (key != null && id == null) {
                localPrimaryCache.put(key, cachedItem);
            }

            // Store ID mapping if provided
            if (id != null && localLongKeyCache != null) {
                localLongKeyCache.put(id, uniqueId);
            }

            // Update parameter indexes
            updateLocalParameterIndexesThreadSafe(parameters, uniqueId);

        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    private void updateLocalParameterIndexesThreadSafe(List<SearchParameter> parameters, String uniqueId) {
        if (parameters == null || parameters.isEmpty() || localParamCache == null) {
            return;
        }

        parameterPatternLock.writeLock().lock();
        try {
            Set<String> patterns = generateHierarchicalPatterns(parameters);

            for (String pattern : patterns) {
                Set<String> ids = localParamCache.getIfPresent(pattern);
                if (ids == null) {
                    ids = new HashSet<>();
                } else {
                    ids = new HashSet<>(ids); // Create mutable copy
                }
                ids.add(uniqueId);
                localParamCache.put(pattern, ids);
            }

            // Store pattern mapping for cleanup (thread-safe map)
            parameterPatterns.computeIfAbsent(uniqueId, k -> ConcurrentHashMap.newKeySet()).addAll(patterns);

        } finally {
            parameterPatternLock.writeLock().unlock();
        }
    }

    private void linkInRemoteCaches(String key, Long id) {
        if (config.isRemoteCacheEnabled()) {
            try {
                redisCache.link(key, id);
            } catch (Exception e) {
                logger.warn("Error linking in Redis cache for key={}, id={}: {}", key, id, e.getMessage());
            }
        }

        if (config.isDatabaseCacheEnabled()) {
            try {
                databaseCache.link(key, id);
            } catch (Exception e) {
                logger.warn("Error linking in database cache for key={}, id={}: {}", key, id, e.getMessage());
            }
        }
    }

    private void linkInRemoteCaches(String key, List<SearchParameter> parameters) {
        if (config.isRemoteCacheEnabled()) {
            try {
                redisCache.link(key, parameters);
            } catch (Exception e) {
                logger.warn("Error linking in Redis cache for key={}: {}", key, e.getMessage());
            }
        }

        if (config.isDatabaseCacheEnabled()) {
            try {
                databaseCache.link(key, parameters);
            } catch (Exception e) {
                logger.warn("Error linking in database cache for key={}: {}", key, e.getMessage());
            }
        }
    }

    private void linkInRemoteCaches(Long id, List<SearchParameter> parameters) {
        if (config.isRemoteCacheEnabled()) {
            try {
                redisCache.link(id, parameters);
            } catch (Exception e) {
                logger.warn("Error linking in Redis cache for id={}: {}", id, e.getMessage());
            }
        }

        if (config.isDatabaseCacheEnabled()) {
            try {
                databaseCache.link(id, parameters);
            } catch (Exception e) {
                logger.warn("Error linking in database cache for id={}: {}", id, e.getMessage());
            }
        }
    }

    private void updateLocalCacheLinking(String key, Long id) {
        CachedItem<T> cachedItem = findInLocalCacheThreadSafe(key, null);
        if (cachedItem != null) {
            CachedItem<T> updatedItem = new CachedItem<>(
                    key, id, cachedItem.getValue(),
                    cachedItem.getParameters(), cachedItem.getTtl());

            String uniqueId = updatedItem.generateUniqueId();
            localPrimaryCache.put(uniqueId, updatedItem);

            if (localLongKeyCache != null) {
                localLongKeyCache.put(id, uniqueId);
            }
        }
    }

    private void updateLocalParameterLinking(String key, List<SearchParameter> parameters) {
        CachedItem<T> cachedItem = findInLocalCacheThreadSafe(key, null);
        if (cachedItem != null) {
            updateLocalParameterIndexesThreadSafe(parameters, cachedItem.generateUniqueId());
        }
    }

    private void updateLocalParameterLinkingById(Long id, List<SearchParameter> parameters) {
        if (localLongKeyCache != null) {
            String uniqueId = localLongKeyCache.getIfPresent(id);
            if (uniqueId != null) {
                updateLocalParameterIndexesThreadSafe(parameters, uniqueId);
            }
        }
    }

    private static class CachedItemInfo {
        final String uniqueId;
        final String associatedKey;
        final Long associatedId;
        final List<SearchParameter> parameters;

        CachedItemInfo(String uniqueId, String associatedKey, Long associatedId, List<SearchParameter> parameters) {
            this.uniqueId = uniqueId;
            this.associatedKey = associatedKey;
            this.associatedId = associatedId;
            this.parameters = parameters != null ? parameters : Collections.emptyList();
        }
    }

    private CachedItemInfo findCachedItemInfo(String key, Long id) {
        CachedItem<T> cachedItem = findInLocalCacheThreadSafe(key, id);
        if (cachedItem != null) {
            return new CachedItemInfo(
                    cachedItem.generateUniqueId(),
                    cachedItem.getStringKey(),
                    cachedItem.getLongKey(),
                    cachedItem.getParameters()
            );
        }
        return new CachedItemInfo(null, key, id, Collections.emptyList());
    }

    private CachedItemInfo findCachedItemInfoByKey(String key) {
        CachedItem<T> cachedItem = null;
        Long associatedId = null;

        if (localPrimaryCache != null) {
            cachedItem = localPrimaryCache.getIfPresent(key);
            if (cachedItem == null) {
                // Search through entries
                for (Map.Entry<String, CachedItem<T>> entry : localPrimaryCache.asMap().entrySet()) {
                    CachedItem<T> item = entry.getValue();
                    if (key.equals(item.getStringKey())) {
                        cachedItem = item;
                        break;
                    }
                }
            }

            if (cachedItem != null) {
                associatedId = cachedItem.getLongKey();
            }
        }

        return new CachedItemInfo(
                cachedItem != null ? cachedItem.generateUniqueId() : null,
                key,
                associatedId,
                cachedItem != null ? cachedItem.getParameters() : Collections.emptyList()
        );
    }

    private CachedItemInfo findCachedItemInfoById(Long id) {
        String key = null;
        String uniqueId = null;
        List<SearchParameter> parameters = Collections.emptyList();

        if (localLongKeyCache != null) {
            uniqueId = localLongKeyCache.getIfPresent(id);
            if (uniqueId != null && localPrimaryCache != null) {
                CachedItem<T> cachedItem = localPrimaryCache.getIfPresent(uniqueId);
                if (cachedItem != null) {
                    key = cachedItem.getStringKey();
                    parameters = cachedItem.getParameters();
                }
            }
        }

        return new CachedItemInfo(uniqueId, key, id, parameters);
    }

    private void invalidateLocalCachesThreadSafe(String uniqueId, String key, Long id, List<SearchParameter> parameters) {
        if (!config.isLocalCacheEnabled()) {
            return;
        }

        // Invalidate by uniqueId
        if (uniqueId != null && localPrimaryCache != null) {
            localPrimaryCache.invalidate(uniqueId);
        }

        // Invalidate by key
        if (key != null && localPrimaryCache != null) {
            localPrimaryCache.invalidate(key);
        }

        // Invalidate by ID
        if (id != null && localLongKeyCache != null) {
            localLongKeyCache.invalidate(id);
        }

        // Remove from parameter indexes
        if (parameters != null && !parameters.isEmpty() && localParamCache != null) {
            String patternKey = uniqueId != null ? uniqueId : (key != null ? key : (id != null ? id.toString() : "unknown"));
            removeFromParameterIndexesThreadSafe(parameters, patternKey);
        }
    }

    private void removeFromParameterIndexesThreadSafe(List<SearchParameter> parameters, String uniqueId) {
        parameterPatternLock.writeLock().lock();
        try {
            Set<String> patterns = parameterPatterns.get(uniqueId);
            if (patterns == null) {
                patterns = generateHierarchicalPatterns(parameters);
            }

            for (String pattern : patterns) {
                Set<String> ids = localParamCache.getIfPresent(pattern);
                if (ids != null) {
                    Set<String> updatedIds = new HashSet<>(ids);
                    updatedIds.remove(uniqueId);

                    if (updatedIds.isEmpty()) {
                        localParamCache.invalidate(pattern);
                    } else {
                        localParamCache.put(pattern, updatedIds);
                    }
                }
            }

            parameterPatterns.remove(uniqueId);

        } finally {
            parameterPatternLock.writeLock().unlock();
        }
    }

    private void invalidateRemoteCachesWithErrorHandling(String key, Long id) {
        // Invalidate in Redis
        if (config.isRemoteCacheEnabled()) {
            try {
                if (key != null) {
                    redisCache.invalidate(key);
                }
                if (id != null) {
                    redisCache.invalidate(id);
                }
                if (key != null && id != null) {
                    redisCache.invalidate(key, id);
                }
            } catch (Exception e) {
                logger.warn("Error invalidating Redis cache for key={}, id={}: {}", key, id, e.getMessage());
            }
        }

        // Invalidate in database
        if (config.isDatabaseCacheEnabled()) {
            try {
                if (key != null) {
                    databaseCache.invalidate(key);
                }
                if (id != null) {
                    databaseCache.invalidate(id);
                }
                if (key != null && id != null) {
                    databaseCache.invalidate(key, id);
                }
            } catch (Exception e) {
                logger.warn("Error invalidating database cache for key={}, id={}: {}", key, id, e.getMessage());
            }
        }
    }

    private void clearLocalCaches() {
        if (localPrimaryCache != null) {
            localPrimaryCache.invalidateAll();
        }
        if (localLongKeyCache != null) {
            localLongKeyCache.invalidateAll();
        }
        if (localParamCache != null) {
            localParamCache.invalidateAll();
        }
        parameterPatterns.clear();
    }

    private void clearRemoteCaches() {
        if (config.isRemoteCacheEnabled() && redisCache != null) {
            try {
                redisCache.invalidateAll();
            } catch (Exception e) {
                logger.warn("Error clearing Redis cache: {}", e.getMessage());
            }
        }

        if (config.isDatabaseCacheEnabled() && databaseCache != null) {
            try {
                databaseCache.invalidateAll();
            } catch (Exception e) {
                logger.warn("Error clearing database cache: {}", e.getMessage());
            }
        }
    }

    private void shutdownRemoteServices() {
        if (config.isRemoteCacheEnabled() && redisCache != null) {
            try {
                redisCache.shutdown();
            } catch (Exception e) {
                logger.warn("Error shutting down Redis cache: {}", e.getMessage());
            }
        }

        if (config.isDatabaseCacheEnabled() && databaseCache != null) {
            try {
                databaseCache.shutdown();
            } catch (Exception e) {
                logger.warn("Error shutting down database cache: {}", e.getMessage());
            }
        }
    }

    private void cleanupThreadLocalVariables() {
        try {
            CacheContext.clear();
        } catch (Exception e) {
            logger.warn("Error cleaning up ThreadLocal variables: {}", e.getMessage());
        }
    }

    private Set<String> generateHierarchicalPatterns(List<SearchParameter> parameters) {
        if (parameters == null || parameters.isEmpty()) {
            return Collections.emptySet();
        }

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