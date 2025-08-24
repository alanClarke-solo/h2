package ac.h2;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.util.concurrent.Striped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class TransparentCacheService<T> {
    private static final Logger logger = LoggerFactory.getLogger(TransparentCacheService.class);

    private final Class<T> valueType;

    // Thread safety components
    private final Striped<ReadWriteLock> locks = Striped.lazyWeakReadWriteLock(64);
    private final Object localCacheLock = new Object();
    private final Object statisticsLock = new Object();

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

    public TransparentCacheService(
            Class<T> valueType,
            HierarchicalCacheService<T> redisCache,
            DatabaseCacheProvider<T> databaseCache,
            CacheConfiguration config) {
        this.valueType = valueType;
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

    // ==================== THREAD SAFETY HELPER METHODS ====================

    /**
     * Get read-write lock for a specific key combination.
     */
    private ReadWriteLock getLockForKey(String key, Long id) {
        String lockKey = (key != null ? key : "") + ":" + (id != null ? id.toString() : "");
        return locks.get(lockKey);
    }

    /**
     * Get current value without external locking (assumes caller holds appropriate lock).
     */
    private T getCurrentValue(String key, Long id, List<SearchParameter> parameters, Class<T> valueType) {
        // Check local cache first
        if (config.isLocalCacheEnabled()) {
            CachedItem<T> cachedItem = findInLocalCache(key, id);
            if (cachedItem != null && !cachedItem.isExpired()) {
                return cachedItem.getValue();
            }
        }

        // Check remote caches
        Optional<T> remoteValue = getFromRemote(key, id, parameters, valueType);
        return remoteValue.orElse(null);
    }

    /**
     * Performs atomic put operation across all cache layers.
     */
    private void performAtomicPut(String key, Long id, List<SearchParameter> parameters, T value, long ttlMillis) {
        // Update local cache first (faster, acts as a buffer)
        updateLocalCacheAtomically(key, id, parameters, value);

        // Then update remote caches
        writeToRemote(key, id, parameters, value, ttlMillis);
    }

    /**
     * Thread-safe local cache update.
     */
    private void updateLocalCacheAtomically(String key, Long id, List<SearchParameter> parameters, T value) {
        if (!config.isLocalCacheEnabled()) return;

        synchronized (localCacheLock) {
            // Remove old entries to prevent stale parameter mappings
            invalidateLocalEntriesForKey(key, id);

            // Add new entry
            cacheLocallyIfEnabled(key, id, parameters, value);
        }
    }

    /**
     * Thread-safe statistics update for put operations.
     */
    private void updateStatisticsForPut(T previousValue) {
        synchronized (statisticsLock) {
            if (previousValue == null) {
                statistics.incrementValues(); // New value
            }
            // If previousValue != null, we're replacing, no change in count
        }
    }

    /**
     * Remove local cache entries for a key to prevent stale data.
     */
    private void invalidateLocalEntriesForKey(String key, Long id) {
        if (!config.isLocalCacheEnabled()) return;

        // Find and remove existing cached item
        CachedItem<T> existingItem = findInLocalCache(key, id);
        if (existingItem != null) {
            String uniqueId = existingItem.generateUniqueId();

            // Remove from primary cache
            if (localPrimaryCache != null) {
                localPrimaryCache.invalidate(uniqueId);
                if (key != null) {
                    localPrimaryCache.invalidate(key);
                }
            }

            // Remove from ID mapping
            if (id != null && localLongKeyCache != null) {
                localLongKeyCache.invalidate(id);
            }

            // Remove parameter mappings
            removeFromParameterIndexes(existingItem.getParameters(), uniqueId);
        }
    }

    // ==================== PUT OPERATIONS (THREAD-SAFE) ====================

    /**
     * Puts a value with the given key and parameters.
     * @return the previous value associated with the key, or null if there was no mapping
     */
    public T put(String key, List<SearchParameter> parameters, T value) {
        return put(key, null, parameters != null ? parameters : Collections.emptyList(), value, config.getRemoteCacheTtlMillis());
    }

    /**
     * Puts a value with the given key, parameters and TTL.
     * @return the previous value associated with the key, or null if there was no mapping
     */
    public T put(String key, List<SearchParameter> parameters, T value, long ttlMillis) {
        return put(key, null, parameters != null ? parameters : Collections.emptyList(), value, ttlMillis);
    }

    /**
     * Puts a value with the given key, ID and parameters.
     * @return the previous value associated with the key, or null if there was no mapping
     */
    public T put(String key, Long id, List<SearchParameter> parameters, T value) {
        return put(key, id, parameters != null ? parameters : Collections.emptyList(), value, config.getRemoteCacheTtlMillis());
    }

    /**
     * Thread-safe put operation with previous value return.
     */
    public T put(String key, Long id, List<SearchParameter> parameters, T value, long ttlMillis) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }

        List<SearchParameter> normalizedParams = parameters != null ? parameters : Collections.emptyList();
        ReadWriteLock lock = getLockForKey(key, id);

        // Use write lock for put operations
        lock.writeLock().lock();
        try {
            // Get previous value atomically
            T previousValue = getCurrentValue(key, id, normalizedParams, (Class<T>) value.getClass());

            // Perform the put operation
            performAtomicPut(key, id, normalizedParams, value, ttlMillis);

            // Update statistics
            updateStatisticsForPut(previousValue);

            return previousValue;

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Thread-safe putIfAbsent operation.
     */
    public T putIfAbsent(String key, List<SearchParameter> parameters, T value) {
        return putIfAbsent(key, null, parameters, value, config.getRemoteCacheTtlMillis());
    }

    /**
     * Thread-safe putIfAbsent operation.
     */
    public T putIfAbsent(String key, Long id, List<SearchParameter> parameters, T value) {
        return putIfAbsent(key, id, parameters, value, config.getRemoteCacheTtlMillis());
    }

    /**
     * Thread-safe putIfAbsent operation.
     */
    public T putIfAbsent(String key, Long id, List<SearchParameter> parameters, T value, long ttlMillis) {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and value cannot be null");
        }

        List<SearchParameter> normalizedParams = parameters != null ? parameters : Collections.emptyList();
        ReadWriteLock lock = getLockForKey(key, id);

        lock.writeLock().lock();
        try {
            // Check if value exists
            T existingValue = getCurrentValue(key, id, normalizedParams, (Class<T>) value.getClass());
            if (existingValue != null) {
                return existingValue; // Value already exists, don't overwrite
            }

            // No existing value, perform put
            performAtomicPut(key, id, normalizedParams, value, ttlMillis);

            synchronized (statisticsLock) {
                statistics.incrementValues();
            }

            return null;

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Thread-safe replace operation.
     */
    public T replace(String key, List<SearchParameter> parameters, T value) {
        return replace(key, null, parameters, value, config.getRemoteCacheTtlMillis());
    }

    /**
     * Thread-safe replace operation.
     */
    public T replace(String key, Long id, List<SearchParameter> parameters, T value) {
        return replace(key, id, parameters, value, config.getRemoteCacheTtlMillis());
    }

    /**
     * Thread-safe replace operation.
     */
    public T replace(String key, Long id, List<SearchParameter> parameters, T value, long ttlMillis) {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and value cannot be null");
        }

        List<SearchParameter> normalizedParams = parameters != null ? parameters : Collections.emptyList();
        ReadWriteLock lock = getLockForKey(key, id);

        lock.writeLock().lock();
        try {
            // Check if value exists
            T existingValue = getCurrentValue(key, id, normalizedParams, (Class<T>) value.getClass());
            if (existingValue == null) {
                return null; // No existing value, don't replace
            }

            // Replace existing value
            performAtomicPut(key, id, normalizedParams, value, ttlMillis);

            return existingValue;

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Thread-safe conditional replace operation.
     */
    public boolean replace(String key, List<SearchParameter> parameters, T oldValue, T newValue) {
        return replace(key, null, parameters, oldValue, newValue, config.getRemoteCacheTtlMillis());
    }

    /**
     * Thread-safe conditional replace operation.
     */
    public boolean replace(String key, Long id, List<SearchParameter> parameters,
                           T oldValue, T newValue, long ttlMillis) {
        if (key == null || newValue == null) {
            throw new IllegalArgumentException("Key and new value cannot be null");
        }

        List<SearchParameter> normalizedParams = parameters != null ? parameters : Collections.emptyList();
        ReadWriteLock lock = getLockForKey(key, id);

        lock.writeLock().lock();
        try {
            // Check current value
            T currentValue = getCurrentValue(key, id, normalizedParams, (Class<T>) newValue.getClass());

            // Compare with expected old value
            if (!Objects.equals(currentValue, oldValue)) {
                return false; // Values don't match
            }

            // Replace the value
            performAtomicPut(key, id, normalizedParams, newValue, ttlMillis);

            return true;

        } finally {
            lock.writeLock().unlock();
        }
    }

    // ==================== GET OPERATIONS (THREAD-SAFE) ====================

    /**
     * Thread-safe get operation.
     */
    public Optional<T> get(String key, Class<T> valueType) {
        if (key == null) return Optional.empty();

        ReadWriteLock lock = getLockForKey(key, null);

        lock.readLock().lock();
        try {
            return performAtomicGet(key, null, Collections.emptyList(), valueType);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Thread-safe get operation with key and ID.
     */
    public Optional<T> get(String key, Long id, Class<T> valueType) {
        if (key == null || id == null) return Optional.empty();

        ReadWriteLock lock = getLockForKey(key, id);

        lock.readLock().lock();
        try {
            return performAtomicGet(key, id, Collections.emptyList(), valueType);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Thread-safe get operation with ID only.
     */
    public Optional<T> get(Long id, Class<T> valueType) {
        if (id == null) return Optional.empty();

        ReadWriteLock lock = getLockForKey(null, id);

        lock.readLock().lock();
        try {
            return performAtomicGet(null, id, Collections.emptyList(), valueType);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Thread-safe get operation with parameters.
     */
    public List<T> get(List<SearchParameter> parameters, Class<T> valueType) {
        if (parameters == null || parameters.isEmpty()) {
            return Collections.emptyList();
        }

        // For parameter-based searches, we don't have a specific key to lock on
        // Use a hash of the parameters as the lock key
        String paramLockKey = generateParameterLockKey(parameters);
        ReadWriteLock lock = locks.get(paramLockKey);

        lock.readLock().lock();
        try {
            return performParameterSearch(parameters, valueType);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Thread-safe get operation with key and parameters.
     */
    public List<T> get(String key, List<SearchParameter> parameters, Class<T> valueType) {
        if (key == null) {
            return Collections.emptyList();
        }

        ReadWriteLock lock = getLockForKey(key, null);

        lock.readLock().lock();
        try {
            // Combine the key with parameters for a more specific search
            List<SearchParameter> combinedParams = new ArrayList<>();
            if (parameters != null) {
                combinedParams.addAll(parameters);
            }
            combinedParams.add(new SearchParameter("_key", key, 0));

            return performParameterSearch(combinedParams, valueType);
        } finally {
            lock.readLock().unlock();
        }
    }

    // ==================== REMOVE OPERATIONS (THREAD-SAFE) ====================

    /**
     * Thread-safe remove operation.
     */
    public T remove(String key, List<SearchParameter> parameters) {
        return remove(key, null, parameters);
    }

    /**
     * Thread-safe remove operation.
     */
    public T remove(String key, Long id, List<SearchParameter> parameters) {
        if (key == null && id == null) return null;

        ReadWriteLock lock = getLockForKey(key, id);

        lock.writeLock().lock();
        try {
            // Get current value before removing
            List<SearchParameter> normalizedParams = parameters != null ? parameters : Collections.emptyList();
            T previousValue = getCurrentValue(key, id, normalizedParams, valueType);

            // Perform the removal
            performAtomicRemove(key, id);

            // Update statistics
            if (previousValue != null) {
                synchronized (statisticsLock) {
                    statistics.decrementValues();
                }
            }

            return (T) previousValue;

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Thread-safe conditional remove operation.
     */
    public boolean remove(String key, List<SearchParameter> parameters, T value) {
        return remove(key, null, parameters, value);
    }

    /**
     * Thread-safe conditional remove operation.
     */
    public boolean remove(String key, Long id, List<SearchParameter> parameters, T value) {
        if (key == null && id == null) return false;

        ReadWriteLock lock = getLockForKey(key, id);

        lock.writeLock().lock();
        try {
            // Check current value
            List<SearchParameter> normalizedParams = parameters != null ? parameters : Collections.emptyList();
            T currentValue = getCurrentValue(key, id, normalizedParams, (Class<T>) value.getClass());

            // Compare with expected value
            if (!Objects.equals(currentValue, value)) {
                return false; // Values don't match
            }

            // Remove the value
            performAtomicRemove(key, id);

            synchronized (statisticsLock) {
                statistics.decrementValues();
            }

            return true;

        } finally {
            lock.writeLock().unlock();
        }
    }

    // ==================== GET OR COMPUTE OPERATIONS (THREAD-SAFE) ====================

    /**
     * Thread-safe getOrCompute operation.
     */
    public T getOrCompute(String key, Class<T> valueType, Supplier<T> supplier) {
        if (key == null) return null;

        ReadWriteLock lock = getLockForKey(key, null);

        // First try with read lock
        lock.readLock().lock();
        try {
            Optional<T> cached = performAtomicGet(key, null, Collections.emptyList(), valueType);
            if (cached.isPresent()) {
                return cached.get();
            }
        } finally {
            lock.readLock().unlock();
        }

        // Upgrade to write lock for computation
        lock.writeLock().lock();
        try {
            // Double-check after acquiring write lock
            Optional<T> cached = performAtomicGet(key, null, Collections.emptyList(), valueType);
            if (cached.isPresent()) {
                return cached.get();
            }

            // Compute and cache the value
            T computed = supplier.get();
            if (computed != null) {
                performAtomicPut(key, null, Collections.emptyList(), computed, config.getRemoteCacheTtlMillis());
                synchronized (statisticsLock) {
                    statistics.incrementValues();
                }
            }
            return computed;

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Thread-safe getOrCompute operation with key and ID.
     */
    public T getOrCompute(String key, Long id, Class<T> valueType, Supplier<T> supplier) {
        if (key == null || id == null) return null;

        ReadWriteLock lock = getLockForKey(key, id);

        // First try with read lock
        lock.readLock().lock();
        try {
            Optional<T> cached = performAtomicGet(key, id, Collections.emptyList(), valueType);
            if (cached.isPresent()) {
                return cached.get();
            }
        } finally {
            lock.readLock().unlock();
        }

        // Upgrade to write lock for computation
        lock.writeLock().lock();
        try {
            // Double-check after acquiring write lock
            Optional<T> cached = performAtomicGet(key, id, Collections.emptyList(), valueType);
            if (cached.isPresent()) {
                return cached.get();
            }

            // Compute and cache the value
            T computed = supplier.get();
            if (computed != null) {
                performAtomicPut(key, id, Collections.emptyList(), computed, config.getRemoteCacheTtlMillis());
                synchronized (statisticsLock) {
                    statistics.incrementValues();
                }
            }
            return computed;

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Thread-safe getOrCompute operation with ID only.
     */
    public T getOrCompute(Long id, Class<T> valueType, Supplier<T> supplier) {
        if (id == null) return null;

        ReadWriteLock lock = getLockForKey(null, id);

        // First try with read lock
        lock.readLock().lock();
        try {
            Optional<T> cached = performAtomicGet(null, id, Collections.emptyList(), valueType);
            if (cached.isPresent()) {
                return cached.get();
            }
        } finally {
            lock.readLock().unlock();
        }

        // For ID-only computation, we can't cache without a string key
        T computed = supplier.get();
        if (computed != null) {
            // We'd need a way to determine the key from the computed value
            // For now, we'll store it with a generated key based on the ID
            String generatedKey = "id:" + id;

            lock.writeLock().lock();
            try {
                performAtomicPut(generatedKey, id, Collections.emptyList(), computed, config.getRemoteCacheTtlMillis());
                synchronized (statisticsLock) {
                    statistics.incrementValues();
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
        return computed;
    }

    /**
     * Thread-safe getOrCompute operation with parameters.
     */
    public List<T> getOrCompute(List<SearchParameter> parameters, Class<T> valueType, Supplier<List<T>> supplier) {
        if (parameters == null || parameters.isEmpty()) {
            return Collections.emptyList();
        }

        String paramLockKey = generateParameterLockKey(parameters);
        ReadWriteLock lock = locks.get(paramLockKey);

        // First try with read lock
        lock.readLock().lock();
        try {
            List<T> cached = performParameterSearch(parameters, valueType);
            if (!cached.isEmpty()) {
                return cached;
            }
        } finally {
            lock.readLock().unlock();
        }

        // Compute the values (can't easily cache these results since we don't have keys)
        List<T> computed = supplier.get();
        return computed != null ? computed : Collections.emptyList();
    }

    // ==================== LINK OPERATIONS (THREAD-SAFE) ====================

    /**
     * Thread-safe link operation.
     */
    public void link(String key, Long id) {
        if (key == null || id == null) {
            throw new IllegalArgumentException("Key and ID cannot be null");
        }

        // Need to lock both the key and the ID to prevent races
        ReadWriteLock keyLock = getLockForKey(key, null);
        ReadWriteLock idLock = getLockForKey(null, id);

        // Acquire locks in consistent order to prevent deadlock
        ReadWriteLock firstLock = key.compareTo(id.toString()) < 0 ? keyLock : idLock;
        ReadWriteLock secondLock = key.compareTo(id.toString()) < 0 ? idLock : keyLock;

        firstLock.writeLock().lock();
        try {
            secondLock.writeLock().lock();
            try {
                // Perform linking in remote caches
                if (config.isRemoteCacheEnabled()) {
                    redisCache.link(key, id);
                }

                if (config.isDatabaseCacheEnabled()) {
                    databaseCache.link(key, id);
                }

                // Update local cache mapping
                synchronized (localCacheLock) {
                    updateLocalLinkMapping(key, id);
                }

            } finally {
                secondLock.writeLock().unlock();
            }
        } finally {
            firstLock.writeLock().unlock();
        }
    }

    /**
     * Thread-safe link operation with parameters.
     */
    public void link(String key, List<SearchParameter> parameters) {
        if (key == null || parameters == null || parameters.isEmpty()) {
            throw new IllegalArgumentException("Key and parameters cannot be null or empty");
        }

        ReadWriteLock lock = getLockForKey(key, null);

        lock.writeLock().lock();
        try {
            // Link in remote caches
            if (config.isRemoteCacheEnabled()) {
                redisCache.link(key, parameters);
            }

            if (config.isDatabaseCacheEnabled()) {
                databaseCache.link(key, parameters);
            }

            // Update local cache parameter mapping
            synchronized (localCacheLock) {
                updateLocalParameterLinking(key, null, parameters);
            }

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Thread-safe link operation with ID and parameters.
     */
    public void link(Long id, List<SearchParameter> parameters) {
        if (id == null || parameters == null || parameters.isEmpty()) {
            throw new IllegalArgumentException("ID and parameters cannot be null or empty");
        }

        ReadWriteLock lock = getLockForKey(null, id);

        lock.writeLock().lock();
        try {
            // Link in remote caches
            if (config.isRemoteCacheEnabled()) {
                redisCache.link(id, parameters);
            }

            if (config.isDatabaseCacheEnabled()) {
                databaseCache.link(id, parameters);
            }

            // Update local cache parameter mapping
            synchronized (localCacheLock) {
                updateLocalParameterLinking(null, id, parameters);
            }

        } finally {
            lock.writeLock().unlock();
        }
    }

    // ==================== INVALIDATION OPERATIONS (THREAD-SAFE) ====================

    /**
     * Thread-safe invalidate operation.
     */
    public void invalidate(String key, Long id) {
        if (key == null && id == null) return;

        ReadWriteLock lock = getLockForKey(key, id);

        lock.writeLock().lock();
        try {
            performAtomicRemove(key, id);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Thread-safe invalidate operation by key.
     */
    public void invalidate(String key) {
        if (key == null) return;

        ReadWriteLock lock = getLockForKey(key, null);

        lock.writeLock().lock();
        try {
            performAtomicRemove(key, null);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Thread-safe invalidate operation by ID.
     */
    public void invalidate(Long id) {
        if (id == null) return;

        ReadWriteLock lock = getLockForKey(null, id);

        lock.writeLock().lock();
        try {
            performAtomicRemove(null, id);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Thread-safe invalidate all operation.
     */
    public void invalidateAll() {
        // This operation needs to be globally exclusive
        // We'll use a special global lock for this
        ReadWriteLock globalLock = locks.get("__GLOBAL__");

        globalLock.writeLock().lock();
        try {
            // Clear local caches
            synchronized (localCacheLock) {
                if (config.isLocalCacheEnabled()) {
                    if (localPrimaryCache != null) localPrimaryCache.invalidateAll();
                    if (localLongKeyCache != null) localLongKeyCache.invalidateAll();
                    if (localParamCache != null) localParamCache.invalidateAll();
                }
            }

            // Clear remote caches
            if (config.isRemoteCacheEnabled()) {
                redisCache.invalidateAll();
            }

            if (config.isDatabaseCacheEnabled()) {
                databaseCache.invalidateAll();
            }

            // Reset statistics
            synchronized (statisticsLock) {
                statistics.reset();
            }

        } finally {
            globalLock.writeLock().unlock();
        }
    }

    // ==================== UTILITY METHODS ====================

    /**
     * Returns true if this cache contains a mapping for the specified key.
     */
    public boolean containsKey(String key) {
        return get(key, valueType).isPresent();
    }

    /**
     * Returns true if this cache contains a mapping for the specified key and ID.
     */
    public boolean containsKey(String key, Long id) {
        return get(key, id, valueType).isPresent();
    }

    /**
     * Returns the number of cached items (approximate).
     */
    public long size() {
        synchronized (statisticsLock) {
            return statistics.getTotalValues();
        }
    }

    /**
     * Returns true if this cache contains no cached items.
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    // ==================== STATISTICS AND MANAGEMENT ====================

    public CacheStatistics getStatistics() {
        synchronized (statisticsLock) {
            return statistics;
        }
    }

    public void shutdown() {
        ReadWriteLock globalLock = locks.get("__GLOBAL__");

        globalLock.writeLock().lock();
        try {
            invalidateAll();

            if (config.isRemoteCacheEnabled() && redisCache != null) {
                redisCache.shutdown();
            }

            if (config.isDatabaseCacheEnabled() && databaseCache != null) {
                databaseCache.shutdown();
            }
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    // ==================== PRIVATE HELPER METHODS ====================

    /**
     * Thread-safe get operation implementation (assumes appropriate lock is held).
     */
    private Optional<T> performAtomicGet(String key, Long id, List<SearchParameter> parameters, Class<T> valueType) {
        CacheContext context = CacheContext.get();
        boolean skipLocalCache = context != null && context.isSkipLocalCache();
        boolean forceRefresh = context != null && context.isForceRefresh();

        // Check local cache first (if not skipped)
        if (!skipLocalCache && !forceRefresh && config.isLocalCacheEnabled()) {
            CachedItem<T> cachedItem = findInLocalCache(key, id);
            if (cachedItem != null && !cachedItem.isExpired()) {
                synchronized (statisticsLock) {
                    statistics.incrementL1Hits();
                }
                return Optional.of(cachedItem.getValue());
            }
            synchronized (statisticsLock) {
                statistics.incrementL1Misses();
            }
        }

        // Check remote caches
        Optional<T> remoteResult = getFromRemote(key, id, parameters, valueType);

        // Update local cache if found remotely
        if (remoteResult.isPresent() && config.isLocalCacheEnabled() && !skipLocalCache) {
            synchronized (localCacheLock) {
                cacheLocallyIfEnabled(key, id, parameters, remoteResult.get());
            }
        }

        return remoteResult;
    }

    /**
     * Perform parameter-based search (assumes appropriate lock is held).
     */
    private List<T> performParameterSearch(List<SearchParameter> parameters, Class<T> valueType) {
        CacheContext context = CacheContext.get();
        boolean skipLocalCache = context != null && context.isSkipLocalCache();
        boolean forceRefresh = context != null && context.isForceRefresh();

        // Check local cache first unless skipped
        List<T> localResults = Collections.emptyList();
        if (!skipLocalCache && !forceRefresh && config.isLocalCacheEnabled()) {
            localResults = searchInLocalCache(parameters);
            if (!localResults.isEmpty()) {
                synchronized (statisticsLock) {
                    statistics.incrementL1Hits();
                }
                return localResults;
            } else {
                synchronized (statisticsLock) {
                    statistics.incrementL1Misses();
                }
            }
        }

        // If not in local cache or skipping it, check remote
        List<T> remoteResults = searchInRemote(parameters, valueType);

        // If found in remote, update local cache
        if (!remoteResults.isEmpty() && config.isLocalCacheEnabled() && !skipLocalCache) {
            synchronized (localCacheLock) {
                cacheSearchResultsLocally(parameters, remoteResults);
            }
        }

        return remoteResults;
    }

    /**
     * Atomic remove operation (assumes appropriate lock is held).
     */
    private void performAtomicRemove(String key, Long id) {
        // Find cached item to get complete information
        CachedItem<T> cachedItem = null;
        String uniqueId = null;
        List<SearchParameter> parameters = null;

        if (config.isLocalCacheEnabled()) {
            cachedItem = findInLocalCache(key, id);
            if (cachedItem != null) {
                uniqueId = cachedItem.generateUniqueId();
                parameters = cachedItem.getParameters();
            }
        }

        // Remove from all local caches
        synchronized (localCacheLock) {
            invalidateLocalCaches(uniqueId, key, id, parameters);
        }

        // Invalidate remote caches
        invalidateRemoteCaches(key, id);

        synchronized (statisticsLock) {
            statistics.decrementValues();
        }
    }

    /**
     * Update local link mapping (assumes localCacheLock is held).
     */
    private void updateLocalLinkMapping(String key, Long id) {
        if (!config.isLocalCacheEnabled()) return;

        // Find the cached item by key
        CachedItem<T> cachedItem = findInLocalCache(key, null);
        if (cachedItem != null) {
            // Create a new cached item with the ID
            CachedItem<T> updatedItem = new CachedItem<>(
                    key, id, cachedItem.getValue(),
                    cachedItem.getParameters(), cachedItem.getTtl());

            // Update the primary cache
            String uniqueId = updatedItem.generateUniqueId();
            if (localPrimaryCache != null) {
                localPrimaryCache.put(uniqueId, updatedItem);
            }

            // Update the ID mapping
            if (localLongKeyCache != null) {
                localLongKeyCache.put(id, uniqueId);
            }
        }
    }

    /**
     * Update local parameter linking (assumes localCacheLock is held).
     */
    private void updateLocalParameterLinking(String key, Long id, List<SearchParameter> parameters) {
        if (!config.isLocalCacheEnabled()) return;

        // Find the cached item
        CachedItem<T> cachedItem = findInLocalCache(key, id);
        if (cachedItem != null) {
            // Update parameter indexes
            updateLocalParameterIndexes(parameters, cachedItem.generateUniqueId());
        }
    }

    /**
     * Generate lock key for parameter-based operations.
     */
    private String generateParameterLockKey(List<SearchParameter> parameters) {
        return "params:" + parameters.stream()
                .sorted(Comparator.comparingInt(SearchParameter::getLevel))
                .map(SearchParameter::toKey)
                .collect(Collectors.joining(">"));
    }

    // ==================== EXISTING HELPER METHODS (UNCHANGED) ====================

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

            synchronized (statisticsLock) {
                if (result.isPresent()) {
                    statistics.incrementL2Hits();
                } else {
                    statistics.incrementL2Misses();
                }
            }

            return result;
        } catch (Exception e) {
            logger.warn("Error accessing Redis cache: {}", e.getMessage());
            synchronized (statisticsLock) {
                statistics.incrementL2Errors();
            }
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

            synchronized (statisticsLock) {
                if (result.isPresent()) {
                    statistics.incrementL3Hits();

                    // Write back to Redis for future cache hits if enabled
                    if (config.isRemoteCacheEnabled() && result.isPresent()) {
                        try {
                            if (key != null && id != null) {
                                redisCache.put(key, id, parameters, result.get());
                            } else if (key != null) {
                                redisCache.put(key, parameters, result.get());
                            }
                            // Note: can't write back with just an ID
                        } catch (Exception e) {
                            logger.warn("Error writing back to Redis: {}", e.getMessage());
                        }
                    }
                } else {
                    statistics.incrementL3Misses();
                }
            }

            return result;
        } catch (Exception e) {
            logger.warn("Error accessing database cache: {}", e.getMessage());
            synchronized (statisticsLock) {
                statistics.incrementL3Errors();
            }
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

            synchronized (statisticsLock) {
                if (!results.isEmpty()) {
                    statistics.incrementL2Hits();
                } else {
                    statistics.incrementL2Misses();
                }
            }

            return results;
        } catch (Exception e) {
            logger.warn("Error searching Redis cache: {}", e.getMessage());
            synchronized (statisticsLock) {
                statistics.incrementL2Errors();
            }
            return Collections.emptyList();
        }
    }

    private List<T> searchInDatabase(List<SearchParameter> parameters, Class<T> valueType) {
        try {
            List<T> results = databaseCache.get(parameters, valueType);

            synchronized (statisticsLock) {
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
            }

            return results;
        } catch (Exception e) {
            logger.warn("Error searching database cache: {}", e.getMessage());
            synchronized (statisticsLock) {
                statistics.incrementL3Errors();
            }
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
                synchronized (statisticsLock) {
                    statistics.incrementL2Puts();
                }
            } catch (Exception e) {
                logger.warn("Error writing to Redis cache: {}", e.getMessage());
                synchronized (statisticsLock) {
                    statistics.incrementL2Errors();
                }
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
                synchronized (statisticsLock) {
                    statistics.incrementL3Puts();
                }
            } catch (Exception e) {
                logger.warn("Error writing to database cache: {}", e.getMessage());
                synchronized (statisticsLock) {
                    statistics.incrementL3Errors();
                }
            }
        }
    }

    private void cacheLocallyIfEnabled(String key, Long id, List<SearchParameter> parameters, T value) {
        if (!config.isLocalCacheEnabled() || localPrimaryCache == null) {
            return;
        }

        // This method assumes localCacheLock is already held by caller

        // Create cached item
        CachedItem<T> cachedItem = new CachedItem<>(key, id, value, parameters, config.getLocalCacheTtlMillis());
        String uniqueId = cachedItem.generateUniqueId();

        // Store in primary cache
        localPrimaryCache.put(uniqueId, cachedItem);
        synchronized (statisticsLock) {
            statistics.incrementL1Puts();
        }

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

    private void invalidateLocalCaches(String uniqueId, String key, Long id, List<SearchParameter> parameters) {
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
}