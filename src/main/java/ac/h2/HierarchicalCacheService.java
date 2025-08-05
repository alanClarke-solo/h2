package ac.h2;// HierarchicalCacheService.java
import org.redisson.Redisson;
import org.redisson.api.*;
import org.redisson.config.Config;
import org.redisson.codec.Kryo5Codec;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class HierarchicalCacheService<T> {
    private static final String PRIMARY_KEY_PREFIX = "primary:";
    private static final String LONG_KEY_PREFIX = "longkey:";
    private static final String PARAM_PREFIX = "param:";
    private static final String VALUE_PREFIX = "value:";
    private static final String METADATA_PREFIX = "meta:";
    
    private final RedissonClient redissonClient;
    private final long defaultTtlMillis;
    private final CacheStatistics statistics;

    public HierarchicalCacheService(String redisAddress, long defaultTtlMillis) {
        Config config = new Config();
        config.useSingleServer().setAddress(redisAddress);
        config.setCodec(new Kryo5Codec());
        
        this.redissonClient = Redisson.create(config);
        this.defaultTtlMillis = defaultTtlMillis;
        this.statistics = new CacheStatistics();
    }

    public HierarchicalCacheService(RedissonClient redissonClient, long defaultTtlMillis) {
        this.redissonClient = redissonClient;
        this.defaultTtlMillis = defaultTtlMillis;
        this.statistics = new CacheStatistics();
    }

    // PUT OPERATIONS
    public void put(String key, List<SearchParameter> parameters, T value) {
        put(key, null, parameters, value, defaultTtlMillis);
    }

    public void put(String key, List<SearchParameter> parameters, T value, long ttlMillis) {
        put(key, null, parameters, value, ttlMillis);
    }

    public void put(String key, Long id, List<SearchParameter> parameters, T value) {
        put(key, id, parameters, value, defaultTtlMillis);
    }

    public void put(String key, Long id, List<SearchParameter> parameters, T value, long ttlMillis) {
        if (key == null || parameters == null || value == null) {
            throw new IllegalArgumentException("Key, parameters, and value cannot be null");
        }

        CachedItem<T> cachedItem = new CachedItem<>(key, id, value, parameters, ttlMillis);
        String uniqueId = cachedItem.generateUniqueId();
        
        RBatch batch = redissonClient.createBatch();
        
        // Store the actual value
        RBucketAsync<Object> valueBucket = batch.getBucket(VALUE_PREFIX + uniqueId);
        if (ttlMillis > 0) {
            valueBucket.setAsync(cachedItem, ttlMillis, TimeUnit.MILLISECONDS);
        } else {
            valueBucket.setAsync(cachedItem);
        }

        // Create primary key reference
        RBucketAsync<Object> primaryBucket = batch.getBucket(PRIMARY_KEY_PREFIX + key);
        if (ttlMillis > 0) {
            primaryBucket.setAsync(uniqueId, ttlMillis, TimeUnit.MILLISECONDS);
        } else {
            primaryBucket.setAsync(uniqueId);
        }

        // Create long key reference if provided
        if (id != null) {
            RBucketAsync<Object> longKeyBucket = batch.getBucket(LONG_KEY_PREFIX + id);
            if (ttlMillis > 0) {
                longKeyBucket.setAsync(uniqueId, ttlMillis, TimeUnit.MILLISECONDS);
            } else {
                longKeyBucket.setAsync(uniqueId);
            }
        }

        // Create hierarchical parameter references
        createParameterReferences(batch, parameters, uniqueId, ttlMillis);

        batch.execute();
        
        statistics.incrementValues();
        statistics.incrementKeys();
    }

    private void createParameterReferences(RBatch batch, List<SearchParameter> parameters, String uniqueId, long ttlMillis) {
        // Sort parameters by level for consistent ordering
        List<SearchParameter> sortedParams = parameters.stream()
                .sorted(Comparator.comparingInt(SearchParameter::getLevel))
                .collect(Collectors.toList());

        // Generate all possible combinations for hierarchical search
        Set<String> patterns = generateHierarchicalPatterns(sortedParams);
        
        for (String pattern : patterns) {
            RSetAsync<Object> paramSet = batch.getSet(PARAM_PREFIX + pattern);
            paramSet.addAsync(uniqueId);
            if (ttlMillis > 0) {
                paramSet.expireAsync(ttlMillis, TimeUnit.MILLISECONDS);
            }
            statistics.incrementKeys();
        }
    }

    private Set<String> generateHierarchicalPatterns(List<SearchParameter> sortedParams) {
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

    // LINK OPERATIONS
    public void link(String key, Long id) {
        if (key == null || id == null) {
            throw new IllegalArgumentException("Key and id cannot be null");
        }

        RBucket<String> primaryBucket = redissonClient.getBucket(PRIMARY_KEY_PREFIX + key);
        String existingUniqueId = primaryBucket.get();
        
        if (existingUniqueId == null) {
            throw new IllegalStateException("No cached item found for key: " + key);
        }

        RBucket<String> longKeyBucket = redissonClient.getBucket(LONG_KEY_PREFIX + id);
        if (longKeyBucket.get() != null) {
            throw new IllegalStateException("ID " + id + " is already associated with another key");
        }

        // Check if the cached item already has an ID
        RBucket<CachedItem<T>> valueBucket = redissonClient.getBucket(VALUE_PREFIX + existingUniqueId);
        CachedItem<T> cachedItem = valueBucket.get();
        
        if (cachedItem != null && cachedItem.getLongKey() != null) {
            throw new IllegalStateException("Key " + key + " already has an associated ID");
        }

        // Create new cached item with the ID
        if (cachedItem != null) {
            CachedItem<T> newCachedItem = new CachedItem<>(key, id, cachedItem.getValue(), 
                    cachedItem.getParameters(), cachedItem.getTtl());
            String newUniqueId = newCachedItem.generateUniqueId();
            
            RBatch batch = redissonClient.createBatch();
            
            // Store new cached item
            RBucketAsync<Object> newValueBucket = batch.getBucket(VALUE_PREFIX + newUniqueId);
            if (cachedItem.getTtl() > 0) {
                long remainingTtl = cachedItem.getTtl() - (System.currentTimeMillis() - cachedItem.getCreatedAt());
                if (remainingTtl > 0) {
                    newValueBucket.setAsync(newCachedItem, remainingTtl, TimeUnit.MILLISECONDS);
                }
            } else {
                newValueBucket.setAsync(newCachedItem);
            }
            
            // Update primary key reference
            RBucketAsync<Object> primaryBucketBatch = batch.getBucket(PRIMARY_KEY_PREFIX + key);
            primaryBucketBatch.setAsync(newUniqueId);
            
            // Create long key reference
            RBucketAsync<Object> longKeyBucketBatch = batch.getBucket(LONG_KEY_PREFIX + id);
            longKeyBucketBatch.setAsync(newUniqueId);
            
            // Update parameter references
            updateParameterReferences(batch, cachedItem.getParameters(), existingUniqueId, newUniqueId, cachedItem.getTtl());
            
            // Remove old cached item
            RBucketAsync<Object> oldValueBucket = batch.getBucket(VALUE_PREFIX + existingUniqueId);
            oldValueBucket.deleteAsync();
            
            batch.execute();
        }
    }

    public void link(String key, List<SearchParameter> parameters) {
        if (key == null || parameters == null || parameters.isEmpty()) {
            throw new IllegalArgumentException("Key and parameters cannot be null or empty");
        }

        RBucket<String> primaryBucket = redissonClient.getBucket(PRIMARY_KEY_PREFIX + key);
        String uniqueId = primaryBucket.get();
        
        if (uniqueId == null) {
            throw new IllegalStateException("No cached item found for key: " + key);
        }

        RBucket<CachedItem<T>> valueBucket = redissonClient.getBucket(VALUE_PREFIX + uniqueId);
        CachedItem<T> cachedItem = valueBucket.get();
        
        if (cachedItem == null) {
            throw new IllegalStateException("Cached item not found");
        }

        // Check which parameters are new
        List<SearchParameter> existingParams = cachedItem.getParameters();
        List<SearchParameter> newParams = parameters.stream()
                .filter(param -> !existingParams.contains(param))
                .collect(Collectors.toList());

        if (newParams.isEmpty()) {
            throw new IllegalStateException("All parameters are already associated with this key");
        }

        // Combine existing and new parameters
        List<SearchParameter> allParams = new ArrayList<>(existingParams);
        allParams.addAll(newParams);

        // Create new cached item with updated parameters
        CachedItem<T> newCachedItem = new CachedItem<>(cachedItem.getStringKey(), 
                cachedItem.getLongKey(), cachedItem.getValue(), allParams, cachedItem.getTtl());

        RBatch batch = redissonClient.createBatch();
        
        // Update cached item
        RBucketAsync<Object> newValueBucket = batch.getBucket(VALUE_PREFIX + uniqueId);
        if (cachedItem.getTtl() > 0) {
            long remainingTtl = cachedItem.getTtl() - (System.currentTimeMillis() - cachedItem.getCreatedAt());
            if (remainingTtl > 0) {
                newValueBucket.setAsync(newCachedItem, remainingTtl, TimeUnit.MILLISECONDS);
            }
        } else {
            newValueBucket.setAsync(newCachedItem);
        }

        // Add new parameter references
        createParameterReferences(batch, newParams, uniqueId, cachedItem.getTtl());

        batch.execute();
    }

    public void link(Long id, List<SearchParameter> parameters) {
        if (id == null || parameters == null || parameters.isEmpty()) {
            throw new IllegalArgumentException("ID and parameters cannot be null or empty");
        }

        RBucket<String> longKeyBucket = redissonClient.getBucket(LONG_KEY_PREFIX + id);
        String uniqueId = longKeyBucket.get();
        
        if (uniqueId == null) {
            throw new IllegalStateException("No cached item found for ID: " + id);
        }

        RBucket<CachedItem<T>> valueBucket = redissonClient.getBucket(VALUE_PREFIX + uniqueId);
        CachedItem<T> cachedItem = valueBucket.get();
        
        if (cachedItem == null) {
            throw new IllegalStateException("Cached item not found");
        }

        // Check which parameters are new
        List<SearchParameter> existingParams = cachedItem.getParameters();
        List<SearchParameter> newParams = parameters.stream()
                .filter(param -> !existingParams.contains(param))
                .collect(Collectors.toList());

        if (newParams.isEmpty()) {
            throw new IllegalStateException("All parameters are already associated with this ID");
        }

        // Combine existing and new parameters
        List<SearchParameter> allParams = new ArrayList<>(existingParams);
        allParams.addAll(newParams);

        // Create new cached item with updated parameters
        CachedItem<T> newCachedItem = new CachedItem<>(cachedItem.getStringKey(), 
                cachedItem.getLongKey(), cachedItem.getValue(), allParams, cachedItem.getTtl());

        RBatch batch = redissonClient.createBatch();
        
        // Update cached item
        RBucketAsync<Object> newValueBucket = batch.getBucket(VALUE_PREFIX + uniqueId);
        if (cachedItem.getTtl() > 0) {
            long remainingTtl = cachedItem.getTtl() - (System.currentTimeMillis() - cachedItem.getCreatedAt());
            if (remainingTtl > 0) {
                newValueBucket.setAsync(newCachedItem, remainingTtl, TimeUnit.MILLISECONDS);
            }
        } else {
            newValueBucket.setAsync(newCachedItem);
        }

        // Add new parameter references
        createParameterReferences(batch, newParams, uniqueId, cachedItem.getTtl());

        batch.execute();
    }

    private void updateParameterReferences(RBatch batch, List<SearchParameter> parameters, 
            String oldUniqueId, String newUniqueId, long ttl) {
        Set<String> patterns = generateHierarchicalPatterns(parameters.stream()
                .sorted(Comparator.comparingInt(SearchParameter::getLevel))
                .collect(Collectors.toList()));
        
        for (String pattern : patterns) {
            RSetAsync<Object> paramSet = batch.getSet(PARAM_PREFIX + pattern);
            paramSet.removeAsync(oldUniqueId);
            paramSet.addAsync(newUniqueId);
            if (ttl > 0) {
                paramSet.expireAsync(ttl, TimeUnit.MILLISECONDS);
            }
        }
    }

    // GET OPERATIONS
    public Optional<T> get(String key, Class<T> valueType) {
        if (key == null) return Optional.empty();
        
        RBucket<String> primaryBucket = redissonClient.getBucket(PRIMARY_KEY_PREFIX + key);
        String uniqueId = primaryBucket.get();
        
        if (uniqueId == null) {
            statistics.incrementMisses();
            return Optional.empty();
        }
        
        return getByUniqueId(uniqueId, valueType);
    }

    public Optional<T> get(String key, Long id, Class<T> valueType) {
        if (key == null || id == null) return Optional.empty();
        
        String expectedUniqueId = key + ":" + id;
        return getByUniqueId(expectedUniqueId, valueType);
    }

    public Optional<T> get(Long id, Class<T> valueType) {
        if (id == null) return Optional.empty();
        
        RBucket<String> longKeyBucket = redissonClient.getBucket(LONG_KEY_PREFIX + id);
        String uniqueId = longKeyBucket.get();
        
        if (uniqueId == null) {
            statistics.incrementMisses();
            return Optional.empty();
        }
        
        return getByUniqueId(uniqueId, valueType);
    }

    public List<T> get(String key, List<SearchParameter> parameters, Class<T> valueType) {
        if (parameters == null || parameters.isEmpty()) {
            Optional<T> single = get(key, valueType);
            return single.map(Collections::singletonList).orElse(Collections.emptyList());
        }
        
        return getByParameters(parameters, valueType);
    }

    public List<T> get(List<SearchParameter> parameters, Class<T> valueType) {
        if (parameters == null || parameters.isEmpty()) {
            statistics.incrementMisses();
            return Collections.emptyList();
        }
        
        return getByParameters(parameters, valueType);
    }

    private Optional<T> getByUniqueId(String uniqueId, Class<T> valueType) {
        RBucket<CachedItem<T>> valueBucket = redissonClient.getBucket(VALUE_PREFIX + uniqueId);
        CachedItem<T> cachedItem = valueBucket.get();
        
        if (cachedItem == null || cachedItem.isExpired()) {
            if (cachedItem != null && cachedItem.isExpired()) {
                invalidateByUniqueId(uniqueId);
            }
            statistics.incrementMisses();
            return Optional.empty();
        }
        
        statistics.incrementHits();
        return Optional.of(cachedItem.getValue());
    }

    private List<T> getByParameters(List<SearchParameter> parameters, Class<T> valueType) {
        // Sort parameters by level
        List<SearchParameter> sortedParams = parameters.stream()
                .sorted(Comparator.comparingInt(SearchParameter::getLevel))
                .collect(Collectors.toList());

        // Try exact match first
        String exactPattern = sortedParams.stream()
                .map(SearchParameter::toKey)
                .collect(Collectors.joining(">"));

        Set<String> uniqueIds = getUniqueIdsByPattern(exactPattern);
        
        if (uniqueIds.isEmpty()) {
            // Try hierarchical degradation - find items with more specific patterns
            uniqueIds = findItemsWithHierarchicalDegradation(sortedParams);
        }

        List<T> results = new ArrayList<>();
        for (String uniqueId : uniqueIds) {
            Optional<T> item = getByUniqueId(uniqueId, valueType);
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
        
        // Get all possible patterns and find matches
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
        
        // Check if all search parameters are contained in the pattern
        for (String part : patternParts) {
            if (searchParamKeys.contains(part)) {
                searchParamKeys.remove(part);
            }
        }
        
        return searchParamKeys.isEmpty();
    }

    // GET OR COMPUTE OPERATIONS
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
            // Cannot put with only ID, need at least a string key
            throw new IllegalStateException("Cannot cache item with only Long ID - string key required");
        }
        return computed;
    }

    public List<T> getOrCompute(List<SearchParameter> parameters, Class<T> valueType, Supplier<List<T>> supplier) {
        List<T> cached = get(parameters, valueType);
        if (!cached.isEmpty()) {
            return cached;
        }
        
        List<T> computed = supplier.get();
        // Note: Cannot automatically cache computed results without explicit keys
        return computed != null ? computed : Collections.emptyList();
    }

    // INVALIDATION OPERATIONS
    public void invalidate(String key) {
        if (key == null) return;

        try {
            RBucket<String> primaryBucket = redissonClient.getBucket(PRIMARY_KEY_PREFIX + key);
            String uniqueId = primaryBucket.get();

            if (uniqueId != null) {
                invalidateByUniqueId(uniqueId);
            }
        } catch (Exception e) {
            System.err.println("Error during invalidation of key '" + key + "': " + e.getMessage());
            // Don't rethrow - invalidation should be best-effort during cleanup
        }
    }

    public void invalidate(Long id) {
        if (id == null) return;

        try {
            RBucket<String> longKeyBucket = redissonClient.getBucket(LONG_KEY_PREFIX + id);
            String uniqueId = longKeyBucket.get();

            if (uniqueId != null) {
                invalidateByUniqueId(uniqueId);
            }
        } catch (Exception e) {
            System.err.println("Error during invalidation of ID '" + id + "': " + e.getMessage());
        }
    }

    public void invalidate(String key, Long id) {
        if (key == null || id == null) return;

        // First try to find by primary key
        RBucket<String> primaryBucket = redissonClient.getBucket(PRIMARY_KEY_PREFIX + key);
        String uniqueId = primaryBucket.get();

        if (uniqueId != null) {
            // Verify this item also has the correct Long ID
            RBucket<CachedItem<T>> valueBucket = redissonClient.getBucket(VALUE_PREFIX + uniqueId);
            CachedItem<T> cachedItem = valueBucket.get();

            if (cachedItem != null && Objects.equals(cachedItem.getLongKey(), id)) {
                invalidateByUniqueId(uniqueId);
            }
        }
    }

    public void invalidateByPattern(List<SearchParameter> parameters) {
        if (parameters == null || parameters.isEmpty()) return;
        
        Set<String> patterns = generateHierarchicalPatterns(parameters.stream()
                .sorted(Comparator.comparingInt(SearchParameter::getLevel))
                .collect(Collectors.toList()));
        
        Set<String> uniqueIds = new HashSet<>();
        for (String pattern : patterns) {
            uniqueIds.addAll(getUniqueIdsByPattern(pattern));
        }
        
        for (String uniqueId : uniqueIds) {
            invalidateByUniqueId(uniqueId);
        }
    }

    private void invalidateByUniqueId(String uniqueId) {
        RBucket<CachedItem<T>> valueBucket = redissonClient.getBucket(VALUE_PREFIX + uniqueId);
        CachedItem<T> cachedItem = valueBucket.get();

        if (cachedItem == null) return;

        RBatch batch = redissonClient.createBatch();

        // Remove value
        RBucketAsync<Object> valueBucketBatch = batch.getBucket(VALUE_PREFIX + uniqueId);
        valueBucketBatch.deleteAsync();

        // Remove primary key reference
        if (cachedItem.getStringKey() != null) {
            RBucketAsync<Object> primaryBucket = batch.getBucket(PRIMARY_KEY_PREFIX + cachedItem.getStringKey());
            primaryBucket.deleteAsync();
        }

        // Remove long key reference if exists
        if (cachedItem.getLongKey() != null) {
            RBucketAsync<Object> longKeyBucket = batch.getBucket(LONG_KEY_PREFIX + cachedItem.getLongKey());
            longKeyBucket.deleteAsync();
        }

        // Remove parameter references
        Set<String> patterns = generateHierarchicalPatterns(cachedItem.getParameters().stream()
                .sorted(Comparator.comparingInt(SearchParameter::getLevel))
                .collect(Collectors.toList()));

        for (String pattern : patterns) {
            RSetAsync<Object> paramSet = batch.getSet(PARAM_PREFIX + pattern);
            paramSet.removeAsync(uniqueId);
        }

        // Execute the batch once
        batch.execute();

        // Clean up empty parameter sets (done separately after the main batch)
        for (String pattern : patterns) {
            try {
                RSet<String> paramSetSync = redissonClient.getSet(PARAM_PREFIX + pattern);
                if (paramSetSync != null && paramSetSync.size() == 0) {
                    paramSetSync.delete();
                }
            } catch (Exception e) {
                // Ignore cleanup errors - this is best effort
            }
        }

        statistics.decrementValues();
        statistics.decrementKeys();
    }

    public void invalidateAll() {
        RKeys keys = redissonClient.getKeys();
        keys.deleteByPattern(PRIMARY_KEY_PREFIX + "*");
        keys.deleteByPattern(LONG_KEY_PREFIX + "*");
        keys.deleteByPattern(PARAM_PREFIX + "*");
        keys.deleteByPattern(VALUE_PREFIX + "*");
        keys.deleteByPattern(METADATA_PREFIX + "*");
        
        // Reset statistics
        statistics.getTotalKeys();
        statistics.getTotalValues();
    }

    // STATISTICS
    public CacheStatistics getStatistics() {
        return statistics;
    }

    // CLEANUP
    public void shutdown() {
        redissonClient.shutdown();
    }
}
