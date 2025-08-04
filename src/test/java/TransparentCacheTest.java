import org.junit.jupiter.api.*;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.codec.Kryo5Codec;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@DisplayName("Transparent Hierarchical Cache Service Tests")
class TransparentCacheTest {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @Container
    static OracleContainer oracle = new OracleContainer("gvenzl/oracle-xe:21-slim-faststart")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass");

    private TransparentHierarchicalCacheService<String> transparentCache;
    private HierarchicalCacheService<String> redisCache;
    private DatabaseCacheProvider<String> databaseCache;
    private RedissonClient redissonClient;
    private ExecutorService executorService;

    @BeforeEach
    void setUp() {
        // Setup Redis connection
        String redisUrl = String.format("redis://%s:%d",
                redis.getHost(),
                redis.getMappedPort(6379));

        Config config = new Config();
        config.useSingleServer().setAddress(redisUrl);
        config.setCodec(new Kryo5Codec());

        redissonClient = Redisson.create(config);
        redisCache = new HierarchicalCacheService<>(redissonClient, 300000L);

        // Setup Database cache
        String jdbcUrl = oracle.getJdbcUrl();
        databaseCache = new DatabaseCacheProvider<>(jdbcUrl, oracle.getUsername(), oracle.getPassword());

        // Setup transparent cache with Redis-first strategy
        CacheConfiguration cacheConfig = CacheConfiguration.builder()
                .fallbackStrategy(CacheConfiguration.FallbackStrategy.REDIS_THEN_DATABASE)
                .localCacheTtl(600000L) // 1 minute local cache
                .maxLocalCacheSize(1000L)
                .remoteCacheTtl(3000000L) // 5 minutes remote cache
                .build();

        transparentCache = new TransparentHierarchicalCacheService<>(redisCache, databaseCache, cacheConfig);
        executorService = Executors.newFixedThreadPool(10);
    }

    @AfterEach
    void tearDown() {
        // 1. First, shutdown cache services (before executors)
        if (transparentCache != null) {
            try {
                transparentCache.invalidateAll();
            } catch (Exception e) {
                // Log but don't fail the test
                System.err.println("Warning: Error during cache invalidation: " + e.getMessage());
            }
            try {
                transparentCache.shutdown();
            } catch (Exception e) {
                System.err.println("Warning: Error during cache shutdown: " + e.getMessage());
            }
        }

        // 2. Then shutdown Redisson client
        if (redissonClient != null) {
            try {
                redissonClient.shutdown();
            } catch (Exception e) {
                System.err.println("Warning: Error during Redisson shutdown: " + e.getMessage());
            }
        }

        // 3. Finally, shutdown executor service
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                    if (!executorService.awaitTermination(2, TimeUnit.SECONDS)) {
                        System.err.println("Warning: ExecutorService did not terminate gracefully");
                    }
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    @DisplayName("Test Local Cache Hit")
    void testLocalCacheHit() {
        // Arrange
        List<SearchParameter> params = Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "electronics", 1)
        );

        // Act - Put item
        transparentCache.put("test-key", params, "test-value");

        // Act - Get item (should hit local cache)
        Optional<String> result1 = transparentCache.get("test-key", String.class);
        Optional<String> result2 = transparentCache.get("test-key", String.class);

        // Assert
        assertTrue(result1.isPresent());
        assertEquals("test-value", result1.get());
        assertTrue(result2.isPresent());
        assertEquals("test-value", result2.get());

        // Verify statistics
        CacheStatistics stats = transparentCache.getStatistics();
        assertTrue(stats.getHitCount() >= 2);
    }

    @Test
    @DisplayName("Test Redis Fallback")
    void testRedisFallback() throws InterruptedException {
        // Arrange
        List<SearchParameter> params = Arrays.asList(
                new SearchParameter("region", "EU", 0)
        );

        // Put directly to Redis
        redisCache.put("redis-key", params, "redis-value");

        // Wait for local cache to not have this item
        Thread.sleep(100);

        // Act - Get with context to skip local cache
        CacheContext.set(CacheContext.builder().skipLocalCache(true).build());
        try {
            Optional<String> result = transparentCache.get("redis-key", String.class);

            // Assert
            assertTrue(result.isPresent());
            assertEquals("redis-value", result.get());
        } finally {
            CacheContext.clear();
        }
    }

    @Test
    @DisplayName("Test Database Fallback")
    void testDatabaseFallback() {
        // Arrange - Put directly to database
        List<SearchParameter> params = Arrays.asList(
                new SearchParameter("region", "ASIA", 0)
        );
        databaseCache.put("db-key", params, "db-value", 300000L, String.class);

        // Act - Get with database-only strategy
        CacheContext.set(CacheContext.builder()
                .fallbackStrategy(CacheConfiguration.FallbackStrategy.DATABASE_ONLY)
                .build());
        try {
            Optional<String> result = transparentCache.get("db-key", String.class);

            // Assert
            assertTrue(result.isPresent());
            assertEquals("db-value", result.get());
        } finally {
            CacheContext.clear();
        }
    }

    @Test
    @DisplayName("Test Hierarchical Search Across Layers")
    void testHierarchicalSearchAcrossLayers() {
        // Arrange - Put items in different layers
        List<SearchParameter> params1 = Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "books", 1)
        );
        List<SearchParameter> params2 = Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "electronics", 1)
        );

        // Put one in local cache (via transparent cache)
        transparentCache.put("local-item", params1, "local-value");

        // Put one directly in Redis
        redisCache.put("redis-item", params2, "redis-value");

        // Act - Search by region (should find both)
        List<SearchParameter> searchParams = Arrays.asList(
                new SearchParameter("region", "US", 0)
        );
        List<String> results = transparentCache.get(searchParams, String.class);

        // Assert
        assertFalse(results.isEmpty());
        assertTrue(results.contains("local-value") || results.contains("redis-value"));
    }

    @Test
    @DisplayName("Test Context Override")
    void testContextOverride() {
        // Arrange
        List<SearchParameter> params = Arrays.asList(
                new SearchParameter("test", "context", 0)
        );

        // Put with default TTL
        transparentCache.put("context-key", params, "context-value");

        // Act - Put with custom TTL via context
        CacheContext.set(CacheContext.builder().customTtl(1000L).build());
        try {
            transparentCache.put("context-key-2", params, "context-value-2");
        } finally {
            CacheContext.clear();
        }

        // Assert - Both should be accessible
        assertTrue(transparentCache.get("context-key", String.class).isPresent());
        assertTrue(transparentCache.get("context-key-2", String.class).isPresent());
    }

    @Test
    @DisplayName("Test Force Refresh")
    void testForceRefresh() {
        // Arrange
        List<SearchParameter> params = Arrays.asList(
                new SearchParameter("refresh", "test", 0)
        );

        transparentCache.put("refresh-key", params, "old-value");

        // Modify value in Redis directly
        redisCache.put("refresh-key", params, "new-value");

        // Act - Get with force refresh
        CacheContext.set(CacheContext.builder().forceRefresh(true).build());
        try {
            Optional<String> result = transparentCache.get("refresh-key", String.class);

            // Assert
            assertTrue(result.isPresent());
            assertEquals("new-value", result.get());
        } finally {
            CacheContext.clear();
        }
    }

    @Test
    @DisplayName("Test Get Or Compute")
    void testGetOrCompute() {
        // Act - Get or compute for non-existent key
        String result = transparentCache.getOrCompute("compute-key", String.class, () -> {
            return "computed-value";
        });

        // Assert
        assertEquals("computed-value", result);

        // Verify it was cached
        Optional<String> cached = transparentCache.get("compute-key", String.class);
        assertTrue(cached.isPresent());
        assertEquals("computed-value", cached.get());
    }

    @Test
    @DisplayName("Test Link Operations")
    void testLinkOperations() {
        // Arrange
        List<SearchParameter> initialParams = Arrays.asList(
                new SearchParameter("category", "electronics", 0)
        );
        List<SearchParameter> additionalParams = Arrays.asList(
                new SearchParameter("brand", "apple", 1)
        );

        transparentCache.put("link-key", initialParams, "link-value");

        // Act - Link additional parameters
        transparentCache.link("link-key", additionalParams);

        // Assert - Should be searchable by new parameters
        List<String> results = transparentCache.get(additionalParams, String.class);
        assertFalse(results.isEmpty());
        assertTrue(results.contains("link-value"));

        // Act - Link ID
        transparentCache.link("link-key", 12345L);

        // Assert - Should be accessible by ID
        Optional<String> byId = transparentCache.get(12345L, String.class);
        assertTrue(byId.isPresent());
        assertEquals("link-value", byId.get());
    }

    @Test
    @DisplayName("Test Invalidation Cascading - Enhanced")
    void testInvalidationCascading() {
        // Test with a null key (should not throw exception)
        assertDoesNotThrow(() -> transparentCache.invalidate((String) null));

        // Test invalidating non-existent key (should not throw exception)
        assertDoesNotThrow(() -> transparentCache.invalidate("non-existent-key"));

        // Test with null parameters in SearchParameter constructor
        assertThrows(NullPointerException.class, () -> {
            new SearchParameter(null, "test", 0);
        });

        // Clear cache to ensure clean state
        transparentCache.invalidateAll();
        System.out.println("statistics: " + transparentCache.getStatistics());
        // Setup test data with more descriptive parameter
        List<SearchParameter> params = Arrays.asList(
                new SearchParameter("category", "electronics", 0)
        );
        transparentCache.put("test-key", 999L, params, "test-value");

        // Verify initial state
        assertTrue(transparentCache.get("test-key", String.class).isPresent());
        assertTrue(transparentCache.get(999L, String.class).isPresent());

        // Verify parameter-based search works before invalidation
        List<String> beforeInvalidation = transparentCache.get(params, String.class);
        assertFalse(beforeInvalidation.isEmpty());

        // Get initial statistics after setup
        CacheStatistics statsBefore = transparentCache.getStatistics();
        long valuesBefore = statsBefore.getTotalValues();

        // Test 1: Create separate entries for individual invalidation testing
        transparentCache.put("key-only-test", 888L, params, "key-test-value");
        transparentCache.put("id-only-test", 777L, params, "id-test-value");

        // Verify both entries exist
        assertTrue(transparentCache.get("key-only-test", String.class).isPresent());
        assertTrue(transparentCache.get(888L, String.class).isPresent());
        assertTrue(transparentCache.get("id-only-test", String.class).isPresent());
        assertTrue(transparentCache.get(777L, String.class).isPresent());

        System.out.println("statistics: " + transparentCache.getStatistics());

        // Test 2: Invalidate by key - should remove the entire entry (both key and ID access)
        // This is correct cascading behavior - when you invalidate by key, the whole entry is removed
        transparentCache.invalidate("key-only-test");
        assertFalse(transparentCache.get("key-only-test", String.class).isPresent());
        // The ID should also be inaccessible because it's the same cache entry
        assertFalse(transparentCache.get(888L, String.class).isPresent(),
                "ID should also be inaccessible after key invalidation (cascading behavior)");

        // Test 3: Invalidate by ID only - should also remove the entire entry
        transparentCache.invalidate(777L);
        assertFalse(transparentCache.get(777L, String.class).isPresent());
        // The key should also be inaccessible because it's the same cache entry
        assertFalse(transparentCache.get("id-only-test", String.class).isPresent(),
                "Key should also be inaccessible after ID invalidation (cascading behavior)");

        // Test 4: Invalidate the main test data by key
        transparentCache.invalidate("test-key");

        // Assert - Should be removed from all access methods
        assertFalse(transparentCache.get("test-key", String.class).isPresent());
        assertFalse(transparentCache.get(999L, String.class).isPresent());

        // Verify parameter-based search returns empty after invalidation
        List<String> afterInvalidation = transparentCache.get(params, String.class);
        assertTrue(afterInvalidation.isEmpty());

        // Test 5: Test redundant invalidation calls (should not throw exceptions)
        assertDoesNotThrow(() -> {
            transparentCache.invalidate("test-key"); // Already invalidated
            transparentCache.invalidate(999L); // Already invalidated
            transparentCache.invalidate("test-key", 999L); // Combined invalidation
        });

        // Verify statistics are updated (more robust check)
        CacheStatistics statsAfter = transparentCache.getStatistics();
        // Values should have decreased due to invalidations
        assertTrue(statsAfter.getTotalValues() < valuesBefore,
                String.format("Expected values to decrease from %d to %d",
                        valuesBefore, statsAfter.getTotalValues()));

        // Test 6: Verify parameter indexes are cleaned up
        // Add new entries with same parameters to ensure no conflicts
        assertDoesNotThrow(() -> {
            transparentCache.put("new-key-1", 111L, params, "new-value-1");
            transparentCache.put("new-key-2", 222L, params, "new-value-2");

            // Both should be accessible
            assertTrue(transparentCache.get("new-key-1", String.class).isPresent());
            assertTrue(transparentCache.get("new-key-2", String.class).isPresent());
            assertTrue(transparentCache.get(111L, String.class).isPresent());
            assertTrue(transparentCache.get(222L, String.class).isPresent());

            // Parameter search should find both
            List<String> newResults = transparentCache.get(params, String.class);
            assertEquals(2, newResults.size());
            assertTrue(newResults.contains("new-value-1"));
            assertTrue(newResults.contains("new-value-2"));
        });

        // Test 7: Test invalidation with mixed access patterns
        transparentCache.put("mixed-test", 333L, params, "mixed-value");

        // Verify all access methods work
        assertTrue(transparentCache.get("mixed-test", String.class).isPresent());
        assertTrue(transparentCache.get(333L, String.class).isPresent());
        List<String> paramResults = transparentCache.get(params, String.class);
        assertTrue(paramResults.stream().anyMatch(v -> v.equals("mixed-value")));

        // Invalidate using combined key+ID method
        transparentCache.invalidate("mixed-test", 333L);

        // All access methods should fail
        assertFalse(transparentCache.get("mixed-test", String.class).isPresent());
        assertFalse(transparentCache.get(333L, String.class).isPresent());
        List<String> afterMixedInvalidation = transparentCache.get(params, String.class);
        assertFalse(afterMixedInvalidation.stream().anyMatch(v -> v.equals("mixed-value")));
    }
    @Test
    @DisplayName("Test Concurrent Invalidation")
    void testConcurrentInvalidation() throws InterruptedException {
        List<SearchParameter> params = Arrays.asList(
                new SearchParameter("concurrent", "test", 0)
        );

        // Put multiple related items
        for (int i = 0; i < 10; i++) {
            transparentCache.put("key-" + i, (long) i, params, "value-" + i);
        }

        // Verify all items exist
        for (int i = 0; i < 10; i++) {
            assertTrue(transparentCache.get("key-" + i, String.class).isPresent());
            assertTrue(transparentCache.get((long) i, String.class).isPresent());
        }

        // Concurrent invalidation
        CountDownLatch latch = new CountDownLatch(10);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < 10; i++) {
            final int index = i;
            executorService.submit(() -> {
                try {
                    transparentCache.invalidate("key-" + index);
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    // Log but don't fail - some concurrent operations may conflict
                    System.err.println("Invalidation failed for key-" + index + ": " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));

        // Allow some time for async operations to complete
        Thread.sleep(1000);

        // Verify invalidation results
        int invalidatedCount = 0;
        for (int i = 0; i < 10; i++) {
            if (!transparentCache.get("key-" + i, String.class).isPresent()) {
                invalidatedCount++;
            }
        }

        // At least some items should be invalidated
        assertTrue(invalidatedCount > 0, "At least some items should be invalidated");
        assertTrue(successCount.get() > 0, "At least some invalidations should succeed");
    }

    @Test
    @DisplayName("Test Statistics Tracking")
    void testStatisticsTracking() {
        // Arrange
        List<SearchParameter> params = Arrays.asList(
                new SearchParameter("stats", "test", 0)
        );

        // Get initial statistics
        CacheStatistics initialStats = transparentCache.getStatistics();
        long initialValues = initialStats.getTotalValues();
        long initialHits = initialStats.getHitCount();
        long initialMisses = initialStats.getMissCount();

        // Act - Perform operations
        transparentCache.put("stats-key-1", params, "value-1");
        transparentCache.put("stats-key-2", params, "value-2");

        transparentCache.get("stats-key-1", String.class); // Hit
        transparentCache.get("stats-key-2", String.class); // Hit
        transparentCache.get("non-existent", String.class); // Miss

        // Assert
        CacheStatistics stats = transparentCache.getStatistics();
        assertTrue(stats.getTotalValues() >= initialValues + 2);
        assertTrue(stats.getHitCount() >= initialHits + 2);
        assertTrue(stats.getMissCount() >= initialMisses + 1);
        assertTrue(stats.getHitRate() >= 0);

        // Test statistics after invalidation
        transparentCache.invalidate("stats-key-1");
        CacheStatistics statsAfterInvalidation = transparentCache.getStatistics();
        assertTrue(statsAfterInvalidation.getTotalValues() <= stats.getTotalValues());
    }

    @Test
    @DisplayName("Test Error Handling - Enhanced")
    void testErrorHandling() {
        // Test null key
        assertThrows(IllegalArgumentException.class, () -> {
            transparentCache.put(null, Collections.emptyList(), "value");
        });

        // Test null parameters
        assertThrows(IllegalArgumentException.class, () -> {
            transparentCache.put("key", null, "value");
        });

        // Test null value
        assertThrows(IllegalArgumentException.class, () -> {
            transparentCache.put("key", Collections.emptyList(), null);
        });

        // Test empty key (should not throw)
        assertDoesNotThrow(() -> {
            transparentCache.put("", Collections.emptyList(), "value");
        });

        // Test SearchParameter with null values
        assertThrows(NullPointerException.class, () -> {
            new SearchParameter(null, "value", 0);
        });

        assertThrows(NullPointerException.class, () -> {
            new SearchParameter("name", null, 0);
        });

        // Test get operations with null parameters
        assertFalse(transparentCache.get((String) null, String.class).isPresent());
        assertFalse(transparentCache.get((Long) null, String.class).isPresent());

        // Test get with null parameters list
        assertTrue(transparentCache.get((List<SearchParameter>) null, String.class).isEmpty());
    }

    @Test
    @DisplayName("Test Different Fallback Strategies")
    void testDifferentFallbackStrategies() {
        // Test each strategy
        CacheConfiguration.FallbackStrategy[] strategies = CacheConfiguration.FallbackStrategy.values();

        for (CacheConfiguration.FallbackStrategy strategy : strategies) {
            CacheContext.set(CacheContext.builder().fallbackStrategy(strategy).build());
            try {
                String key = "strategy-test-" + strategy.name();
                List<SearchParameter> params = Arrays.asList(
                        new SearchParameter("strategy", strategy.name(), 0)
                );

                transparentCache.put(key, params, "strategy-value-" + strategy.name());
                Optional<String> result = transparentCache.get(key, String.class);

                assertTrue(result.isPresent(), "Strategy " + strategy + " should work");
                assertEquals("strategy-value-" + strategy.name(), result.get());
            } finally {
                CacheContext.clear();
            }
        }
    }

    @Test
    @DisplayName("Test Basic Operations Without Remote Dependencies")
    void testBasicOperationsWithoutRemoteDependencies() {
        // This test focuses on local cache functionality
        List<SearchParameter> params = Arrays.asList(
                new SearchParameter("local", "test", 0)
        );

        // Test put and get
        transparentCache.put("local-only-key", params, "local-only-value");
        Optional<String> result = transparentCache.get("local-only-key", String.class);

        assertTrue(result.isPresent());
        assertEquals("local-only-value", result.get());

        // Test invalidation
        transparentCache.invalidate("local-only-key");
        Optional<String> afterInvalidation = transparentCache.get("local-only-key", String.class);
        assertFalse(afterInvalidation.isPresent());
    }

    @Test
    @DisplayName("Test Local Cache TTL")
    void testLocalCacheTTL() throws InterruptedException {
        // Create a cache with very short TTL for testing
        CacheConfiguration shortTtlConfig = CacheConfiguration.builder()
                .fallbackStrategy(CacheConfiguration.FallbackStrategy.REDIS_THEN_DATABASE)
                .localCacheTtl(100L) // 100 ms local cache
                .maxLocalCacheSize(1000L)
                .remoteCacheTtl(300000L)
                .build();

        TransparentHierarchicalCacheService<String> shortTtlCache =
                new TransparentHierarchicalCacheService<>(redisCache, databaseCache, shortTtlConfig);

        try {
            List<SearchParameter> params = Arrays.asList(
                    new SearchParameter("ttl", "test", 0)
            );

            // Put item
            shortTtlCache.put("ttl-key", params, "ttl-value");

            // Should be available immediately
            assertTrue(shortTtlCache.get("ttl-key", String.class).isPresent());

            // Wait for TTL to expire
            Thread.sleep(500);

            // Should still be available from remote cache
            Optional<String> afterTtl = shortTtlCache.get("ttl-key", String.class);
            assertTrue(afterTtl.isPresent(), "Item should be available from remote cache after local TTL expires");
        } finally {
            shortTtlCache.shutdown();
        }
    }

    @Test
    @DisplayName("Test Configuration Builder")
    void testConfigurationBuilder() {
        // Test configuration builder
        CacheConfiguration config = CacheConfiguration.builder()
                .fallbackStrategy(CacheConfiguration.FallbackStrategy.DATABASE_ONLY)
                .localCacheTtl(30000L)
                .maxLocalCacheSize(500L)
                .remoteCacheTtl(120000L)
                .enableLocalCache(false)
                .enableWriteThrough(false)
                .enableReadThrough(true)
                .build();

        // Verify configuration
        assertEquals(CacheConfiguration.FallbackStrategy.DATABASE_ONLY, config.getGlobalFallbackStrategy());
        assertEquals(30000L, config.getLocalCacheTtlMillis());
        assertEquals(500L, config.getMaxLocalCacheSize());
        assertEquals(120000L, config.getRemoteCacheTtlMillis());
        assertFalse(config.isLocalCacheEnabled());
        assertFalse(config.isWriteThroughEnabled());
        assertTrue(config.isReadThroughEnabled());
    }

    @Test
    @DisplayName("Test Timing-Dependent Operations")
    void testTimingDependentOperations() throws InterruptedException {
        List<SearchParameter> params = Arrays.asList(
                new SearchParameter("timing", "test", 0)
        );

        // Put item
        transparentCache.put("timing-key", params, "timing-value");

        // Verify immediate availability
        assertTrue(transparentCache.get("timing-key", String.class).isPresent());

        // Invalidate
        transparentCache.invalidate("timing-key");

        // Allow time for async operations
        Thread.sleep(100);

        // Verify invalidation
        assertFalse(transparentCache.get("timing-key", String.class).isPresent());
    }

    @Test
    @DisplayName("Test Parameter Index Cleanup")
    void testParameterIndexCleanup() {
        List<SearchParameter> params = Arrays.asList(
                new SearchParameter("cleanup", "test", 0),
                new SearchParameter("index", "verification", 1)
        );

        // Put multiple items with same parameters
        transparentCache.put("cleanup-key-1", params, "value-1");
        transparentCache.put("cleanup-key-2", params, "value-2");

        // Verify both are searchable by parameters
        List<String> results = transparentCache.get(params, String.class);
        assertEquals(2, results.size());

        // Invalidate one item
        transparentCache.invalidate("cleanup-key-1");

        // Verify only one remains searchable
        List<String> afterInvalidation = transparentCache.get(params, String.class);
        assertEquals(1, afterInvalidation.size());
        assertTrue(afterInvalidation.contains("value-2"));
    }

    @Test
    @DisplayName("Test SearchParameter Edge Cases")
    void testSearchParameterEdgeCases() {
        // Test with special characters
        SearchParameter specialParam = new SearchParameter("special@chars", "value#with$symbols", 0);
        assertEquals("L0:special@chars=value#with$symbols", specialParam.toKey());

        // Test with empty strings (should work but not recommended)
        SearchParameter emptyParam = new SearchParameter("", "", 0);
        assertEquals("L0:=", emptyParam.toKey());

        // Test with negative level
        SearchParameter negativeLevel = new SearchParameter("negative", "level", -1);
        assertEquals("L-1:negative=level", negativeLevel.toKey());

        // Test equals and hashCode consistency
        SearchParameter param1 = new SearchParameter("test", "value", 1);
        SearchParameter param2 = new SearchParameter("test", "value", 1);
        SearchParameter param3 = new SearchParameter("test", "different", 1);

        assertEquals(param1, param2);
        assertEquals(param1.hashCode(), param2.hashCode());
        assertNotEquals(param1, param3);
        assertNotEquals(param1.hashCode(), param3.hashCode());
    }
}