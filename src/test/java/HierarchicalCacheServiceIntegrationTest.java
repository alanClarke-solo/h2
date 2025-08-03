// HierarchicalCacheServiceIntegrationTest.java
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.codec.Kryo5Codec;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class HierarchicalCacheServiceIntegrationTest {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    private RedissonClient redissonClient;
    private HierarchicalCacheService<String> cacheService;
    private List<SearchParameter> iphoneParameters;
    private List<SearchParameter> samsungParameters;

    @BeforeEach
    void setUp() {
        String redisUrl = String.format("redis://%s:%d", 
            redis.getHost(), 
            redis.getMappedPort(6379));
        
        Config config = new Config();
        config.useSingleServer().setAddress(redisUrl);
        config.setCodec(new Kryo5Codec());
        
        redissonClient = Redisson.create(config);
        cacheService = new HierarchicalCacheService<>(redissonClient, 300000L); // 5 minutes TTL
        
        iphoneParameters = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1),
            new SearchParameter("brand", "apple", 2),
            new SearchParameter("product", "cellphone", 3)
        );
        
        samsungParameters = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1),
            new SearchParameter("brand", "samsung", 2),
            new SearchParameter("product", "cellphone", 3)
        );
    }

    @AfterEach
    void tearDown() {
        if (cacheService != null) {
            cacheService.invalidateAll();
            cacheService.shutdown();
        }
    }

    @Test
    void testBasicPutAndGet() {
        // Arrange
        String iphoneSpec = "iPhone 17s - 6.1-inch display, A18 chip";
        
        // Act
        cacheService.put("iphone17s", iphoneParameters, iphoneSpec);
        Optional<String> result = cacheService.get("iphone17s", String.class);
        
        // Assert
        assertTrue(result.isPresent());
        assertEquals(iphoneSpec, result.get());
    }

    @Test
    void testPutWithLongIdAndGet() {
        // Arrange
        String iphoneSpec = "iPhone 17s - 6.1-inch display, A18 chip";
        
        // Act
        cacheService.put("iphone17s", 2371L, iphoneParameters, iphoneSpec);
        
        // Test get by string key
        Optional<String> byKey = cacheService.get("iphone17s", String.class);
        assertTrue(byKey.isPresent());
        assertEquals(iphoneSpec, byKey.get());
        
        // Test get by long ID
        Optional<String> byId = cacheService.get(2371L, String.class);
        assertTrue(byId.isPresent());
        assertEquals(iphoneSpec, byId.get());
        
        // Test get by combination
        Optional<String> byCombination = cacheService.get("iphone17s", 2371L, String.class);
        assertTrue(byCombination.isPresent());
        assertEquals(iphoneSpec, byCombination.get());
    }

    @Test
    void testHierarchicalSearch() {
        // Arrange
        String iphoneSpec = "iPhone 17s Specification";
        String samsungSpec = "Samsung Galaxy S25 Specification";
        
        cacheService.put("iphone17s", 2371L, iphoneParameters, iphoneSpec);
        cacheService.put("galaxy-s25", 2372L, samsungParameters, samsungSpec);
        
        // Test search by region (should return both)
        List<SearchParameter> regionSearch = Arrays.asList(
            new SearchParameter("region", "US", 0)
        );
        List<String> regionResults = cacheService.get(regionSearch, String.class);
        assertEquals(2, regionResults.size());
        assertTrue(regionResults.contains(iphoneSpec));
        assertTrue(regionResults.contains(samsungSpec));
        
        // Test search by brand (should return only iPhone)
        List<SearchParameter> brandSearch = Arrays.asList(
            new SearchParameter("brand", "apple", 2)
        );
        List<String> brandResults = cacheService.get(brandSearch, String.class);
        assertEquals(1, brandResults.size());
        assertEquals(iphoneSpec, brandResults.get(0));
        
        // Test search by region + category
        List<SearchParameter> regionCategorySearch = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1)
        );
        List<String> regionCategoryResults = cacheService.get(regionCategorySearch, String.class);
        assertEquals(2, regionCategoryResults.size());
    }

    @Test
    void testHierarchicalSearchWithGaps() {
        // Arrange
        String iphoneSpec = "iPhone 17s Specification";
        cacheService.put("iphone17s", iphoneParameters, iphoneSpec);
        
        // Test search with gaps (L0 + L3, skipping L1, L2)
        List<SearchParameter> gappedSearch = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("product", "cellphone", 3)
        );
        List<String> results = cacheService.get(gappedSearch, String.class);
        
        // Assert
        assertEquals(1, results.size());
        assertEquals(iphoneSpec, results.get(0));
    }

    @Test
    void testLinkOperations() {
        // Arrange
        String iphoneSpec = "iPhone 17s Specification";
        cacheService.put("iphone17s", iphoneParameters, iphoneSpec);
        
        // Test linking ID to existing key
        cacheService.link("iphone17s", 2371L);
        
        Optional<String> byId = cacheService.get(2371L, String.class);
        assertTrue(byId.isPresent());
        assertEquals(iphoneSpec, byId.get());
        
        // Test linking additional parameters
        List<SearchParameter> additionalParams = Arrays.asList(
            new SearchParameter("color", "black", 4),
            new SearchParameter("storage", "128GB", 5)
        );
        cacheService.link("iphone17s", additionalParams);
        
        // Test search by new parameter
        List<SearchParameter> colorSearch = Arrays.asList(
            new SearchParameter("color", "black", 4)
        );
        List<String> colorResults = cacheService.get(colorSearch, String.class);
        assertEquals(1, colorResults.size());
        assertEquals(iphoneSpec, colorResults.get(0));
    }

    @Test
    void testLinkExceptions() {
        // Test link with non-existent key
        assertThrows(IllegalStateException.class, () -> 
            cacheService.link("nonexistent", 123L));
        
        // Set up existing item
        cacheService.put("iphone17s", 2371L, iphoneParameters, "iPhone Spec");
        
        // Test link with already used ID
        assertThrows(IllegalStateException.class, () -> 
            cacheService.link("another-key", 2371L));
    }

    @Test
    void testGetOrCompute() {
        // Test cache miss - should compute
        String computedValue = "Computed iPhone Specification";
        String result1 = cacheService.getOrCompute("new-iphone", String.class, () -> computedValue);
        assertEquals(computedValue, result1);
        
        // Test cache hit - should not compute again
        String result2 = cacheService.getOrCompute("new-iphone", String.class, () -> "Different Value");
        assertEquals(computedValue, result2); // Should return cached value, not new computed value
    }

    @Test
    void testTTLExpiration() throws InterruptedException {
        // Arrange - use very short TTL
        String iphoneSpec = "iPhone 17s Specification";
        cacheService.put("iphone17s", iphoneParameters, iphoneSpec, 100L); // 100ms TTL
        
        // Verify item is initially cached
        Optional<String> initial = cacheService.get("iphone17s", String.class);
        assertTrue(initial.isPresent());
        
        // Wait for expiration
        TimeUnit.MILLISECONDS.sleep(150);
        
        // Verify item is expired and removed
        Optional<String> expired = cacheService.get("iphone17s", String.class);
        assertFalse(expired.isPresent());
    }

    @Test
    void testInvalidation() {
        // Arrange
        String iphoneSpec = "iPhone 17s Specification";
        String samsungSpec = "Samsung Galaxy S25 Specification";
        
        cacheService.put("iphone17s", 2371L, iphoneParameters, iphoneSpec);
        cacheService.put("galaxy-s25", 2372L, samsungParameters, samsungSpec);
        
        // Verify both items are cached
        assertTrue(cacheService.get("iphone17s", String.class).isPresent());
        assertTrue(cacheService.get("galaxy-s25", String.class).isPresent());
        
        // Test invalidate by key
        cacheService.invalidate("iphone17s");
        assertFalse(cacheService.get("iphone17s", String.class).isPresent());
        assertFalse(cacheService.get(2371L, String.class).isPresent());
        assertTrue(cacheService.get("galaxy-s25", String.class).isPresent());
        
        // Test invalidate by ID
        cacheService.invalidate(2372L);
        assertFalse(cacheService.get("galaxy-s25", String.class).isPresent());
        assertFalse(cacheService.get(2372L, String.class).isPresent());
    }

    @Test
    void testInvalidateByPattern() {
        // Arrange
        String iphoneSpec = "iPhone 17s Specification";
        String samsungSpec = "Samsung Galaxy S25 Specification";
        
        cacheService.put("iphone17s", iphoneParameters, iphoneSpec);
        cacheService.put("galaxy-s25", samsungParameters, samsungSpec);
        
        // Test invalidate by pattern (apple brand)
        List<SearchParameter> applePattern = Arrays.asList(
            new SearchParameter("brand", "apple", 2)
        );
        cacheService.invalidateByPattern(applePattern);
        
        // Verify only iPhone is invalidated
        assertFalse(cacheService.get("iphone17s", String.class).isPresent());
        assertTrue(cacheService.get("galaxy-s25", String.class).isPresent());
    }

    @Test
    void testStatistics() {
        // Initial statistics
        CacheStatistics initialStats = cacheService.getStatistics();
        assertEquals(0, initialStats.getTotalKeys());
        assertEquals(0, initialStats.getTotalValues());
        
        // Add some items
        cacheService.put("iphone17s", iphoneParameters, "iPhone Spec");
        cacheService.put("galaxy-s25", samsungParameters, "Samsung Spec");
        
        // Perform some operations
        cacheService.get("iphone17s", String.class); // Hit
        cacheService.get("nonexistent", String.class); // Miss
        
        CacheStatistics stats = cacheService.getStatistics();
        assertTrue(stats.getTotalKeys() > 0);
        assertTrue(stats.getTotalValues() > 0);
        assertTrue(stats.getKeysToValuesRatio() > 1.0); // Multiple keys per value due to hierarchical indexing
        assertEquals(1, stats.getHitCount());
        assertEquals(1, stats.getMissCount());
        assertEquals(0.5, stats.getHitRate(), 0.01);
    }

    @Test
    void testComplexHierarchicalScenario() {
        // Arrange - Set up a complex hierarchy
        List<SearchParameter> laptop1Params = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1),
            new SearchParameter("brand", "apple", 2),
            new SearchParameter("product", "laptop", 3),
            new SearchParameter("model", "macbook-pro", 4)
        );
        
        List<SearchParameter> laptop2Params = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1),
            new SearchParameter("brand", "apple", 2),
            new SearchParameter("product", "laptop", 3),
            new SearchParameter("model", "macbook-air", 4)
        );
        
        cacheService.put("macbook-pro-16", laptop1Params, "MacBook Pro 16-inch");
        cacheService.put("macbook-air-13", laptop2Params, "MacBook Air 13-inch");
        cacheService.put("iphone17s", iphoneParameters, "iPhone 17s");
        
        // Test various search patterns
        
        // Search by region only (should return all 3)
        List<SearchParameter> regionOnly = Arrays.asList(
            new SearchParameter("region", "US", 0)
        );
        assertEquals(3, cacheService.get(regionOnly, String.class).size());
        
        // Search by region + brand (should return all 3 - all Apple products)
        List<SearchParameter> regionBrand = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("brand", "apple", 2)
        );
        assertEquals(3, cacheService.get(regionBrand, String.class).size());
        
        // Search by brand + product (should return only laptops)
        List<SearchParameter> brandProduct = Arrays.asList(
            new SearchParameter("brand", "apple", 2),
            new SearchParameter("product", "laptop", 3)
        );
        assertEquals(2, cacheService.get(brandProduct, String.class).size());
        
        // Search by specific model (should return only one)
        List<SearchParameter> specificModel = Arrays.asList(
            new SearchParameter("model", "macbook-pro", 4)
        );
        assertEquals(1, cacheService.get(specificModel, String.class).size());
        assertEquals("MacBook Pro 16-inch", cacheService.get(specificModel, String.class).get(0));
    }

    @Test
    void testConcurrentOperations() throws InterruptedException {
        // This test verifies basic thread safety
        int numThreads = 10;
        int operationsPerThread = 100;
        List<Thread> threads = new ArrayList<>();
        
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            Thread thread = new Thread(() -> {
                for (int j = 0; j < operationsPerThread; j++) {
                    String key = "key-" + threadId + "-" + j;
                    String value = "value-" + threadId + "-" + j;
                    
                    // Put operation
                    cacheService.put(key, iphoneParameters, value);
                    
                    // Get operation
                    Optional<String> result = cacheService.get(key, String.class);
                    assertTrue(result.isPresent());
                    assertEquals(value, result.get());
                }
            });
            threads.add(thread);
            thread.start();
        }
        
        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }
        
        // Verify statistics
        CacheStatistics stats = cacheService.getStatistics();
        assertEquals(numThreads * operationsPerThread, stats.getTotalValues());
        assertTrue(stats.getHitCount() >= numThreads * operationsPerThread);
    }
}
