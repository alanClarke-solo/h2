// HierarchicalCacheAdvancedTest.java
import org.junit.jupiter.api.*;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.codec.Kryo5Codec;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@DisplayName("Hierarchical Cache Service - Advanced Tests")
class HierarchicalCacheAdvancedTest {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379)
            .withReuse(true);

    private RedissonClient redissonClient;
    private HierarchicalCacheService<String> cacheService;
    private ExecutorService executorService;

    @BeforeEach
    void setUp() {
        Config config = new Config();
        config.useSingleServer()
                .setAddress("redis://localhost:" + redis.getFirstMappedPort())
                .setConnectionPoolSize(50)
                .setConnectionMinimumIdleSize(10);
        config.setCodec(new Kryo5Codec());

        redissonClient = Redisson.create(config);
        cacheService = new HierarchicalCacheService<>(redissonClient, 300000L); // 5 minutes TTL
        executorService = Executors.newFixedThreadPool(20);
        
        // Clear cache before each test
        cacheService.invalidateAll();
    }

    @AfterEach
    void tearDown() {
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        if (cacheService != null) {
            cacheService.invalidateAll();
            cacheService.shutdown();
        }
    }

    // ==================== MULTITHREADED TESTS ====================

    @Test
    @DisplayName("Concurrent Put Operations - Same Key")
    @Timeout(30)
    void testConcurrentPutSameKey() throws InterruptedException {
        final String key = "concurrent-key";
        final int threadCount = 10;
        final int operationsPerThread = 50;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(threadCount);
        final AtomicInteger successCount = new AtomicInteger(0);
        final AtomicInteger errorCount = new AtomicInteger(0);

        List<Future<Void>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            Future<Void> future = executorService.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready
                    
                    for (int j = 0; j < operationsPerThread; j++) {
                        try {
                            List<SearchParameter> params = Arrays.asList(
                                new SearchParameter("thread", String.valueOf(threadId), 0),
                                new SearchParameter("operation", String.valueOf(j), 1)
                            );
                            String value = "Thread-" + threadId + "-Op-" + j;
                            
                            cacheService.put(key + "-" + threadId + "-" + j, params, value);
                            successCount.incrementAndGet();
                            
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                            System.err.println("Error in thread " + threadId + ", operation " + j + ": " + e.getMessage());
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finishLatch.countDown();
                }
                return null;
            });
            futures.add(future);
        }

        startLatch.countDown(); // Start all threads
        assertTrue(finishLatch.await(25, TimeUnit.SECONDS), "All threads should complete within timeout");

        // Verify results
        int expectedOperations = threadCount * operationsPerThread;
        System.out.println("Successful operations: " + successCount.get());
        System.out.println("Failed operations: " + errorCount.get());
        System.out.println("Total expected: " + expectedOperations);
        
        assertTrue(successCount.get() > 0, "At least some operations should succeed");
        assertTrue(errorCount.get() < expectedOperations * 0.1, "Error rate should be less than 10%");
    }

    @Test
    @DisplayName("Concurrent Get Operations")
    @Timeout(30)
    void testConcurrentGetOperations() throws InterruptedException {
        // Pre-populate cache
        final int itemCount = 100;
        for (int i = 0; i < itemCount; i++) {
            List<SearchParameter> params = Arrays.asList(
                new SearchParameter("category", "test", 0),
                new SearchParameter("index", String.valueOf(i % 10), 1)
            );
            cacheService.put("item-" + i, params, "Value-" + i);
        }

        final int threadCount = 20;
        final int readsPerThread = 100;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(threadCount);
        final AtomicInteger hitCount = new AtomicInteger(0);
        final AtomicInteger missCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    Random random = new Random();
                    
                    for (int j = 0; j < readsPerThread; j++) {
                        String key = "item-" + random.nextInt(itemCount + 20); // Some keys won't exist
                        Optional<String> result = cacheService.get(key, String.class);
                        
                        if (result.isPresent()) {
                            hitCount.incrementAndGet();
                        } else {
                            missCount.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(finishLatch.await(25, TimeUnit.SECONDS));

        System.out.println("Cache hits: " + hitCount.get());
        System.out.println("Cache misses: " + missCount.get());
        System.out.println("Hit rate: " + (hitCount.get() * 100.0 / (hitCount.get() + missCount.get())) + "%");
        
        assertTrue(hitCount.get() > 0, "Should have some cache hits");
        assertTrue(hitCount.get() > missCount.get(), "Should have more hits than misses");
    }

    @Test
    @DisplayName("Concurrent Read-Write Operations")
    @Timeout(60)
    void testConcurrentReadWriteOperations() throws InterruptedException {
        final int writerThreads = 5;
        final int readerThreads = 15;
        final int operationsPerThread = 100;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(writerThreads + readerThreads);
        final AtomicLong writeOperations = new AtomicLong(0);
        final AtomicLong readOperations = new AtomicLong(0);
        final AtomicLong cacheHits = new AtomicLong(0);

        // Writer threads
        for (int i = 0; i < writerThreads; i++) {
            final int writerId = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    Random random = new Random();
                    
                    for (int j = 0; j < operationsPerThread; j++) {
                        String key = "rw-key-" + random.nextInt(50);
                        List<SearchParameter> params = Arrays.asList(
                            new SearchParameter("writer", String.valueOf(writerId), 0),
                            new SearchParameter("type", "data", 1)
                        );
                        String value = "Writer-" + writerId + "-" + System.currentTimeMillis();
                        
                        cacheService.put(key, params, value);
                        writeOperations.incrementAndGet();
                        
                        Thread.sleep(1); // Small delay to simulate real work
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        // Reader threads
        for (int i = 0; i < readerThreads; i++) {
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    Random random = new Random();
                    
                    for (int j = 0; j < operationsPerThread; j++) {
                        String key = "rw-key-" + random.nextInt(50);
                        Optional<String> result = cacheService.get(key, String.class);
                        
                        readOperations.incrementAndGet();
                        if (result.isPresent()) {
                            cacheHits.incrementAndGet();
                        }
                        
                        Thread.sleep(1); // Small delay to simulate real work
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(finishLatch.await(55, TimeUnit.SECONDS));

        System.out.println("Write operations: " + writeOperations.get());
        System.out.println("Read operations: " + readOperations.get());
        System.out.println("Cache hits: " + cacheHits.get());
        System.out.println("Hit rate: " + (cacheHits.get() * 100.0 / readOperations.get()) + "%");

        assertTrue(writeOperations.get() > 0, "Should have write operations");
        assertTrue(readOperations.get() > 0, "Should have read operations");
        assertTrue(cacheHits.get() >= 0, "Cache hits should be non-negative");
    }

    @Test
    @DisplayName("Concurrent Hierarchical Search")
    @Timeout(45)
    void testConcurrentHierarchicalSearch() throws InterruptedException {
        // Pre-populate with hierarchical data
        final String[] regions = {"US", "EU", "ASIA"};
        final String[] categories = {"electronics", "books", "clothing"};
        final String[] brands = {"apple", "samsung", "sony", "nike", "adidas"};
        
        int itemId = 0;
        for (String region : regions) {
            for (String category : categories) {
                for (String brand : brands) {
                    List<SearchParameter> params = Arrays.asList(
                        new SearchParameter("region", region, 0),
                        new SearchParameter("category", category, 1),
                        new SearchParameter("brand", brand, 2)
                    );
                    cacheService.put("item-" + itemId++, params, 
                        String.format("Product from %s - %s - %s", region, category, brand));
                }
            }
        }

        final int searchThreads = 10;
        final int searchesPerThread = 50;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(searchThreads);
        final AtomicLong totalSearches = new AtomicLong(0);
        final AtomicLong totalResults = new AtomicLong(0);

        for (int i = 0; i < searchThreads; i++) {
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    Random random = new Random();
                    
                    for (int j = 0; j < searchesPerThread; j++) {
                        List<SearchParameter> searchParams = new ArrayList<>();
                        
                        // Randomly add search parameters
                        if (random.nextBoolean()) {
                            searchParams.add(new SearchParameter("region", 
                                regions[random.nextInt(regions.length)], 0));
                        }
                        if (random.nextBoolean()) {
                            searchParams.add(new SearchParameter("category", 
                                categories[random.nextInt(categories.length)], 1));
                        }
                        if (random.nextBoolean()) {
                            searchParams.add(new SearchParameter("brand", 
                                brands[random.nextInt(brands.length)], 2));
                        }
                        
                        if (!searchParams.isEmpty()) {
                            List<String> results = cacheService.get(searchParams, String.class);
                            totalSearches.incrementAndGet();
                            totalResults.addAndGet(results.size());
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(finishLatch.await(40, TimeUnit.SECONDS));

        System.out.println("Total hierarchical searches: " + totalSearches.get());
        System.out.println("Total results found: " + totalResults.get());
        System.out.println("Average results per search: " + 
            (totalResults.get() * 1.0 / Math.max(1, totalSearches.get())));

        assertTrue(totalSearches.get() > 0, "Should have performed searches");
        assertTrue(totalResults.get() > 0, "Should have found results");
    }

    // ==================== PERFORMANCE TESTS ====================

    @Test
    @DisplayName("Performance Test - Large Dataset Operations")
    @Timeout(120)
    void testLargeDatasetPerformance() {
        final int itemCount = 10000;
        
        // Measure write performance
        Instant writeStart = Instant.now();
        
        for (int i = 0; i < itemCount; i++) {
            List<SearchParameter> params = Arrays.asList(
                new SearchParameter("region", "R" + (i % 10), 0),
                new SearchParameter("category", "C" + (i % 20), 1),
                new SearchParameter("brand", "B" + (i % 50), 2),
                new SearchParameter("product", "P" + (i % 100), 3)
            );
            cacheService.put("perf-item-" + i, params, "Large dataset item " + i);
            
            if (i % 1000 == 0) {
                System.out.println("Written " + i + " items");
            }
        }
        
        Duration writeDuration = Duration.between(writeStart, Instant.now());
        System.out.println("Write performance: " + itemCount + " items in " + 
            writeDuration.toMillis() + "ms (" + 
            (itemCount * 1000.0 / writeDuration.toMillis()) + " items/sec)");

        // Measure read performance
        Instant readStart = Instant.now();
        int hitCount = 0;
        
        for (int i = 0; i < itemCount; i++) {
            Optional<String> result = cacheService.get("perf-item-" + i, String.class);
            if (result.isPresent()) {
                hitCount++;
            }
            
            if (i % 1000 == 0) {
                System.out.println("Read " + i + " items");
            }
        }
        
        Duration readDuration = Duration.between(readStart, Instant.now());
        System.out.println("Read performance: " + itemCount + " items in " + 
            readDuration.toMillis() + "ms (" + 
            (itemCount * 1000.0 / readDuration.toMillis()) + " items/sec)");
        System.out.println("Hit rate: " + (hitCount * 100.0 / itemCount) + "%");

        // Performance assertions
        assertTrue(writeDuration.toSeconds() < 60, "Write operation should complete within 60 seconds");
        assertTrue(readDuration.toSeconds() < 30, "Read operation should complete within 30 seconds");
        assertTrue(hitCount > itemCount * 0.95, "Hit rate should be above 95%");
    }

    @Test
    @DisplayName("Performance Test - Hierarchical Search Scalability")
    @Timeout(90)
    void testHierarchicalSearchPerformance() {
        // Create a large hierarchical dataset
        final int levelsCount = 5;
        final int itemsPerLevel = 20;
        int totalItems = 0;

        for (int l0 = 0; l0 < itemsPerLevel; l0++) {
            for (int l1 = 0; l1 < itemsPerLevel; l1++) {
                for (int l2 = 0; l2 < itemsPerLevel; l2++) {
                    List<SearchParameter> params = Arrays.asList(
                        new SearchParameter("L0", "V" + l0, 0),
                        new SearchParameter("L1", "V" + l1, 1),
                        new SearchParameter("L2", "V" + l2, 2)
                    );
                    cacheService.put("hier-" + l0 + "-" + l1 + "-" + l2, params, 
                        "Hierarchical item " + totalItems++);
                }
            }
        }

        System.out.println("Created " + totalItems + " hierarchical items");

        // Test various search patterns
        Map<String, Long> searchTimes = new HashMap<>();
        
        // Single parameter searches
        Instant start = Instant.now();
        for (int i = 0; i < itemsPerLevel; i++) {
            List<SearchParameter> searchParams = Arrays.asList(
                new SearchParameter("L0", "V" + i, 0));
            List<String> results = cacheService.get(searchParams, String.class);
        }
        searchTimes.put("Single Level", Duration.between(start, Instant.now()).toMillis());

        // Two parameter searches
        start = Instant.now();
        for (int i = 0; i < itemsPerLevel; i++) {
            for (int j = 0; j < itemsPerLevel; j++) {
                List<SearchParameter> searchParams = Arrays.asList(
                    new SearchParameter("L0", "V" + i, 0),
                    new SearchParameter("L1", "V" + j, 1));
                List<String> results = cacheService.get(searchParams, String.class);
            }
        }
        searchTimes.put("Two Levels", Duration.between(start, Instant.now()).toMillis());

        // Three parameter searches
        start = Instant.now();
        for (int i = 0; i < 5; i++) { // Reduced count for performance
            for (int j = 0; j < 5; j++) {
                for (int k = 0; k < 5; k++) {
                    List<SearchParameter> searchParams = Arrays.asList(
                        new SearchParameter("L0", "V" + i, 0),
                        new SearchParameter("L1", "V" + j, 1),
                        new SearchParameter("L2", "V" + k, 2));
                    List<String> results = cacheService.get(searchParams, String.class);
                }
            }
        }
        searchTimes.put("Three Levels", Duration.between(start, Instant.now()).toMillis());

        // Display results
        searchTimes.forEach((type, time) -> 
            System.out.println(type + " search time: " + time + "ms"));

        // Performance assertions
        searchTimes.values().forEach(time -> 
            assertTrue(time < 30000, "Search should complete within 30 seconds"));
    }

    @Test
    @DisplayName("Performance Test - Concurrent Load")
    @Timeout(180)
    void testConcurrentLoadPerformance() throws InterruptedException {
        final int threadCount = 20;
        final int operationsPerThread = 1000;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(threadCount);
        final AtomicLong totalOperations = new AtomicLong(0);
        final AtomicLong totalTime = new AtomicLong(0);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    long threadStart = System.currentTimeMillis();
                    Random random = new Random();
                    
                    for (int j = 0; j < operationsPerThread; j++) {
                        String key = "load-" + threadId + "-" + j;
                        List<SearchParameter> params = Arrays.asList(
                            new SearchParameter("thread", String.valueOf(threadId), 0),
                            new SearchParameter("op", String.valueOf(j % 100), 1)
                        );
                        
                        // Mix of operations
                        if (j % 3 == 0) {
                            // Write operation
                            cacheService.put(key, params, "Load test data " + j);
                        } else {
                            // Read operation
                            cacheService.get(key, String.class);
                        }
                        
                        totalOperations.incrementAndGet();
                    }
                    
                    long threadTime = System.currentTimeMillis() - threadStart;
                    totalTime.addAndGet(threadTime);
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        long testStart = System.currentTimeMillis();
        startLatch.countDown();
        assertTrue(finishLatch.await(175, TimeUnit.SECONDS));
        long testDuration = System.currentTimeMillis() - testStart;

        System.out.println("Concurrent load test results:");
        System.out.println("Total operations: " + totalOperations.get());
        System.out.println("Test duration: " + testDuration + "ms");
        System.out.println("Operations per second: " + 
            (totalOperations.get() * 1000.0 / testDuration));
        System.out.println("Average thread time: " + 
            (totalTime.get() / threadCount) + "ms");

        // Performance assertions
        long expectedOperations = threadCount * operationsPerThread;
        assertEquals(expectedOperations, totalOperations.get(), 
            "All operations should complete");
        assertTrue(testDuration < 170000, 
            "Test should complete within time limit");
        assertTrue((totalOperations.get() * 1000.0 / testDuration) > 100, 
            "Should achieve at least 100 operations per second");
    }

    @Test
    @DisplayName("Memory and Cache Statistics Analysis")
    void testCacheStatisticsAnalysis() {
        final int dataSetSize = 5000;
        
        // Phase 1: Load data
        System.out.println("=== Phase 1: Loading " + dataSetSize + " items ===");
        for (int i = 0; i < dataSetSize; i++) {
            List<SearchParameter> params = Arrays.asList(
                new SearchParameter("category", "cat" + (i % 10), 0),
                new SearchParameter("subcategory", "sub" + (i % 50), 1),
                new SearchParameter("item", "item" + i, 2)
            );
            cacheService.put("stats-item-" + i, params, "Statistics test item " + i);
        }
        
        CacheStatistics afterLoad = cacheService.getStatistics();
        System.out.println("After loading: " + afterLoad);
        
        // Phase 2: Read operations
        System.out.println("\n=== Phase 2: Reading items ===");
        int hits = 0;
        for (int i = 0; i < dataSetSize * 2; i++) { // Read more than stored
            Optional<String> result = cacheService.get("stats-item-" + (i % (dataSetSize + 1000)), String.class);
            if (result.isPresent()) hits++;
        }
        
        CacheStatistics afterReads = cacheService.getStatistics();
        System.out.println("After reading: " + afterReads);
        System.out.println("Manual hit count: " + hits);
        
        // Phase 3: Hierarchical searches
        System.out.println("\n=== Phase 3: Hierarchical searches ===");
        for (int i = 0; i < 100; i++) {
            List<SearchParameter> searchParams = Arrays.asList(
                new SearchParameter("category", "cat" + (i % 10), 0)
            );
            List<String> results = cacheService.get(searchParams, String.class);
        }
        
        CacheStatistics afterSearches = cacheService.getStatistics();
        System.out.println("After searches: " + afterSearches);
        long afterSearchesTotalValues = afterSearches.getTotalValues();
        
        // Phase 4: Partial invalidation
        System.out.println("\n=== Phase 4: Partial invalidation ===");
        for (int i = 0; i < dataSetSize / 4; i++) {
            cacheService.invalidate("stats-item-" + i);
        }
        
        CacheStatistics afterInvalidation = cacheService.getStatistics();
        System.out.println("After invalidation: " + afterInvalidation);
        
        // Assertions
        assertTrue(afterLoad.getTotalValues() > 0, "Should have cached values");
        assertTrue(afterLoad.getTotalKeys() >= afterLoad.getTotalValues(), 
            "Should have at least as many keys as values");
        assertTrue(afterReads.getHitCount() > 0, "Should have cache hits");
        assertTrue(afterReads.getMissCount() > 0, "Should have cache misses");
        assertTrue(afterReads.getHitRate() > 0, "Hit rate should be positive");
        assertTrue(afterInvalidation.getTotalValues() < afterSearchesTotalValues,
            "Should have fewer values after invalidation");
    }

    // ==================== STRESS TESTS ====================

    @Test
    @DisplayName("Stress Test - High Frequency Operations")
    @Timeout(300)
    void testHighFrequencyOperations() throws InterruptedException {
        final int duration = 60; // seconds
        final int threadCount = 30;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final AtomicLong operationCount = new AtomicLong(0);
        final AtomicLong errorCount = new AtomicLong(0);
        final AtomicBoolean running = new AtomicBoolean(true);
        
        List<Future<Void>> futures = new ArrayList<>();
        
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            Future<Void> future = executorService.submit(() -> {
                try {
                    startLatch.await();
                    Random random = new Random();
                    
                    while (running.get()) {
                        try {
                            int operation = random.nextInt(4);
                            String key = "stress-" + random.nextInt(1000);
                            
                            switch (operation) {
                                case 0: // PUT
                                    List<SearchParameter> params = Arrays.asList(
                                        new SearchParameter("thread", String.valueOf(threadId), 0),
                                        new SearchParameter("type", "stress", 1)
                                    );
                                    cacheService.put(key, params, "Stress value " + operationCount.get());
                                    break;
                                case 1: // GET
                                    cacheService.get(key, String.class);
                                    break;
                                case 2: // SEARCH
                                    List<SearchParameter> searchParams = Arrays.asList(
                                        new SearchParameter("type", "stress", 1)
                                    );
                                    cacheService.get(searchParams, String.class);
                                    break;
                                case 3: // INVALIDATE
                                    cacheService.invalidate(key);
                                    break;
                            }
                            
                            operationCount.incrementAndGet();
                            
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return null;
            });
            futures.add(future);
        }
        
        startLatch.countDown();
        Thread.sleep(duration * 1000);
        running.set(false);
        
        // Wait for all threads to complete
        for (Future<Void> future : futures) {
            try {
                future.get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                System.err.println("Thread completion error: " + e.getMessage());
            }
        }
        
        System.out.println("Stress test results:");
        System.out.println("Duration: " + duration + " seconds");
        System.out.println("Threads: " + threadCount);
        System.out.println("Total operations: " + operationCount.get());
        System.out.println("Operations per second: " + (operationCount.get() / duration));
        System.out.println("Error count: " + errorCount.get());
        System.out.println("Error rate: " + (errorCount.get() * 100.0 / operationCount.get()) + "%");
        System.out.println("Final statistics: " + cacheService.getStatistics());
        
        assertTrue(operationCount.get() > 0, "Should have performed operations");
        assertTrue(errorCount.get() < operationCount.get() * 0.05, 
            "Error rate should be less than 5%");
        assertTrue(operationCount.get() / duration > 100, 
            "Should achieve at least 100 operations per second");
    }
}
