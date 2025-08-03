// HierarchicalCacheServiceUnitTest.java
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.redisson.api.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class HierarchicalCacheServiceUnitTest {

    @Mock
    private RedissonClient redissonClient;

    @Mock
    private RBucket<String> stringBucket;

    @Mock
    private RBucket<CachedItem<String>> cachedItemBucket;

    @Mock
    private RSet<String> stringSet;

    @Mock
    private RBatch batch;

    @Mock
    private RKeys keys;

    // Additional mocks for async operations
    @Mock
    private RBucketAsync<Object> asyncObjectBucket;

    @Mock
    private RBucketAsync<CachedItem<String>> asyncCachedItemBucket;

    @Mock
    private RSetAsync<Object> asyncStringSet;

    @Mock
    private RFuture<Void> voidFuture;

    @Mock
    private RFuture<Boolean> booleanFuture;

    private HierarchicalCacheService<String> cacheService;
    private List<SearchParameter> testParameters;
    private final String TEST_VALUE = "Test iPhone Specification";
    private final String TEST_KEY = "iphone17s";
    private final Long TEST_ID = 2371L;

    @BeforeEach
    void setUp() {
        cacheService = new HierarchicalCacheService<>(redissonClient, 300000L);

        testParameters = Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "electronics", 1),
                new SearchParameter("brand", "apple", 2)
        );

        // Only configure the most commonly used mocks that are needed by most tests
        when(redissonClient.createBatch()).thenReturn(batch);
        when(batch.execute()).thenReturn(null);

        // Configure async method returns to prevent NullPointerExceptions for put operations
        when(asyncObjectBucket.setAsync(any())).thenReturn(voidFuture);
        when(asyncObjectBucket.setAsync(any(), anyLong(), any(TimeUnit.class))).thenReturn(voidFuture);
        when(asyncCachedItemBucket.setAsync(any())).thenReturn(voidFuture);
        when(asyncCachedItemBucket.setAsync(any(), anyLong(), any(TimeUnit.class))).thenReturn(voidFuture);
        when(asyncStringSet.addAsync(any())).thenReturn(booleanFuture);

        // Configure batch operations for put operations
        when(batch.getBucket(anyString())).thenReturn(asyncObjectBucket);
        when(batch.getSet(anyString())).thenReturn(asyncStringSet);
    }

    @Test
    void testPutWithKeyAndParameters() {
        // Act
        cacheService.put(TEST_KEY, testParameters, TEST_VALUE);

        // Assert
        verify(redissonClient).createBatch();
        verify(batch).execute();
        verify(batch, atLeastOnce()).getBucket(contains("value:"));
        verify(batch, atLeastOnce()).getBucket(contains("primary:"));
        verify(batch, atLeastOnce()).getSet(contains("param:"));
    }

    @Test
    void testPutWithKeyIdAndParameters() {
        // Act
        cacheService.put(TEST_KEY, TEST_ID, testParameters, TEST_VALUE);

        // Assert
        verify(redissonClient).createBatch();
        verify(batch).execute();
        verify(batch, atLeastOnce()).getBucket(contains("value:"));
        verify(batch, atLeastOnce()).getBucket(contains("primary:"));
        verify(batch, atLeastOnce()).getBucket(contains("longkey:"));
        verify(batch, atLeastOnce()).getSet(contains("param:"));
    }

    @Test
    void testPutWithCustomTtl() {
        // Arrange
        long customTtl = 600000L;
        Mockito.<RBucketAsync<CachedItem<String>>>when(batch.getBucket(startsWith("value:"))).thenReturn(asyncCachedItemBucket);

        // Act
        cacheService.put(TEST_KEY, testParameters, TEST_VALUE, customTtl);

        // Assert
        verify(batch, atLeastOnce()).getBucket(anyString());
        verify(asyncCachedItemBucket, atLeastOnce()).setAsync(any(), eq(customTtl), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void testPutWithNullKey() {
        // Act & Assert
        assertThrows(IllegalArgumentException.class, () ->
                cacheService.put(null, testParameters, TEST_VALUE));
    }

    @Test
    void testPutWithNullParameters() {
        // Act & Assert
        assertThrows(IllegalArgumentException.class, () ->
                cacheService.put(TEST_KEY, null, TEST_VALUE));
    }

    @Test
    void testPutWithNullValue() {
        // Act & Assert
        assertThrows(IllegalArgumentException.class, () ->
                cacheService.put(TEST_KEY, testParameters, null));
    }

    @Test
    void testGetByStringKey() {
        // Arrange
        String uniqueId = TEST_KEY;
        CachedItem<String> cachedItem = new CachedItem<>(TEST_KEY, null, TEST_VALUE, testParameters, 300000L);

        when(stringBucket.get()).thenReturn(uniqueId);
        when(cachedItemBucket.get()).thenReturn(cachedItem);
        when(redissonClient.<String>getBucket("primary:" + TEST_KEY)).thenReturn(stringBucket);
        when(redissonClient.<CachedItem<String>>getBucket("value:" + uniqueId)).thenReturn(cachedItemBucket);

        // Act
        Optional<String> result = cacheService.get(TEST_KEY, String.class);

        // Assert
        assertTrue(result.isPresent());
        assertEquals(TEST_VALUE, result.get());
        verify(redissonClient).getBucket("primary:" + TEST_KEY);
        verify(redissonClient).getBucket("value:" + uniqueId);
    }

    @Test
    void testGetByStringKeyNotFound() {
        // Arrange
        when(stringBucket.get()).thenReturn(null);
        when(redissonClient.<String>getBucket("primary:" + TEST_KEY)).thenReturn(stringBucket);

        // Act
        Optional<String> result = cacheService.get(TEST_KEY, String.class);

        // Assert
        assertFalse(result.isPresent());
        verify(redissonClient).getBucket("primary:" + TEST_KEY);
    }

    @Test
    void testGetByLongId() {
        // Arrange
        String uniqueId = TEST_KEY + ":" + TEST_ID;
        CachedItem<String> cachedItem = new CachedItem<>(TEST_KEY, TEST_ID, TEST_VALUE, testParameters, 300000L);

        when(stringBucket.get()).thenReturn(uniqueId);
        when(cachedItemBucket.get()).thenReturn(cachedItem);
        when(redissonClient.<String>getBucket("longkey:" + TEST_ID)).thenReturn(stringBucket);
        when(redissonClient.<CachedItem<String>>getBucket("value:" + uniqueId)).thenReturn(cachedItemBucket);

        // Act
        Optional<String> result = cacheService.get(TEST_ID, String.class);

        // Assert
        assertTrue(result.isPresent());
        assertEquals(TEST_VALUE, result.get());
        verify(redissonClient).getBucket("longkey:" + TEST_ID);
        verify(redissonClient).getBucket("value:" + uniqueId);
    }

    @Test
    void testGetByKeyAndId() {
        // Arrange
        String uniqueId = TEST_KEY + ":" + TEST_ID;
        CachedItem<String> cachedItem = new CachedItem<>(TEST_KEY, TEST_ID, TEST_VALUE, testParameters, 300000L);

        when(cachedItemBucket.get()).thenReturn(cachedItem);
        when(redissonClient.<CachedItem<String>>getBucket("value:" + uniqueId)).thenReturn(cachedItemBucket);

        // Act
        Optional<String> result = cacheService.get(TEST_KEY, TEST_ID, String.class);

        // Assert
        assertTrue(result.isPresent());
        assertEquals(TEST_VALUE, result.get());
        verify(redissonClient).getBucket("value:" + uniqueId);
    }

    @Test
    void testGetByParameters() {
        // Arrange
        List<SearchParameter> searchParams = Arrays.asList(
                new SearchParameter("region", "US", 0)
        );

        String uniqueId = TEST_KEY + ":" + TEST_ID;
        Set<String> uniqueIds = Collections.singleton(uniqueId);
        CachedItem<String> cachedItem = new CachedItem<>(TEST_KEY, TEST_ID, TEST_VALUE, testParameters, 300000L);

        when(stringSet.readAll()).thenReturn(uniqueIds);
        when(cachedItemBucket.get()).thenReturn(cachedItem);
        Mockito.<RSet<String>>when(redissonClient.getSet(contains("param:"))).thenReturn(stringSet);
        when(redissonClient.<CachedItem<String>>getBucket("value:" + uniqueId)).thenReturn(cachedItemBucket);

        // Act
        List<String> results = cacheService.get(searchParams, String.class);

        // Assert
        assertEquals(1, results.size());
        assertEquals(TEST_VALUE, results.get(0));
    }

    @Test
    void testGetExpiredItem() {
        // Arrange
        String uniqueId = TEST_KEY;
        CachedItem<String> expiredItem = new CachedItem<>(TEST_KEY, null, TEST_VALUE, testParameters, 1L); // Very short TTL

        // Wait to ensure expiration
        try {
            Thread.sleep(2);
        } catch (InterruptedException e) {
        }

        when(stringBucket.get()).thenReturn(uniqueId);
        when(cachedItemBucket.get()).thenReturn(expiredItem);
        when(redissonClient.<String>getBucket("primary:" + TEST_KEY)).thenReturn(stringBucket);
        when(redissonClient.<CachedItem<String>>getBucket("value:" + uniqueId)).thenReturn(cachedItemBucket);

        // Act
        Optional<String> result = cacheService.get(TEST_KEY, String.class);

        // Assert
        assertFalse(result.isPresent());
    }

    @Test
    void testLinkKeyWithId() {
        // Arrange
        String existingUniqueId = TEST_KEY;
        CachedItem<String> existingItem = new CachedItem<>(TEST_KEY, null, TEST_VALUE, testParameters, 300000L);

        when(stringBucket.get()).thenReturn(existingUniqueId).thenReturn(null); // First call returns existing, second returns null for ID check
        when(cachedItemBucket.get()).thenReturn(existingItem);
        when(redissonClient.<String>getBucket("primary:" + TEST_KEY)).thenReturn(stringBucket);
        when(redissonClient.<String>getBucket("longkey:" + TEST_ID)).thenReturn(stringBucket);
        when(redissonClient.<CachedItem<String>>getBucket("value:" + existingUniqueId)).thenReturn(cachedItemBucket);

        // Configure async operations for link
        when(asyncObjectBucket.deleteAsync()).thenReturn(booleanFuture);
        when(asyncCachedItemBucket.deleteAsync()).thenReturn(booleanFuture);
        when(asyncStringSet.removeAsync(any())).thenReturn(booleanFuture);

        // Act
        cacheService.link(TEST_KEY, TEST_ID);

        // Assert
        verify(redissonClient).createBatch();
        verify(batch).execute();
    }

    @Test
    void testLinkKeyWithIdAlreadyExists() {
        // Arrange
        when(stringBucket.get()).thenReturn("existingId");
        when(redissonClient.<String>getBucket("primary:" + TEST_KEY)).thenReturn(stringBucket);
        when(redissonClient.<String>getBucket("longkey:" + TEST_ID)).thenReturn(stringBucket);

        // Act & Assert
        assertThrows(IllegalStateException.class, () ->
                cacheService.link(TEST_KEY, TEST_ID));
    }

    @Test
    void testLinkKeyNotFound() {
        // Arrange
        when(stringBucket.get()).thenReturn(null);
        when(redissonClient.<String>getBucket("primary:" + TEST_KEY)).thenReturn(stringBucket);

        // Act & Assert
        assertThrows(IllegalStateException.class, () ->
                cacheService.link(TEST_KEY, TEST_ID));
    }

    @Test
    void testLinkKeyWithParameters() {
        // Arrange
        String uniqueId = TEST_KEY;
        List<SearchParameter> newParams = Arrays.asList(
                new SearchParameter("color", "black", 4)
        );
        CachedItem<String> existingItem = new CachedItem<>(TEST_KEY, null, TEST_VALUE, testParameters, 300000L);

        when(stringBucket.get()).thenReturn(uniqueId);
        when(cachedItemBucket.get()).thenReturn(existingItem);
        when(redissonClient.<String>getBucket("primary:" + TEST_KEY)).thenReturn(stringBucket);
        when(redissonClient.<CachedItem<String>>getBucket("value:" + uniqueId)).thenReturn(cachedItemBucket);

        // Act
        cacheService.link(TEST_KEY, newParams);

        // Assert
        verify(redissonClient).createBatch();
        verify(batch).execute();
    }

    @Test
    void testLinkParametersAllExist() {
        // Arrange
        String uniqueId = TEST_KEY;
        CachedItem<String> existingItem = new CachedItem<>(TEST_KEY, null, TEST_VALUE, testParameters, 300000L);

        when(stringBucket.get()).thenReturn(uniqueId);
        when(cachedItemBucket.get()).thenReturn(existingItem);
        when(redissonClient.<String>getBucket("primary:" + TEST_KEY)).thenReturn(stringBucket);
        when(redissonClient.<CachedItem<String>>getBucket("value:" + uniqueId)).thenReturn(cachedItemBucket);

        // Act & Assert
        assertThrows(IllegalStateException.class, () ->
                cacheService.link(TEST_KEY, testParameters)); // Same parameters as existing
    }

    @Test
    void testGetOrComputeHit() {
        // Arrange
        String uniqueId = TEST_KEY;
        CachedItem<String> cachedItem = new CachedItem<>(TEST_KEY, null, TEST_VALUE, testParameters, 300000L);
        Supplier<String> supplier = mock(Supplier.class);

        when(stringBucket.get()).thenReturn(uniqueId);
        when(cachedItemBucket.get()).thenReturn(cachedItem);
        when(redissonClient.<String>getBucket("primary:" + TEST_KEY)).thenReturn(stringBucket);
        when(redissonClient.<CachedItem<String>>getBucket("value:" + uniqueId)).thenReturn(cachedItemBucket);

        // Act
        String result = cacheService.getOrCompute(TEST_KEY, String.class, supplier);

        // Assert
        assertEquals(TEST_VALUE, result);
        verify(supplier, never()).get(); // Should not compute since cache hit
    }

    @Test
    void testGetOrComputeMiss() {
        // Arrange
        String computedValue = "Computed Value";
        Supplier<String> supplier = () -> computedValue;

        when(stringBucket.get()).thenReturn(null);
        when(redissonClient.<String>getBucket("primary:" + TEST_KEY)).thenReturn(stringBucket);

        // Act
        String result = cacheService.getOrCompute(TEST_KEY, String.class, supplier);

        // Assert
        assertEquals(computedValue, result);
        verify(redissonClient).createBatch(); // Should cache the computed value
    }

    @Test
    void testInvalidateByKey() {
        // Arrange
        String uniqueId = TEST_KEY;
        CachedItem<String> cachedItem = new CachedItem<>(TEST_KEY, null, TEST_VALUE, testParameters, 300000L);

        when(stringBucket.get()).thenReturn(uniqueId);
        when(cachedItemBucket.get()).thenReturn(cachedItem);
        when(redissonClient.<String>getBucket("primary:" + TEST_KEY)).thenReturn(stringBucket);
        when(redissonClient.<CachedItem<String>>getBucket("value:" + uniqueId)).thenReturn(cachedItemBucket);

        // Configure async operations for invalidation
        when(asyncObjectBucket.deleteAsync()).thenReturn(booleanFuture);
        when(asyncCachedItemBucket.deleteAsync()).thenReturn(booleanFuture);
        when(asyncStringSet.removeAsync(any())).thenReturn(booleanFuture);

        // Act
        cacheService.invalidate(TEST_KEY);

        // Assert
        verify(redissonClient).createBatch();
        verify(batch).execute();
    }

    @Test
    void testInvalidateById() {
        // Arrange
        String uniqueId = TEST_KEY + ":" + TEST_ID;
        CachedItem<String> cachedItem = new CachedItem<>(TEST_KEY, TEST_ID, TEST_VALUE, testParameters, 300000L);

        when(stringBucket.get()).thenReturn(uniqueId);
        when(cachedItemBucket.get()).thenReturn(cachedItem);
        when(redissonClient.<String>getBucket("longkey:" + TEST_ID)).thenReturn(stringBucket);
        when(redissonClient.<CachedItem<String>>getBucket("value:" + uniqueId)).thenReturn(cachedItemBucket);

        // Configure async operations for invalidation
        when(asyncObjectBucket.deleteAsync()).thenReturn(booleanFuture);
        when(asyncCachedItemBucket.deleteAsync()).thenReturn(booleanFuture);
        when(asyncStringSet.removeAsync(any())).thenReturn(booleanFuture);

        // Act
        cacheService.invalidate(TEST_ID);

        // Assert
        verify(redissonClient).createBatch();
        verify(batch).execute();
    }

    @Test
    void testInvalidateAll() {
        // Arrange
        when(redissonClient.getKeys()).thenReturn(keys);

        // Act
        cacheService.invalidateAll();

        // Assert
        verify(keys).deleteByPattern("primary:*");
        verify(keys).deleteByPattern("longkey:*");
        verify(keys).deleteByPattern("param:*");
        verify(keys).deleteByPattern("value:*");
        verify(keys).deleteByPattern("meta:*");
    }

    @Test
    void testStatistics() {
        // Act
        cacheService.put(TEST_KEY, testParameters, TEST_VALUE);
        CacheStatistics stats = cacheService.getStatistics();

        // Assert
        assertTrue(stats.getTotalValues() > 0);
        assertTrue(stats.getTotalKeys() > 0);
        assertTrue(stats.getKeysToValuesRatio() > 0);
    }

    @Test
    void testGetWithNullKey() {
        // Act
        Optional<String> result = cacheService.get((String) null, String.class);

        // Assert
        assertFalse(result.isPresent());
    }

    @Test
    void testGetWithNullId() {
        // Act
        Optional<String> result = cacheService.get((Long) null, String.class);

        // Assert
        assertFalse(result.isPresent());
    }

    @Test
    void testGetByParametersEmpty() {
        // Act
        List<String> results = cacheService.get(Collections.emptyList(), String.class);

        // Assert
        assertTrue(results.isEmpty());
    }

    @Test
    void testGetByParametersNull() {
        // Act
        List<String> results = cacheService.get((List<SearchParameter>) null, String.class);

        // Assert
        assertTrue(results.isEmpty());
    }

    @Test
    void testShutdown() {
        // Act
        cacheService.shutdown();

        // Assert
        verify(redissonClient).shutdown();
    }
}