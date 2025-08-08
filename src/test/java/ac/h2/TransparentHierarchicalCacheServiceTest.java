package ac.h2;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TransparentHierarchicalCacheServiceTest {

    private static final String TEST_KEY = "testKey";

    @Test
    void put_WhenKeyIsNull_ShouldThrowIllegalArgumentException() {
        HierarchicalCacheService<Object> redisCacheMock = mock(HierarchicalCacheService.class);
        DatabaseCacheProvider<Object> databaseCacheMock = mock(DatabaseCacheProvider.class);
        CacheConfiguration config = mock(CacheConfiguration.class);

        TransparentHierarchicalCacheService<Object> service =
                new TransparentHierarchicalCacheService<>(redisCacheMock, databaseCacheMock, config);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> service.put(null, Collections.singletonList(new SearchParameter("name", "value", 1)), "value"));
        assertEquals("Key cannot be null", exception.getMessage());
    }

    @Test
    void put_WhenValueIsNull_ShouldThrowIllegalArgumentException() {
        HierarchicalCacheService<Object> redisCacheMock = mock(HierarchicalCacheService.class);
        DatabaseCacheProvider<Object> databaseCacheMock = mock(DatabaseCacheProvider.class);
        CacheConfiguration config = mock(CacheConfiguration.class);

        TransparentHierarchicalCacheService<Object> service =
                new TransparentHierarchicalCacheService<>(redisCacheMock, databaseCacheMock, config);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> service.put(TEST_KEY, Collections.singletonList(new SearchParameter("name", "value", 1)), null));
        assertEquals("Value cannot be null", exception.getMessage());
    }

    @Test
    void put_WithValidKeyAndValue_AreStoredInRemoteCache() {
        HierarchicalCacheService<Object> redisCacheMock = mock(HierarchicalCacheService.class);
        DatabaseCacheProvider<Object> databaseCacheMock = mock(DatabaseCacheProvider.class);
        CacheConfiguration config = Mockito.mock(CacheConfiguration.class);

        when(config.isRemoteCacheEnabled()).thenReturn(true);
        when(config.getRemoteCacheTtlMillis()).thenReturn(60000L);
        when(config.isLocalCacheEnabled()).thenReturn(false);

        TransparentHierarchicalCacheService<Object> service =
                new TransparentHierarchicalCacheService<>(redisCacheMock, databaseCacheMock, config);

        String value = "value";
        List<SearchParameter> params = Collections.singletonList(new SearchParameter("param", "val", 1));
        service.put(TEST_KEY, params, value);

        verify(redisCacheMock, times(1))
                .put(TEST_KEY, params, value, 60000L);
    }

    @Test
    void put_WithLocalCacheEnabled_StoresInLocalCache() {
        HierarchicalCacheService<Object> redisCacheMock = mock(HierarchicalCacheService.class);
        DatabaseCacheProvider<Object> databaseCacheMock = mock(DatabaseCacheProvider.class);
        CacheConfiguration config = Mockito.mock(CacheConfiguration.class);

        when(config.isRemoteCacheEnabled()).thenReturn(true);
        when(config.isLocalCacheEnabled()).thenReturn(true);
        when(config.getLocalCacheTtlMillis()).thenReturn(60000L);
        when(config.getRemoteCacheTtlMillis()).thenReturn(60000L);
        when(config.getMaxLocalCacheSize()).thenReturn(100L);

        TransparentHierarchicalCacheService<Object> service =
                new TransparentHierarchicalCacheService<>(redisCacheMock, databaseCacheMock, config);

        String value = "value";
        List<SearchParameter> params = Collections.singletonList(new SearchParameter("param", "val", 1));
        service.put(TEST_KEY, params, value);

        verify(redisCacheMock).put(TEST_KEY, params, value, 60000L);
    }

    @Test
    void put_WithTTL_SetsCorrectExpirationTime() {
        HierarchicalCacheService<Object> redisCacheMock = mock(HierarchicalCacheService.class);
        DatabaseCacheProvider<Object> databaseCacheMock = mock(DatabaseCacheProvider.class);
        CacheConfiguration config = mock(CacheConfiguration.class);

        when(config.isRemoteCacheEnabled()).thenReturn(true);

        TransparentHierarchicalCacheService<Object> service =
                new TransparentHierarchicalCacheService<>(redisCacheMock, databaseCacheMock, config);

        String value = "value";
        long customTtl = 30000L;
        List<SearchParameter> params = Collections.singletonList(new SearchParameter("param", "val", 1));
        service.put(TEST_KEY, params, value, customTtl);

        verify(redisCacheMock).put(TEST_KEY, params, value, customTtl);
    }

    @Test
    void put_WithValidKeyParamsValueAndId_StoresInLocalAndRemoteCache() {
        HierarchicalCacheService<Object> redisCacheMock = mock(HierarchicalCacheService.class);
        DatabaseCacheProvider<Object> databaseCacheMock = mock(DatabaseCacheProvider.class);
        CacheConfiguration config = mock(CacheConfiguration.class);

        when(config.isRemoteCacheEnabled()).thenReturn(true);
        when(config.isLocalCacheEnabled()).thenReturn(true);
        when(config.getLocalCacheTtlMillis()).thenReturn(60000L);
        when(config.getRemoteCacheTtlMillis()).thenReturn(60000L);

        TransparentHierarchicalCacheService<Object> service =
                new TransparentHierarchicalCacheService<>(redisCacheMock, databaseCacheMock, config);

        String value = "value";
        Long id = 123L;
        List<SearchParameter> params = Collections.singletonList(new SearchParameter("param", "val", 1));
        service.put(TEST_KEY, id, params, value);

        verify(redisCacheMock).put(TEST_KEY, id, params, value, 60000L);
    }

    @Test
    void put_WithNullKeyAndValue_ShouldThrowIllegalArgumentException() {
        HierarchicalCacheService<Object> redisCacheMock = mock(HierarchicalCacheService.class);
        DatabaseCacheProvider<Object> databaseCacheMock = mock(DatabaseCacheProvider.class);
        CacheConfiguration config = mock(CacheConfiguration.class);

        TransparentHierarchicalCacheService<Object> service =
                new TransparentHierarchicalCacheService<>(redisCacheMock, databaseCacheMock, config);

        assertThrows(IllegalArgumentException.class,
                () -> service.put(null, null, null),
                "Expected IllegalArgumentException when key and value are null");
    }

    @Test
    void put_WithLocalAndRemoteEnabled_StoresInBothCaches() {
        HierarchicalCacheService<Object> redisCacheMock = mock(HierarchicalCacheService.class);
        DatabaseCacheProvider<Object> databaseCacheMock = mock(DatabaseCacheProvider.class);
        CacheConfiguration config = mock(CacheConfiguration.class);

        when(config.isRemoteCacheEnabled()).thenReturn(true);
        when(config.isLocalCacheEnabled()).thenReturn(true);
        when(config.getRemoteCacheTtlMillis()).thenReturn(120000L);
        when(config.getLocalCacheTtlMillis()).thenReturn(120000L);

        TransparentHierarchicalCacheService<Object> service =
                new TransparentHierarchicalCacheService<>(redisCacheMock, databaseCacheMock, config);

        String value = "testValue";
        List<SearchParameter> params = Collections.singletonList(new SearchParameter("param", "value", 1));
        service.put("testKey", params, value);

        verify(redisCacheMock).put("testKey", params, value, 120000L);
    }

    @Test
    void put_WithRemoteCacheEnabled_ExceptionInRemoteCacheHandlingGracefully() {
        HierarchicalCacheService<Object> redisCacheMock = mock(HierarchicalCacheService.class);
        DatabaseCacheProvider<Object> databaseCacheMock = mock(DatabaseCacheProvider.class);
        CacheConfiguration config = mock(CacheConfiguration.class);

        when(config.isRemoteCacheEnabled()).thenReturn(true);
        doThrow(new RuntimeException("Remote cache error")).when(redisCacheMock).put(anyString(), anyList(), any(), anyLong());

        TransparentHierarchicalCacheService<Object> service =
                new TransparentHierarchicalCacheService<>(redisCacheMock, databaseCacheMock, config);

        assertDoesNotThrow(() -> service.put(TEST_KEY, Collections.singletonList(new SearchParameter("name", "value", 1)), "value"));
    }

    @Test
    void put_WithDuplicateSearchParameters_ShouldHandleCorrectly() {
        HierarchicalCacheService<Object> redisCacheMock = mock(HierarchicalCacheService.class);
        DatabaseCacheProvider<Object> databaseCacheMock = mock(DatabaseCacheProvider.class);
        CacheConfiguration config = mock(CacheConfiguration.class);

        when(config.isRemoteCacheEnabled()).thenReturn(true);

        TransparentHierarchicalCacheService<Object> service =
                new TransparentHierarchicalCacheService<>(redisCacheMock, databaseCacheMock, config);

        List<SearchParameter> params = Arrays.asList(
                new SearchParameter("name", "value", 1),
                new SearchParameter("name", "value", 1)
        );
        String value = "value";
        service.put(TEST_KEY, params, value);

        verify(redisCacheMock).put(TEST_KEY, params, value, config.getRemoteCacheTtlMillis());
    }

    @Test
    void put_WithExceedingMaximumSize_GracefullyHandles() {
        HierarchicalCacheService<Object> redisCacheMock = mock(HierarchicalCacheService.class);
        DatabaseCacheProvider<Object> databaseCacheMock = mock(DatabaseCacheProvider.class);
        CacheConfiguration config = mock(CacheConfiguration.class);

        when(config.isRemoteCacheEnabled()).thenReturn(true);

        TransparentHierarchicalCacheService<Object> service =
                new TransparentHierarchicalCacheService<>(redisCacheMock, databaseCacheMock, config);

        StringBuilder largeValue = new StringBuilder();
        for (int i = 0; i < 100000; i++) {
            largeValue.append("test");
        }

        List<SearchParameter> params = Collections.singletonList(new SearchParameter("param", "val", 1));
        service.put(TEST_KEY, params, largeValue.toString());

        verify(redisCacheMock).put(TEST_KEY, params, largeValue.toString(), config.getRemoteCacheTtlMillis());
    }

    @Test
    void put_WithFallbackIncludesDatabase_PersistsCorrectly() {
        HierarchicalCacheService<Object> redisCacheMock = mock(HierarchicalCacheService.class);
        DatabaseCacheProvider<Object> databaseCacheMock = mock(DatabaseCacheProvider.class);
        CacheConfiguration config = mock(CacheConfiguration.class);

        when(config.isDatabaseCacheEnabled()).thenReturn(true);

        TransparentHierarchicalCacheService<Object> service =
                new TransparentHierarchicalCacheService<>(redisCacheMock, databaseCacheMock, config);

        List<SearchParameter> params = Collections.singletonList(new SearchParameter("param", "val", 1));
        service.put(TEST_KEY, params, "value");

        verify(databaseCacheMock).put(TEST_KEY, params, "value", config.getRemoteCacheTtlMillis());
    }

    @Test
    void put_WithSameKeyDifferentTTLs_ShouldHandleCorrectly() {
        HierarchicalCacheService<Object> redisCacheMock = mock(HierarchicalCacheService.class);
        DatabaseCacheProvider<Object> databaseCacheMock = mock(DatabaseCacheProvider.class);
        CacheConfiguration config = mock(CacheConfiguration.class);

        when(config.isRemoteCacheEnabled()).thenReturn(true);

        TransparentHierarchicalCacheService<Object> service =
                new TransparentHierarchicalCacheService<>(redisCacheMock, databaseCacheMock, config);

        List<SearchParameter> params = Collections.singletonList(new SearchParameter("param", "val", 1));
        service.put(TEST_KEY, params, "value", 30000L);
        service.put(TEST_KEY, params, "value", 60000L);

        verify(redisCacheMock).put(TEST_KEY, params, "value", 30000L);
        verify(redisCacheMock).put(TEST_KEY, params, "value", 60000L);
    }

    @Test
    void put_WithCustomTTL_SetsExpirationForBothCaches() {
        HierarchicalCacheService<Object> redisCacheMock = mock(HierarchicalCacheService.class);
        DatabaseCacheProvider<Object> databaseCacheMock = mock(DatabaseCacheProvider.class);
        CacheConfiguration config = mock(CacheConfiguration.class);

        when(config.isLocalCacheEnabled()).thenReturn(true);
        when(config.isRemoteCacheEnabled()).thenReturn(true);

        TransparentHierarchicalCacheService<Object> service =
                new TransparentHierarchicalCacheService<>(redisCacheMock, databaseCacheMock, config);

        String value = "testValue";
        long customTtl = 45000L;
        List<SearchParameter> params = Collections.singletonList(new SearchParameter("param", "value", 1));
        service.put("testKey", params, value, customTtl);

        verify(redisCacheMock).put("testKey", params, value, customTtl);
    }

    @Test
    void put_WithNullParameters_ShouldHandleGracefully() {
        HierarchicalCacheService<Object> redisCacheMock = mock(HierarchicalCacheService.class);
        DatabaseCacheProvider<Object> databaseCacheMock = mock(DatabaseCacheProvider.class);
        CacheConfiguration config = mock(CacheConfiguration.class);

        when(config.isRemoteCacheEnabled()).thenReturn(true);

        TransparentHierarchicalCacheService<Object> service =
                new TransparentHierarchicalCacheService<>(redisCacheMock, databaseCacheMock, config);

        String value = "testValue";
        service.put("testKey", null, value);

        verify(redisCacheMock, times(1)).put("testKey", Collections.emptyList(), value, config.getRemoteCacheTtlMillis());
    }

    @Test
    void put_WithLocalAndRemoteCachingDisabled_ShouldNotCache() {
        HierarchicalCacheService<Object> redisCacheMock = mock(HierarchicalCacheService.class);
        DatabaseCacheProvider<Object> databaseCacheMock = mock(DatabaseCacheProvider.class);
        CacheConfiguration config = mock(CacheConfiguration.class);

        when(config.isLocalCacheEnabled()).thenReturn(false);
        when(config.isRemoteCacheEnabled()).thenReturn(false);

        TransparentHierarchicalCacheService<Object> service =
                new TransparentHierarchicalCacheService<>(redisCacheMock, databaseCacheMock, config);

        String value = "value";
        List<SearchParameter> params = Collections.singletonList(new SearchParameter("param", "val", 1));
        service.put("testKey", params, value);

        verifyNoInteractions(redisCacheMock);
        verifyNoInteractions(databaseCacheMock);
    }

    @Test
    void put_WithLocalCacheEnabled_RemoteCacheDisabled_ShouldOnlyUseLocalCache() {
        HierarchicalCacheService<Object> redisCacheMock = mock(HierarchicalCacheService.class);
        DatabaseCacheProvider<Object> databaseCacheMock = mock(DatabaseCacheProvider.class);
        CacheConfiguration config = mock(CacheConfiguration.class);

        when(config.isLocalCacheEnabled()).thenReturn(true);
        when(config.isRemoteCacheEnabled()).thenReturn(false);
        when(config.getLocalCacheTtlMillis()).thenReturn(60000L);
        when(config.getMaxLocalCacheSize()).thenReturn(100L);

        TransparentHierarchicalCacheService<Object> service =
                new TransparentHierarchicalCacheService<>(redisCacheMock, databaseCacheMock, config);

        String value = "value";
        List<SearchParameter> params = Collections.singletonList(new SearchParameter("param", "val", 1));
        service.put("testKey", params, value);

        verifyNoInteractions(redisCacheMock);
    }

    @Test
    void put_WithUniqueKeyAndId_ShouldLinkProperly() {
        HierarchicalCacheService<Object> redisCacheMock = mock(HierarchicalCacheService.class);
        DatabaseCacheProvider<Object> databaseCacheMock = mock(DatabaseCacheProvider.class);
        CacheConfiguration config = mock(CacheConfiguration.class);

        when(config.isRemoteCacheEnabled()).thenReturn(true);
        when(config.isLocalCacheEnabled()).thenReturn(true);
        when(config.getLocalCacheTtlMillis()).thenReturn(60000L);
        when(config.getRemoteCacheTtlMillis()).thenReturn(60000L);

        TransparentHierarchicalCacheService<Object> service =
                new TransparentHierarchicalCacheService<>(redisCacheMock, databaseCacheMock, config);

        String value = "value";
        Long id = 456L;
        List<SearchParameter> params = Collections.singletonList(new SearchParameter("param", "val", 1));
        service.put("testKey", id, params, value);

        verify(redisCacheMock).put("testKey", id, params, value, 60000L);
    }
}