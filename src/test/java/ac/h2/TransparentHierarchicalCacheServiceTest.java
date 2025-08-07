package ac.h2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TransparentHierarchicalCacheServiceTest {
    
    @Mock
    private HierarchicalCacheService<String> redisCache;
    @Mock private DatabaseCacheProvider<String> databaseCache;    // NEW
    @Mock private CacheConfiguration config;
    
    private TransparentHierarchicalCacheService<String> cacheService;
    
    @BeforeEach
    void setUp() {
        // Configure mocks
        when(config.isLocalCacheEnabled()).thenReturn(true);
        when(config.isRemoteCacheEnabled()).thenReturn(true);
        when(config.isDatabaseCacheEnabled()).thenReturn(true);   // NEW
        
        cacheService = new TransparentHierarchicalCacheService<>(
            redisCache, 
            databaseCache,    // NEW parameter
            config
        );
    }
    
    @Test
    void testGet_WithDatabaseFallback() {
        // Arrange
        String key = "test-key";
        String value = "test-value";
        
        // Mock L1 and L2 misses, L3 hit
        when(redisCache.get(key, String.class)).thenReturn(Optional.empty());
        when(databaseCache.get(key, String.class)).thenReturn(Optional.of(value));
        
        // Act
        Optional<String> result = cacheService.get(key, String.class);
        
        // Assert
        assertTrue(result.isPresent());
        assertEquals(value, result.get());
        
        // Verify database was called and write-back occurred
        verify(databaseCache).get(key, String.class);
        verify(redisCache).put(key, Collections.emptyList(), value);  // Write-back to L2
    }
}
