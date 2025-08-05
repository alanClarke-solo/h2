// CacheController.java (Enhanced with statistics reset endpoints)
package com.example.cache.controller;

import com.example.cache.service.HierarchicalCacheManager;
import com.example.cache.service.HierarchicalCacheService;
import com.example.cache.stats.CacheStatistics;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.Map;

@RestController
@RequestMapping("/api/cache")
public class CacheController {

    private final HierarchicalCacheService cacheService;
    private final HierarchicalCacheManager cacheManager;

    public CacheController(HierarchicalCacheService cacheService, HierarchicalCacheManager cacheManager) {
        this.cacheService = cacheService;
        this.cacheManager = cacheManager;
    }

    @GetMapping("/stats")
    public CacheStatistics getStatistics() {
        return cacheService.getStatistics();
    }

    @GetMapping("/stats/snapshot")
    public CacheStatistics.CacheStatisticsSnapshot getStatisticsSnapshot() {
        return cacheService.getStatistics().getSnapshot();
    }

    @PostMapping("/stats/reset")
    public ResponseEntity<Map<String, String>> resetStatistics() {
        cacheService.getStatistics().reset();
        return ResponseEntity.ok(Map.of(
                "message", "Cache statistics have been reset",
                "timestamp", cacheService.getStatistics().getLastResetAt().toString()
        ));
    }

    @PostMapping("/stats/hard-reset")
    public ResponseEntity<Map<String, String>> hardResetStatistics() {
        cacheService.getStatistics().hardReset();
        return ResponseEntity.ok(Map.of(
                "message", "Cache statistics have been completely reset",
                "timestamp", cacheService.getStatistics().getCreatedAt().toString()
        ));
    }

    @GetMapping("/names")
    public Collection<String> getCacheNames() {
        return cacheManager.getCacheNames();
    }

    @DeleteMapping("/clear/{cacheName}")
    public ResponseEntity<Map<String, String>> clearCache(@PathVariable String cacheName) {
        cacheManager.clearCache(cacheName);
        return ResponseEntity.ok(Map.of(
                "message", "Cache '" + cacheName + "' has been cleared"
        ));
    }

    @DeleteMapping("/clear-all")
    public ResponseEntity<Map<String, String>> clearAllCaches() {
        cacheManager.clearAllCaches();
        cacheService.invalidateAll();
        return ResponseEntity.ok(Map.of(
                "message", "All caches have been cleared"
        ));
    }

    @DeleteMapping("/evict/{cacheName}")
    public ResponseEntity<Map<String, String>> evictFromCache(
            @PathVariable String cacheName,
            @RequestParam String key) {
        var cache = cacheManager.getCache(cacheName);
        if (cache != null) {
            cache.evict(key);
            return ResponseEntity.ok(Map.of(
                    "message", "Key '" + key + "' has been evicted from cache '" + cacheName + "'"
            ));
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        CacheStatistics stats = cacheService.getStatistics();
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "totalRequests", stats.getTotalRequests(),
                "hitRatio", String.format("%.2f%%", stats.getHitRatio() * 100),
                "totalKeys", stats.getTotalKeys(),
                "totalValues", stats.getTotalValues(),
                "uptime", java.time.Duration.between(stats.getCreatedAt(), java.time.LocalDateTime.now()).toString()
        ));
    }
}