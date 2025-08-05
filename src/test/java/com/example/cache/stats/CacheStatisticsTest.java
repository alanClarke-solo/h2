// CacheStatisticsTest.java
package com.example.cache.stats;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;

class CacheStatisticsTest {

    private CacheStatistics statistics;

    @BeforeEach
    void setUp() {
        statistics = new CacheStatistics();
    }

    @Test
    void testReset() {
        // Given - populate some statistics
        statistics.incrementHits();
        statistics.incrementMisses();
        statistics.incrementKeys();
        statistics.incrementValues();
        statistics.incrementL1Hits();
        statistics.incrementL2Hits();
        
        LocalDateTime beforeReset = LocalDateTime.now();
        
        // When
        statistics.reset();
        
        // Then
        assertThat(statistics.getHits()).isEqualTo(0);
        assertThat(statistics.getMisses()).isEqualTo(0);
        assertThat(statistics.getTotalKeys()).isEqualTo(0);
        assertThat(statistics.getTotalValues()).isEqualTo(0);
        assertThat(statistics.getL1Hits()).isEqualTo(0);
        assertThat(statistics.getL2Hits()).isEqualTo(0);
        assertThat(statistics.getLastResetAt()).isAfterOrEqualTo(beforeReset);
        
        // Creation time should remain unchanged
        assertThat(statistics.getCreatedAt()).isBefore(statistics.getLastResetAt());
    }

    @Test
    void testHardReset() {
        // Given - populate some statistics and wait a bit
        statistics.incrementHits();
        statistics.incrementMisses();
        
        LocalDateTime originalCreatedAt = statistics.getCreatedAt();
        
        // When
        statistics.hardReset();
        
        // Then
        assertThat(statistics.getHits()).isEqualTo(0);
        assertThat(statistics.getMisses()).isEqualTo(0);
        assertThat(statistics.getCreatedAt()).isAfterOrEqualTo(originalCreatedAt);
        assertThat(statistics.getLastResetAt()).isEqualTo(statistics.getCreatedAt());
    }

    @Test
    void testSnapshot() {
        // Given
        statistics.incrementHits();
        statistics.incrementMisses();
        statistics.incrementKeys();
        
        // When
        CacheStatistics.CacheStatisticsSnapshot snapshot = statistics.getSnapshot();
        
        // Then
        assertThat(snapshot.getHits()).isEqualTo(1);
        assertThat(snapshot.getMisses()).isEqualTo(1);
        assertThat(snapshot.getTotalKeys()).isEqualTo(1);
        assertThat(snapshot.getHitRatio()).isEqualTo(0.5);
        assertThat(snapshot.getSnapshotAt()).isNotNull();
        
        // Modifying original should not affect snapshot
        statistics.incrementHits();
        assertThat(snapshot.getHits()).isEqualTo(1); // unchanged
        assertThat(statistics.getHits()).isEqualTo(2); // changed
    }
}
