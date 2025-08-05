// CacheStatistics.java (Enhanced with reset method)
package com.example.cache.stats;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicLong;

public class CacheStatistics {
    private final AtomicLong hits = new AtomicLong(0);
    private final AtomicLong misses = new AtomicLong(0);
    private final AtomicLong totalKeys = new AtomicLong(0);
    private final AtomicLong totalValues = new AtomicLong(0);
    private final AtomicLong evictions = new AtomicLong(0);
    private final AtomicLong l1Hits = new AtomicLong(0);
    private final AtomicLong l2Hits = new AtomicLong(0);
    private final AtomicLong l1Misses = new AtomicLong(0);
    private final AtomicLong l2Misses = new AtomicLong(0);
    private volatile LocalDateTime createdAt = LocalDateTime.now();
    private volatile LocalDateTime lastResetAt = LocalDateTime.now();

    // Hit/Miss operations
    public void incrementHits() {
        hits.incrementAndGet();
    }

    public void incrementMisses() {
        misses.incrementAndGet();
    }

    public void incrementL1Hits() {
        l1Hits.incrementAndGet();
        incrementHits();
    }

    public void incrementL2Hits() {
        l2Hits.incrementAndGet();
        incrementHits();
    }

    public void incrementL1Misses() {
        l1Misses.incrementAndGet();
    }

    public void incrementL2Misses() {
        l2Misses.incrementAndGet();
    }

    // Key/Value operations
    public void incrementKeys() {
        totalKeys.incrementAndGet();
    }

    public void incrementValues() {
        totalValues.incrementAndGet();
    }

    public void decrementKeys() {
        totalKeys.decrementAndGet();
    }

    public void decrementValues() {
        totalValues.decrementAndGet();
    }

    public void incrementEvictions() {
        evictions.incrementAndGet();
    }

    // Getters
    public long getHits() {
        return hits.get();
    }

    public long getMisses() {
        return misses.get();
    }

    public long getTotalRequests() {
        return getHits() + getMisses();
    }

    public double getHitRatio() {
        long totalRequests = getTotalRequests();
        return totalRequests > 0 ? (double) getHits() / totalRequests : 0.0;
    }

    public double getMissRatio() {
        return 1.0 - getHitRatio();
    }

    public long getTotalKeys() {
        return totalKeys.get();
    }

    public long getTotalValues() {
        return totalValues.get();
    }

    public long getEvictions() {
        return evictions.get();
    }

    public long getL1Hits() {
        return l1Hits.get();
    }

    public long getL2Hits() {
        return l2Hits.get();
    }

    public long getL1Misses() {
        return l1Misses.get();
    }

    public long getL2Misses() {
        return l2Misses.get();
    }

    public double getL1HitRatio() {
        long totalL1Requests = getL1Hits() + getL1Misses();
        return totalL1Requests > 0 ? (double) getL1Hits() / totalL1Requests : 0.0;
    }

    public double getL2HitRatio() {
        long totalL2Requests = getL2Hits() + getL2Misses();
        return totalL2Requests > 0 ? (double) getL2Hits() / totalL2Requests : 0.0;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public LocalDateTime getLastResetAt() {
        return lastResetAt;
    }

    /**
     * Resets all statistics counters to zero and updates the lastResetAt timestamp.
     * This method is thread-safe and atomic.
     */
    public void reset() {
        hits.set(0);
        misses.set(0);
        totalKeys.set(0);
        totalValues.set(0);
        evictions.set(0);
        l1Hits.set(0);
        l2Hits.set(0);
        l1Misses.set(0);
        l2Misses.set(0);
        lastResetAt = LocalDateTime.now();
    }

    /**
     * Performs a complete reset including the creation timestamp.
     * This effectively reinitializes the statistics as if they were just created.
     */
    public void hardReset() {
        reset();
        createdAt = LocalDateTime.now();
        lastResetAt = createdAt;
    }

    /**
     * Returns a snapshot of current statistics.
     * This creates a new CacheStatistics object with the current values,
     * useful for reporting without affecting the original statistics.
     */
    public CacheStatisticsSnapshot getSnapshot() {
        return new CacheStatisticsSnapshot(
            getHits(),
            getMisses(),
            getTotalKeys(),
            getTotalValues(),
            getEvictions(),
            getL1Hits(),
            getL2Hits(),
            getL1Misses(),
            getL2Misses(),
            getCreatedAt(),
            getLastResetAt()
        );
    }

    /**
     * Returns a formatted string representation of the statistics.
     */
    @Override
    public String toString() {
        return String.format(
            "CacheStatistics{" +
            "hits=%d, misses=%d, hitRatio=%.2f%%, " +
            "totalKeys=%d, totalValues=%d, evictions=%d, " +
            "l1Hits=%d, l2Hits=%d, l1HitRatio=%.2f%%, l2HitRatio=%.2f%%, " +
            "createdAt=%s, lastResetAt=%s}",
            getHits(), getMisses(), getHitRatio() * 100,
            getTotalKeys(), getTotalValues(), getEvictions(),
            getL1Hits(), getL2Hits(), getL1HitRatio() * 100, getL2HitRatio() * 100,
            getCreatedAt(), getLastResetAt()
        );
    }

    /**
     * Immutable snapshot of cache statistics at a point in time.
     */
    public static class CacheStatisticsSnapshot {
        private final long hits;
        private final long misses;
        private final long totalKeys;
        private final long totalValues;
        private final long evictions;
        private final long l1Hits;
        private final long l2Hits;
        private final long l1Misses;
        private final long l2Misses;
        private final LocalDateTime createdAt;
        private final LocalDateTime lastResetAt;
        private final LocalDateTime snapshotAt;

        public CacheStatisticsSnapshot(long hits, long misses, long totalKeys, long totalValues,
                                     long evictions, long l1Hits, long l2Hits, long l1Misses,
                                     long l2Misses, LocalDateTime createdAt, LocalDateTime lastResetAt) {
            this.hits = hits;
            this.misses = misses;
            this.totalKeys = totalKeys;
            this.totalValues = totalValues;
            this.evictions = evictions;
            this.l1Hits = l1Hits;
            this.l2Hits = l2Hits;
            this.l1Misses = l1Misses;
            this.l2Misses = l2Misses;
            this.createdAt = createdAt;
            this.lastResetAt = lastResetAt;
            this.snapshotAt = LocalDateTime.now();
        }

        // Getters
        public long getHits() { return hits; }
        public long getMisses() { return misses; }
        public long getTotalRequests() { return hits + misses; }
        public double getHitRatio() { 
            long totalRequests = getTotalRequests();
            return totalRequests > 0 ? (double) hits / totalRequests : 0.0; 
        }
        public double getMissRatio() { return 1.0 - getHitRatio(); }
        public long getTotalKeys() { return totalKeys; }
        public long getTotalValues() { return totalValues; }
        public long getEvictions() { return evictions; }
        public long getL1Hits() { return l1Hits; }
        public long getL2Hits() { return l2Hits; }
        public long getL1Misses() { return l1Misses; }
        public long getL2Misses() { return l2Misses; }
        public double getL1HitRatio() {
            long totalL1Requests = l1Hits + l1Misses;
            return totalL1Requests > 0 ? (double) l1Hits / totalL1Requests : 0.0;
        }
        public double getL2HitRatio() {
            long totalL2Requests = l2Hits + l2Misses;
            return totalL2Requests > 0 ? (double) l2Hits / totalL2Requests : 0.0;
        }
        public LocalDateTime getCreatedAt() { return createdAt; }
        public LocalDateTime getLastResetAt() { return lastResetAt; }
        public LocalDateTime getSnapshotAt() { return snapshotAt; }

        @Override
        public String toString() {
            return String.format(
                "CacheStatisticsSnapshot{" +
                "hits=%d, misses=%d, hitRatio=%.2f%%, " +
                "totalKeys=%d, totalValues=%d, evictions=%d, " +
                "l1Hits=%d, l2Hits=%d, l1HitRatio=%.2f%%, l2HitRatio=%.2f%%, " +
                "createdAt=%s, lastResetAt=%s, snapshotAt=%s}",
                hits, misses, getHitRatio() * 100,
                totalKeys, totalValues, evictions,
                l1Hits, l2Hits, getL1HitRatio() * 100, getL2HitRatio() * 100,
                createdAt, lastResetAt, snapshotAt
            );
        }
    }
}
