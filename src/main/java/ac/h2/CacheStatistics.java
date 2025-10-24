package ac.h2;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Comprehensive cache statistics for multi-tier hierarchical cache.
 * Tracks statistics for L1 (Local/Caffeine), L2 (Redis), and L3 (Database) cache levels.
 * Thread-safe implementation using atomic operations.
 */
public class CacheStatistics {
    // Overall statistics
    private final AtomicLong requests = new AtomicLong(0);
    private final AtomicLong hits = new AtomicLong(0);
    private final AtomicLong misses = new AtomicLong(0);
    private final AtomicLong totalKeys = new AtomicLong(0);
    private final AtomicLong totalValues = new AtomicLong(0);
    private final AtomicLong evictions = new AtomicLong(0);

    // L1 (Local/Caffeine) cache statistics
    private final AtomicLong l1Hits = new AtomicLong(0);
    private final AtomicLong l1Misses = new AtomicLong(0);
    private final AtomicLong l1Puts = new AtomicLong(0);
    private final AtomicLong l1Evictions = new AtomicLong(0);

    // L2 (Redis/Remote) cache statistics
    private final AtomicLong l2Hits = new AtomicLong(0);
    private final AtomicLong l2Misses = new AtomicLong(0);
    private final AtomicLong l2Puts = new AtomicLong(0);
    private final AtomicLong l2Errors = new AtomicLong(0);

    // L3 (Database) statistics
    private final AtomicLong l3Hits = new AtomicLong(0);
    private final AtomicLong l3Misses = new AtomicLong(0);
    private final AtomicLong l3Puts = new AtomicLong(0);
    private final AtomicLong l3Errors = new AtomicLong(0);

    // Timing
    private volatile LocalDateTime createdAt = LocalDateTime.now();
    private volatile LocalDateTime lastResetAt = LocalDateTime.now();

    // ==================== REQUEST TRACKING ====================

    public void incrementRequests() {
        requests.incrementAndGet();
    }

    public long getRequests() {
        return requests.get();
    }

    public void incrementHits() {
        hits.incrementAndGet();
    }

    public void incrementMisses() {
        misses.incrementAndGet();
    }

    public long getHitCount() {
        return hits.get();
    }

    public long getHits() {
        return hits.get();
    }

    public long getMissCount() {
        return misses.get();
    }

    public long getMisses() {
        return misses.get();
    }

    public long getTotalRequests() {
        return getHits() + getMisses();
    }

    public double getHitRate() {
        long total = hits.get() + misses.get();
        return total == 0 ? 0.0 : (double) hits.get() / total;
    }

    public double getHitRatio() {
        return getHitRate();
    }

    public double getMissRate() {
        return 1.0 - getHitRate();
    }

    public double getMissRatio() {
        return getMissRate();
    }

    // ==================== L1 CACHE STATISTICS ====================

    public void incrementL1Hits() {
        l1Hits.incrementAndGet();
        incrementHits();
    }

    public void incrementL1Misses() {
        l1Misses.incrementAndGet();
    }

    public void incrementL1Puts() {
        l1Puts.incrementAndGet();
    }

    public void incrementL1Evictions() {
        l1Evictions.incrementAndGet();
        evictions.incrementAndGet();
    }

    public long getL1Hits() {
        return l1Hits.get();
    }

    public long getL1Misses() {
        return l1Misses.get();
    }

    public long getL1Puts() {
        return l1Puts.get();
    }

    public long getL1Evictions() {
        return l1Evictions.get();
    }

    public long getL1Requests() {
        return getL1Hits() + getL1Misses();
    }

    public double getL1HitRate() {
        long total = l1Hits.get() + l1Misses.get();
        return total == 0 ? 0.0 : (double) l1Hits.get() / total;
    }

    public double getL1HitRatio() {
        return getL1HitRate();
    }

    // ==================== L2 CACHE STATISTICS ====================

    public void incrementL2Hits() {
        l2Hits.incrementAndGet();
        incrementHits();
    }

    public void incrementL2Misses() {
        l2Misses.incrementAndGet();
    }

    public void incrementL2Puts() {
        l2Puts.incrementAndGet();
    }

    public void incrementL2Errors() {
        l2Errors.incrementAndGet();
    }

    public long getL2Hits() {
        return l2Hits.get();
    }

    public long getL2Misses() {
        return l2Misses.get();
    }

    public long getL2Puts() {
        return l2Puts.get();
    }

    public long getL2Errors() {
        return l2Errors.get();
    }

    public long getL2Requests() {
        return getL2Hits() + getL2Misses();
    }

    public double getL2HitRate() {
        long total = l2Hits.get() + l2Misses.get();
        return total == 0 ? 0.0 : (double) l2Hits.get() / total;
    }

    public double getL2HitRatio() {
        return getL2HitRate();
    }

    // ==================== L3 CACHE STATISTICS ====================

    public void incrementL3Hits() {
        l3Hits.incrementAndGet();
        incrementHits();
    }

    public void incrementL3Misses() {
        l3Misses.incrementAndGet();
    }

    public void incrementL3Puts() {
        l3Puts.incrementAndGet();
    }

    public void incrementL3Errors() {
        l3Errors.incrementAndGet();
    }

    public long getL3Hits() {
        return l3Hits.get();
    }

    public long getL3Misses() {
        return l3Misses.get();
    }

    public long getL3Puts() {
        return l3Puts.get();
    }

    public long getL3Errors() {
        return l3Errors.get();
    }

    public long getL3Requests() {
        return getL3Hits() + getL3Misses();
    }

    public double getL3HitRate() {
        long total = l3Hits.get() + l3Misses.get();
        return total == 0 ? 0.0 : (double) l3Hits.get() / total;
    }

    // ==================== KEY/VALUE TRACKING ====================

    public void incrementKeys() {
        totalKeys.incrementAndGet();
    }

    public void incrementValues() {
        totalValues.incrementAndGet();
    }

    public void decrementKeys() {
        if (totalKeys.get() > 0) {
            totalKeys.decrementAndGet();
        }
    }

    public void decrementValues() {
        if (totalValues.get() > 0) {
            totalValues.decrementAndGet();
        }
    }

    public void incrementEvictions() {
        evictions.incrementAndGet();
    }

    public long getTotalKeys() {
        return totalKeys.get();
    }

    public long getKeys() {
        return totalKeys.get();
    }

    public long getTotalValues() {
        return totalValues.get();
    }

    public long getValues() {
        return totalValues.get();
    }

    public long getEvictions() {
        return evictions.get();
    }

    public double getKeysToValuesRatio() {
        long values = totalValues.get();
        return values == 0 ? 0.0 : (double) totalKeys.get() / values;
    }

    // ==================== TIMING ====================

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public LocalDateTime getLastResetAt() {
        return lastResetAt;
    }

    // ==================== UTILITY METHODS ====================

    /**
     * Resets all statistics counters to zero and updates the lastResetAt timestamp.
     * This method is thread-safe and atomic.
     */
    public void reset() {
        requests.set(0);
        hits.set(0);
        misses.set(0);
        totalKeys.set(0);
        totalValues.set(0);
        evictions.set(0);

        l1Hits.set(0);
        l1Misses.set(0);
        l1Puts.set(0);
        l1Evictions.set(0);

        l2Hits.set(0);
        l2Misses.set(0);
        l2Puts.set(0);
        l2Errors.set(0);

        l3Hits.set(0);
        l3Misses.set(0);
        l3Puts.set(0);
        l3Errors.set(0);

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
     * This creates a new CacheStatisticsSnapshot object with the current values,
     * useful for reporting without affecting the original statistics.
     */
    public CacheStatisticsSnapshot getSnapshot() {
        return new CacheStatisticsSnapshot(
            getRequests(),
            getHits(),
            getMisses(),
            getKeys(),
            getValues(),
            getEvictions(),
            getL1Hits(),
            getL1Misses(),
            getL1Puts(),
            getL1Evictions(),
            getL2Hits(),
            getL2Misses(),
            getL2Puts(),
            getL2Errors(),
            getL3Hits(),
            getL3Misses(),
            getL3Puts(),
            getL3Errors(),
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
            "requests=%d, hits=%d, misses=%d, hitRate=%.2f%%, " +
            "keys=%d, values=%d, ratio=%.2f, evictions=%d, " +
            "L1[hits=%d, misses=%d, puts=%d, evictions=%d, hitRate=%.2f%%], " +
            "L2[hits=%d, misses=%d, puts=%d, errors=%d, hitRate=%.2f%%], " +
            "L3[hits=%d, misses=%d, puts=%d, errors=%d, hitRate=%.2f%%], " +
            "createdAt=%s, lastResetAt=%s}",
            getRequests(), getHits(), getMisses(), getHitRate() * 100,
            getKeys(), getValues(), getKeysToValuesRatio(), getEvictions(),
            getL1Hits(), getL1Misses(), getL1Puts(), getL1Evictions(), getL1HitRate() * 100,
            getL2Hits(), getL2Misses(), getL2Puts(), getL2Errors(), getL2HitRate() * 100,
            getL3Hits(), getL3Misses(), getL3Puts(), getL3Errors(), getL3HitRate() * 100,
            getCreatedAt(), getLastResetAt()
        );
    }

    /**
     * Returns a detailed formatted report of the cache statistics.
     */
    public String getDetailedReport() {
        return String.format("""
            Cache Statistics Report
            =======================
            Overall Performance:
            - Total Requests: %d
            - Cache Hits: %d
            - Cache Misses: %d
            - Hit Rate: %.2f%%
            - Miss Rate: %.2f%%

            Cache Content:
            - Total Keys: %d
            - Total Values: %d
            - Keys/Values Ratio: %.2f
            - Total Evictions: %d

            L1 Cache (Local - Caffeine):
            - Hits: %d
            - Misses: %d
            - Puts: %d
            - Evictions: %d
            - Hit Rate: %.2f%%
            - Requests: %d

            L2 Cache (Remote - Redis):
            - Hits: %d
            - Misses: %d
            - Puts: %d
            - Errors: %d
            - Hit Rate: %.2f%%
            - Requests: %d

            L3 Cache (Database):
            - Hits: %d
            - Misses: %d
            - Puts: %d
            - Errors: %d
            - Hit Rate: %.2f%%
            - Requests: %d

            Timing:
            - Created At: %s
            - Last Reset At: %s
            """,
            getRequests(), getHits(), getMisses(), getHitRate() * 100, getMissRate() * 100,
            getKeys(), getValues(), getKeysToValuesRatio(), getEvictions(),
            getL1Hits(), getL1Misses(), getL1Puts(), getL1Evictions(), getL1HitRate() * 100, getL1Requests(),
            getL2Hits(), getL2Misses(), getL2Puts(), getL2Errors(), getL2HitRate() * 100, getL2Requests(),
            getL3Hits(), getL3Misses(), getL3Puts(), getL3Errors(), getL3HitRate() * 100, getL3Requests(),
            getCreatedAt(), getLastResetAt()
        );
    }

    /**
     * Immutable snapshot of cache statistics at a point in time.
     */
    public static class CacheStatisticsSnapshot {
        private final long requests;
        private final long hits;
        private final long misses;
        private final long keys;
        private final long values;
        private final long evictions;
        private final long l1Hits;
        private final long l1Misses;
        private final long l1Puts;
        private final long l1Evictions;
        private final long l2Hits;
        private final long l2Misses;
        private final long l2Puts;
        private final long l2Errors;
        private final long l3Hits;
        private final long l3Misses;
        private final long l3Puts;
        private final long l3Errors;
        private final LocalDateTime createdAt;
        private final LocalDateTime lastResetAt;
        private final LocalDateTime snapshotAt;

        public CacheStatisticsSnapshot(long requests, long hits, long misses, long keys, long values,
                                     long evictions, long l1Hits, long l1Misses, long l1Puts, long l1Evictions,
                                     long l2Hits, long l2Misses, long l2Puts, long l2Errors,
                                     long l3Hits, long l3Misses, long l3Puts, long l3Errors,
                                     LocalDateTime createdAt, LocalDateTime lastResetAt) {
            this.requests = requests;
            this.hits = hits;
            this.misses = misses;
            this.keys = keys;
            this.values = values;
            this.evictions = evictions;
            this.l1Hits = l1Hits;
            this.l1Misses = l1Misses;
            this.l1Puts = l1Puts;
            this.l1Evictions = l1Evictions;
            this.l2Hits = l2Hits;
            this.l2Misses = l2Misses;
            this.l2Puts = l2Puts;
            this.l2Errors = l2Errors;
            this.l3Hits = l3Hits;
            this.l3Misses = l3Misses;
            this.l3Puts = l3Puts;
            this.l3Errors = l3Errors;
            this.createdAt = createdAt;
            this.lastResetAt = lastResetAt;
            this.snapshotAt = LocalDateTime.now();
        }

        // Getters
        public long getRequests() { return requests; }
        public long getHits() { return hits; }
        public long getHitCount() { return hits; }
        public long getMisses() { return misses; }
        public long getMissCount() { return misses; }
        public long getTotalRequests() { return hits + misses; }
        public double getHitRate() {
            long totalRequests = getTotalRequests();
            return totalRequests > 0 ? (double) hits / totalRequests : 0.0;
        }
        public double getHitRatio() { return getHitRate(); }
        public double getMissRate() { return 1.0 - getHitRate(); }
        public double getMissRatio() { return getMissRate(); }
        public long getKeys() { return keys; }
        public long getTotalKeys() { return keys; }
        public long getValues() { return values; }
        public long getTotalValues() { return values; }
        public long getEvictions() { return evictions; }
        public long getL1Hits() { return l1Hits; }
        public long getL1Misses() { return l1Misses; }
        public long getL1Puts() { return l1Puts; }
        public long getL1Evictions() { return l1Evictions; }
        public double getL1HitRate() {
            long totalL1Requests = l1Hits + l1Misses;
            return totalL1Requests > 0 ? (double) l1Hits / totalL1Requests : 0.0;
        }
        public double getL1HitRatio() { return getL1HitRate(); }
        public long getL2Hits() { return l2Hits; }
        public long getL2Misses() { return l2Misses; }
        public long getL2Puts() { return l2Puts; }
        public long getL2Errors() { return l2Errors; }
        public double getL2HitRate() {
            long totalL2Requests = l2Hits + l2Misses;
            return totalL2Requests > 0 ? (double) l2Hits / totalL2Requests : 0.0;
        }
        public double getL2HitRatio() { return getL2HitRate(); }
        public long getL3Hits() { return l3Hits; }
        public long getL3Misses() { return l3Misses; }
        public long getL3Puts() { return l3Puts; }
        public long getL3Errors() { return l3Errors; }
        public LocalDateTime getCreatedAt() { return createdAt; }
        public LocalDateTime getLastResetAt() { return lastResetAt; }
        public LocalDateTime getSnapshotAt() { return snapshotAt; }

        @Override
        public String toString() {
            return String.format(
                "CacheStatisticsSnapshot{" +
                "requests=%d, hits=%d, misses=%d, hitRate=%.2f%%, " +
                "keys=%d, values=%d, evictions=%d, " +
                "L1[hits=%d, misses=%d, puts=%d, evictions=%d, hitRate=%.2f%%], " +
                "L2[hits=%d, misses=%d, puts=%d, errors=%d, hitRate=%.2f%%], " +
                "L3[hits=%d, misses=%d, puts=%d, errors=%d], " +
                "createdAt=%s, lastResetAt=%s, snapshotAt=%s}",
                requests, hits, misses, getHitRate() * 100,
                keys, values, evictions,
                l1Hits, l1Misses, l1Puts, l1Evictions, getL1HitRate() * 100,
                l2Hits, l2Misses, l2Puts, l2Errors, getL2HitRate() * 100,
                l3Hits, l3Misses, l3Puts, l3Errors,
                createdAt, lastResetAt, snapshotAt
            );
        }
    }
}
