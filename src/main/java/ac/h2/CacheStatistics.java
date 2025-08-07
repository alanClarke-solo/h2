package ac.h2;

import java.util.concurrent.atomic.AtomicLong;

// CacheStatistics.java
public class CacheStatistics {
    private final AtomicLong totalKeys = new AtomicLong(0);
    private final AtomicLong totalValues = new AtomicLong(0);
    private final AtomicLong hitCount = new AtomicLong(0);
    private final AtomicLong missCount = new AtomicLong(0);

    // L1 (Local cache) statistics
    private final AtomicLong l1Hits = new AtomicLong(0);
    private final AtomicLong l1Misses = new AtomicLong(0);
    private final AtomicLong l1Puts = new AtomicLong(0);
    private final AtomicLong l1Evictions = new AtomicLong(0);

    // L2 (Redis) statistics
    private final AtomicLong l2Hits = new AtomicLong(0);
    private final AtomicLong l2Misses = new AtomicLong(0);
    private final AtomicLong l2Puts = new AtomicLong(0);
    private final AtomicLong l2Errors = new AtomicLong(0);

    // L3 (Database) statistics
    private final AtomicLong l3Hits = new AtomicLong(0);
    private final AtomicLong l3Misses = new AtomicLong(0);
    private final AtomicLong l3Puts = new AtomicLong(0);
    private final AtomicLong l3Errors = new AtomicLong(0);

    // L1 methods
    public void incrementL1Hits() { l1Hits.incrementAndGet(); incrementHits(); }
    public void incrementL1Misses() { l1Misses.incrementAndGet(); incrementMisses();}
    public void incrementL1Puts() { l1Puts.incrementAndGet(); }
    public void incrementL1Evictions() { l1Evictions.incrementAndGet(); }

    // L2 methods
    public void incrementL2Hits() { l2Hits.incrementAndGet(); incrementHits(); }
    public void incrementL2Misses() { l2Misses.incrementAndGet(); incrementMisses();}
    public void incrementL2Puts() { l2Puts.incrementAndGet(); }
    public void incrementL2Errors() { l2Errors.incrementAndGet(); }

    // L3 methods
    public void incrementL3Hits() { l3Hits.incrementAndGet(); incrementHits(); }
    public void incrementL3Misses() { l3Misses.incrementAndGet(); incrementMisses();}
    public void incrementL3Puts() { l3Puts.incrementAndGet(); }
    public void incrementL3Errors() { l3Errors.incrementAndGet(); }

    // L1 getters
    public long getL1Hits() { return l1Hits.get(); }
    public long getL1Misses() { return l1Misses.get(); }
    public long getL1Puts() { return l1Puts.get(); }
    public long getL1Evictions() { return l1Evictions.get(); }

    // L2 getters
    public long getL2Hits() { return l2Hits.get(); }
    public long getL2Misses() { return l2Misses.get(); }
    public long getL2Puts() { return l2Puts.get(); }
    public long getL2Errors() { return l2Errors.get(); }

    // L3 getters
    public long getL3Hits() { return l3Hits.get(); }
    public long getL3Misses() { return l3Misses.get(); }
    public long getL3Puts() { return l3Puts.get(); }
    public long getL3Errors() { return l3Errors.get(); }

    public void incrementKeys() {
        totalKeys.incrementAndGet();
    }

    public void decrementKeys() {
        if (totalKeys.get() > 0)
            totalKeys.decrementAndGet();
    }

    public void incrementValues() {
        totalValues.incrementAndGet();
    }

    public void decrementValues() {
        if(totalValues.get() > 0)
            totalValues.decrementAndGet();
    }

    public void incrementHits() {
        hitCount.incrementAndGet();
    }

    public void incrementMisses() {
        missCount.incrementAndGet();
        // Increment miss count for all levels when there's a complete miss
//        l1Misses.incrementAndGet();
//        l2Misses.incrementAndGet();
//        l3Misses.incrementAndGet();
    }

    public long getTotalKeys() {
        return totalKeys.get();
    }

    public long getTotalValues() {
        return totalValues.get();
    }

    public long getHitCount() {
        return hitCount.get();
    }

    public long getMissCount() {
        return missCount.get();
    }

    public double getKeysToValuesRatio() {
        long values = totalValues.get();
        return values == 0 ? 0.0 : (double) totalKeys.get() / values;
    }

    public double getHitRate() {
        long total = hitCount.get() + missCount.get();
        return total == 0 ? 0.0 : (double) hitCount.get() / total;
    }

    public double getL1HitRate() {
        long total = l1Hits.get() + l1Misses.get();
        return total == 0 ? 0.0 : (double) l1Hits.get() / total;
    }

    public double getL2HitRate() {
        long total = l2Hits.get() + l2Misses.get();
        return total == 0 ? 0.0 : (double) l2Hits.get() / total;
    }

    public double getL3HitRate() {
        long total = l3Hits.get() + l3Misses.get();
        return total == 0 ? 0.0 : (double) l3Hits.get() / total;
    }

    @Override
    public String toString() {
        return String.format(
                "CacheStatistics{keys=%d, values=%d, ratio=%.2f, hits=%d, misses=%d, hitRate=%.2f%%, " +
                        "L1(hits=%d, misses=%d, hitRate=%.2f%%), " +
                        "L2(hits=%d, misses=%d, hitRate=%.2f%%), " +
                        "L3(hits=%d, misses=%d, hitRate=%.2f%%)}",
                getTotalKeys(), getTotalValues(), getKeysToValuesRatio(),
                getHitCount(), getMissCount(), getHitRate() * 100,
                getL1Hits(), getL1Misses(), getL1HitRate() * 100,
                getL2Hits(), getL2Misses(), getL2HitRate() * 100,
                getL3Hits(), getL3Misses(), getL3HitRate() * 100
        );
    }

    public void reset() {
        totalKeys.set(0);
        totalValues.set(0);
        hitCount.set(0);
        missCount.set(0);
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
    }
}