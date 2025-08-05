package ac.h2;

import java.util.concurrent.atomic.AtomicLong;

// CacheStatistics.java
public class CacheStatistics {
    private final AtomicLong totalKeys = new AtomicLong(0);
    private final AtomicLong totalValues = new AtomicLong(0);
    private final AtomicLong hitCount = new AtomicLong(0);
    private final AtomicLong missCount = new AtomicLong(0);

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

    @Override
    public String toString() {
        return String.format(
            "CacheStatistics{keys=%d, values=%d, ratio=%.2f, hits=%d, misses=%d, hitRate=%.2f%%}",
            getTotalKeys(), getTotalValues(), getKeysToValuesRatio(),
            getHitCount(), getMissCount(), getHitRate() * 100
        );
    }
}
