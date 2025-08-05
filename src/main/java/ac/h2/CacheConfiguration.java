package ac.h2;

// CacheConfiguration.java - Configuration class for cache settings
public class CacheConfiguration {
    public enum FallbackStrategy {
        REDIS_ONLY,
        DATABASE_ONLY,
        REDIS_THEN_DATABASE,
        DATABASE_THEN_REDIS
    }
    
    private final FallbackStrategy globalFallbackStrategy;
    private final long localCacheTtlMillis;
    private final long maxLocalCacheSize;
    private final long remoteCacheTtlMillis;
    private final boolean enableLocalCache;
    private final boolean enableWriteThrough;
    private final boolean enableReadThrough;
    
    public static class Builder {
        private FallbackStrategy globalFallbackStrategy = FallbackStrategy.REDIS_THEN_DATABASE;
        private long localCacheTtlMillis = 300000L; // 5 minutes
        private long maxLocalCacheSize = 10000L;
        private long remoteCacheTtlMillis = 3600000L; // 1 hour
        private boolean enableLocalCache = true;
        private boolean enableWriteThrough = true;
        private boolean enableReadThrough = true;
        
        public Builder fallbackStrategy(FallbackStrategy strategy) {
            this.globalFallbackStrategy = strategy;
            return this;
        }
        
        public Builder localCacheTtl(long ttlMillis) {
            this.localCacheTtlMillis = ttlMillis;
            return this;
        }
        
        public Builder maxLocalCacheSize(long size) {
            this.maxLocalCacheSize = size;
            return this;
        }
        
        public Builder remoteCacheTtl(long ttlMillis) {
            this.remoteCacheTtlMillis = ttlMillis;
            return this;
        }
        
        public Builder enableLocalCache(boolean enable) {
            this.enableLocalCache = enable;
            return this;
        }
        
        public Builder enableWriteThrough(boolean enable) {
            this.enableWriteThrough = enable;
            return this;
        }
        
        public Builder enableReadThrough(boolean enable) {
            this.enableReadThrough = enable;
            return this;
        }
        
        public CacheConfiguration build() {
            return new CacheConfiguration(this);
        }
    }
    
    private CacheConfiguration(Builder builder) {
        this.globalFallbackStrategy = builder.globalFallbackStrategy;
        this.localCacheTtlMillis = builder.localCacheTtlMillis;
        this.maxLocalCacheSize = builder.maxLocalCacheSize;
        this.remoteCacheTtlMillis = builder.remoteCacheTtlMillis;
        this.enableLocalCache = builder.enableLocalCache;
        this.enableWriteThrough = builder.enableWriteThrough;
        this.enableReadThrough = builder.enableReadThrough;
    }
    
    // Getters
    public FallbackStrategy getGlobalFallbackStrategy() { return globalFallbackStrategy; }
    public long getLocalCacheTtlMillis() { return localCacheTtlMillis; }
    public long getMaxLocalCacheSize() { return maxLocalCacheSize; }
    public long getRemoteCacheTtlMillis() { return remoteCacheTtlMillis; }
    public boolean isLocalCacheEnabled() { return enableLocalCache; }
    public boolean isWriteThroughEnabled() { return enableWriteThrough; }
    public boolean isReadThroughEnabled() { return enableReadThrough; }
    
    public static Builder builder() {
        return new Builder();
    }
}
