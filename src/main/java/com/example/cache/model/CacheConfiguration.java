package com.example.cache.model;

import java.time.Duration;

public class CacheConfiguration {
    private final long maxLocalCacheSize;
    private final long localCacheTtlMillis;
    private final long remoteCacheTtlMillis;
    private final boolean localCacheEnabled;
    private final boolean remoteCacheEnabled;
    private final boolean writeThroughEnabled;
    private final boolean readThroughEnabled;

    public static class Builder {
        private long maxLocalCacheSize = 10000;
        private long localCacheTtlMillis = Duration.ofMinutes(10).toMillis();
        private long remoteCacheTtlMillis = Duration.ofHours(1).toMillis();
        private boolean localCacheEnabled = true;
        private boolean remoteCacheEnabled = true;
        private boolean writeThroughEnabled = true;
        private boolean readThroughEnabled = true;

        public Builder maxLocalCacheSize(long maxLocalCacheSize) {
            this.maxLocalCacheSize = maxLocalCacheSize;
            return this;
        }

        public Builder localCacheTtl(Duration ttl) {
            this.localCacheTtlMillis = ttl.toMillis();
            return this;
        }

        public Builder remoteCacheTtl(Duration ttl) {
            this.remoteCacheTtlMillis = ttl.toMillis();
            return this;
        }

        public Builder localCacheEnabled(boolean enabled) {
            this.localCacheEnabled = enabled;
            return this;
        }

        public Builder remoteCacheEnabled(boolean enabled) {
            this.remoteCacheEnabled = enabled;
            return this;
        }

        public Builder writeThroughEnabled(boolean enabled) {
            this.writeThroughEnabled = enabled;
            return this;
        }

        public Builder readThroughEnabled(boolean enabled) {
            this.readThroughEnabled = enabled;
            return this;
        }

        public CacheConfiguration build() {
            return new CacheConfiguration(this);
        }
    }

    private CacheConfiguration(Builder builder) {
        this.maxLocalCacheSize = builder.maxLocalCacheSize;
        this.localCacheTtlMillis = builder.localCacheTtlMillis;
        this.remoteCacheTtlMillis = builder.remoteCacheTtlMillis;
        this.localCacheEnabled = builder.localCacheEnabled;
        this.remoteCacheEnabled = builder.remoteCacheEnabled;
        this.writeThroughEnabled = builder.writeThroughEnabled;
        this.readThroughEnabled = builder.readThroughEnabled;
    }

    public static Builder builder() {
        return new Builder();
    }

    // Getters
    public long getMaxLocalCacheSize() { return maxLocalCacheSize; }
    public long getLocalCacheTtlMillis() { return localCacheTtlMillis; }
    public long getRemoteCacheTtlMillis() { return remoteCacheTtlMillis; }
    public boolean isLocalCacheEnabled() { return localCacheEnabled; }
    public boolean isRemoteCacheEnabled() { return remoteCacheEnabled; }
    public boolean isWriteThroughEnabled() { return writeThroughEnabled; }
    public boolean isReadThroughEnabled() { return readThroughEnabled; }
}
