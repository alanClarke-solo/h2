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
    private final boolean enableRemoteCache;
    private final boolean enableWriteThrough;
    private final boolean enableReadThrough;
    private final boolean databaseCacheEnabled;
    private final String databaseJdbcUrl;
    private final String databaseUsername;
    private final String databasePassword;
    private final long databaseCacheTtlMillis;

    public static class Builder {
        private FallbackStrategy globalFallbackStrategy = FallbackStrategy.REDIS_THEN_DATABASE;
        private long localCacheTtlMillis = 300000L; // 5 minutes
        private long maxLocalCacheSize = 10000L;
        private long remoteCacheTtlMillis = 3600000L; // 1 hour
        private boolean enableLocalCache = true;
        private boolean enableRemoteCache = true;
        private boolean enableWriteThrough = true;
        private boolean enableReadThrough = true;
        private boolean databaseCacheEnabled = false;
        private String databaseJdbcUrl;
        private String databaseUsername;
        private String databasePassword;
        private long databaseCacheTtlMillis = 3600000L; // 1 hour

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

        public Builder enableRemoteCache(boolean enable) {
            this.enableRemoteCache = enable;
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

        public Builder enableDatabaseCache(boolean enable) {
            this.databaseCacheEnabled = enable;
            return this;
        }

        public Builder databaseJdbcUrl(String url) {
            this.databaseJdbcUrl = url;
            return this;
        }

        public Builder databaseUsername(String username) {
            this.databaseUsername = username;
            return this;
        }

        public Builder databasePassword(String password) {
            this.databasePassword = password;
            return this;
        }

        public Builder databaseCacheTtl(long ttlMillis) {
            this.databaseCacheTtlMillis = ttlMillis;
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
        this.enableRemoteCache = builder.enableRemoteCache;
        this.enableWriteThrough = builder.enableWriteThrough;
        this.enableReadThrough = builder.enableReadThrough;
        this.databaseCacheEnabled = builder.databaseCacheEnabled;
        this.databaseJdbcUrl = builder.databaseJdbcUrl;
        this.databaseUsername = builder.databaseUsername;
        this.databasePassword = builder.databasePassword;
        this.databaseCacheTtlMillis = builder.databaseCacheTtlMillis;
    }

    // Getters
    public FallbackStrategy getGlobalFallbackStrategy() { return globalFallbackStrategy; }
    public long getLocalCacheTtlMillis() { return localCacheTtlMillis; }
    public long getMaxLocalCacheSize() { return maxLocalCacheSize; }
    public long getRemoteCacheTtlMillis() { return remoteCacheTtlMillis; }
    public boolean isLocalCacheEnabled() { return enableLocalCache; }
    public boolean isRemoteCacheEnabled() { return enableRemoteCache; }
    public boolean isWriteThroughEnabled() { return enableWriteThrough; }
    public boolean isReadThroughEnabled() { return enableReadThrough; }
    public boolean isDatabaseCacheEnabled() { return databaseCacheEnabled; }
    public String getDatabaseJdbcUrl() { return databaseJdbcUrl; }
    public String getDatabaseUsername() { return databaseUsername; }
    public String getDatabasePassword() { return databasePassword; }
    public long getDatabaseCacheTtlMillis() { return databaseCacheTtlMillis; }

    public static Builder builder() {
        return new Builder();
    }
}