package ac.h2;

public class CacheContext {
    private static final ThreadLocal<CacheContext> CONTEXT = new ThreadLocal<>();
    
    private final CacheConfiguration.FallbackStrategy fallbackStrategy;
    private final boolean forceRefresh;
    private final boolean skipLocalCache;
    private final Long customTtl;
    
    private CacheContext(Builder builder) {
        this.fallbackStrategy = builder.fallbackStrategy;
        this.forceRefresh = builder.forceRefresh;
        this.skipLocalCache = builder.skipLocalCache;
        this.customTtl = builder.customTtl;
    }
    
    public static class Builder {
        private CacheConfiguration.FallbackStrategy fallbackStrategy;
        private boolean forceRefresh = false;
        private boolean skipLocalCache = false;
        private Long customTtl;
        
        public Builder fallbackStrategy(CacheConfiguration.FallbackStrategy strategy) {
            this.fallbackStrategy = strategy;
            return this;
        }
        
        public Builder forceRefresh(boolean force) {
            this.forceRefresh = force;
            return this;
        }
        
        public Builder skipLocalCache(boolean skip) {
            this.skipLocalCache = skip;
            return this;
        }
        
        public Builder customTtl(long ttl) {
            this.customTtl = ttl;
            return this;
        }
        
        public CacheContext build() {
            return new CacheContext(this);
        }
    }
    
    public static void set(CacheContext context) {
        CONTEXT.set(context);
    }
    
    public static CacheContext get() {
        return CONTEXT.get();
    }
    
    public static void clear() {
        CONTEXT.remove();
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    // Getters
    public CacheConfiguration.FallbackStrategy getFallbackStrategy() { return fallbackStrategy; }
    public boolean isForceRefresh() { return forceRefresh; }
    public boolean isSkipLocalCache() { return skipLocalCache; }
    public Long getCustomTtl() { return customTtl; }
}
