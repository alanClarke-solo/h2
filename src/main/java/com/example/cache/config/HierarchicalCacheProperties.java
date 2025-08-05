// HierarchicalCacheProperties.java
package com.example.cache.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ConfigurationProperties(prefix = "hierarchical.cache")
public class HierarchicalCacheProperties {

    @NestedConfigurationProperty
    private CaffeineProperties caffeine = new CaffeineProperties();

    @NestedConfigurationProperty
    private RedisProperties redis = new RedisProperties();

    @NestedConfigurationProperty
    private Map<String, CacheLevel> levels = new ConcurrentHashMap<>();

    // Getters and setters
    public CaffeineProperties getCaffeine() { return caffeine; }
    public void setCaffeine(CaffeineProperties caffeine) { this.caffeine = caffeine; }

    public RedisProperties getRedis() { return redis; }
    public void setRedis(RedisProperties redis) { this.redis = redis; }

    public Map<String, CacheLevel> getLevels() { return levels; }
    public void setLevels(Map<String, CacheLevel> levels) { this.levels = levels; }

    public static class CaffeineProperties {
        private long maximumSize = 10000;
        private Duration expireAfterWrite = Duration.ofMinutes(10);
        private Duration expireAfterAccess = Duration.ofMinutes(5);
        private boolean recordStats = true;

        // Getters and setters
        public long getMaximumSize() { return maximumSize; }
        public void setMaximumSize(long maximumSize) { this.maximumSize = maximumSize; }

        public Duration getExpireAfterWrite() { return expireAfterWrite; }
        public void setExpireAfterWrite(Duration expireAfterWrite) { this.expireAfterWrite = expireAfterWrite; }

        public Duration getExpireAfterAccess() { return expireAfterAccess; }
        public void setExpireAfterAccess(Duration expireAfterAccess) { this.expireAfterAccess = expireAfterAccess; }

        public boolean isRecordStats() { return recordStats; }
        public void setRecordStats(boolean recordStats) { this.recordStats = recordStats; }
    }

    public static class RedisProperties {
        private String address = "redis://localhost:6379";
        private String password;
        private int connectionPoolSize = 64;
        private int connectionMinimumIdleSize = 10;
        private int timeout = 3000;
        private Duration defaultTtl = Duration.ofHours(1);

        // Getters and setters
        public String getAddress() { return address; }
        public void setAddress(String address) { this.address = address; }

        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }

        public int getConnectionPoolSize() { return connectionPoolSize; }
        public void setConnectionPoolSize(int connectionPoolSize) { this.connectionPoolSize = connectionPoolSize; }

        public int getConnectionMinimumIdleSize() { return connectionMinimumIdleSize; }
        public void setConnectionMinimumIdleSize(int connectionMinimumIdleSize) { this.connectionMinimumIdleSize = connectionMinimumIdleSize; }

        public int getTimeout() { return timeout; }
        public void setTimeout(int timeout) { this.timeout = timeout; }

        public Duration getDefaultTtl() { return defaultTtl; }
        public void setDefaultTtl(Duration defaultTtl) { this.defaultTtl = defaultTtl; }
    }

    public static class CacheLevel {
        private String name;
        private long maximumSize = 1000;
        private Duration ttl = Duration.ofMinutes(30);
        private boolean enableL1 = true;
        private boolean enableL2 = true;

        // Getters and setters
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public long getMaximumSize() { return maximumSize; }
        public void setMaximumSize(long maximumSize) { this.maximumSize = maximumSize; }

        public Duration getTtl() { return ttl; }
        public void setTtl(Duration ttl) { this.ttl = ttl; }

        public boolean isEnableL1() { return enableL1; }
        public void setEnableL1(boolean enableL1) { this.enableL1 = enableL1; }

        public boolean isEnableL2() { return enableL2; }
        public void setEnableL2(boolean enableL2) { this.enableL2 = enableL2; }
    }
}
