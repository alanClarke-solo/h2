// HierarchicalCacheConfiguration.java
package com.example.cache.config;

import com.example.cache.service.HierarchicalCacheManager;
import com.example.cache.service.HierarchicalCacheService;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.concurrent.TimeUnit;

@Configuration
@EnableCaching
@ConditionalOnClass({Caffeine.class, RedissonClient.class})
@EnableConfigurationProperties(HierarchicalCacheProperties.class)
public class HierarchicalCacheConfiguration {

    @Bean
    @Primary
    public RedissonClient redissonClient(HierarchicalCacheProperties properties) {
        Config config = new Config();
        config.useSingleServer()
                .setAddress(properties.getRedis().getAddress())
                .setConnectionPoolSize(properties.getRedis().getConnectionPoolSize())
                .setConnectionMinimumIdleSize(properties.getRedis().getConnectionMinimumIdleSize())
                .setTimeout(properties.getRedis().getTimeout());
        
        if (properties.getRedis().getPassword() != null) {
            config.useSingleServer().setPassword(properties.getRedis().getPassword());
        }
        
        return Redisson.create(config);
    }

    @Bean
    @Primary
    public HierarchicalCacheService hierarchicalCacheService(
            RedissonClient redissonClient, 
            HierarchicalCacheProperties properties) {
        return new HierarchicalCacheService(redissonClient, properties);
    }

    @Bean
    @Primary
    public CacheManager cacheManager(HierarchicalCacheService cacheService) {
        return new HierarchicalCacheManager(cacheService);
    }
}
