package com.h2.spring.cache.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.h2.spring.cache.service.HierarchicalCacheManager;
import com.h2.spring.cache.service.NearNFarHierarchicalCacheService;
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
    public NearNFarHierarchicalCacheService hierarchicalCacheService(
            RedissonClient redissonClient, 
            HierarchicalCacheProperties properties) {
        return new NearNFarHierarchicalCacheService(redissonClient, properties);
    }

    @Bean
    @Primary
    public CacheManager cacheManager(NearNFarHierarchicalCacheService cacheService) {
        return new HierarchicalCacheManager(cacheService);
    }
}
