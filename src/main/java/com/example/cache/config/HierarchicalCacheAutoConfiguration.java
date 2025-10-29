package com.example.cache.config;

import ac.h2.HierarchicalCacheService;
import com.example.cache.model.CacheConfiguration;
import com.example.cache.service.TransparentHierarchicalCacheService;
import com.h2.spring.cache.config.HierarchicalCacheProperties;
import com.h2.spring.cache.service.HierarchicalCacheManager;
import com.h2.spring.cache.service.NearNFarHierarchicalCacheService;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@EnableConfigurationProperties(HierarchicalCacheProperties.class)
public class HierarchicalCacheAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public RedissonClient redissonClient(HierarchicalCacheProperties properties) {
        Config config = new Config();
        config.useSingleServer()
                .setAddress(properties.getRedis().getAddress())
                .setPassword(properties.getRedis().getPassword())
                .setConnectionPoolSize(properties.getRedis().getConnectionPoolSize())
                .setConnectionMinimumIdleSize(properties.getRedis().getConnectionMinimumIdleSize())
                .setTimeout(properties.getRedis().getTimeout());
        
        return Redisson.create(config);
    }

    @Bean
    @ConditionalOnMissingBean
    public NearNFarHierarchicalCacheService hierarchicalCacheService(
            RedissonClient redissonClient,
            HierarchicalCacheProperties properties) {
        return new NearNFarHierarchicalCacheService(redissonClient, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public CacheConfiguration cacheConfiguration(HierarchicalCacheProperties properties) {
        return CacheConfiguration.builder()
                .maxLocalCacheSize(properties.getCaffeine().getMaximumSize())
                .localCacheTtl(properties.getCaffeine().getExpireAfterWrite())
                .remoteCacheTtl(properties.getRedis().getDefaultTtl())
                .localCacheEnabled(true)
                .remoteCacheEnabled(true)
                .writeThroughEnabled(true)
                .readThroughEnabled(true)
                .build();
    }

    @Bean
    @ConditionalOnMissingBean
    public TransparentHierarchicalCacheService<?> transparentHierarchicalCacheService(
            HierarchicalCacheService hierarchicalCacheService,
            CacheConfiguration cacheConfiguration) {
        return new TransparentHierarchicalCacheService<>(hierarchicalCacheService, cacheConfiguration);
    }

    @Bean
    @Primary
    public CacheManager cacheManager(NearNFarHierarchicalCacheService nearNFarHierarchicalCacheService) {
        return new HierarchicalCacheManager(nearNFarHierarchicalCacheService);
    }
}
