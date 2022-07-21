package com.hjrpc.timedtask.config;

import com.hjrpc.timedtask.DistributedDelayedQueue;
import com.hjrpc.timedtask.DistributedDelayedQueueInit;
import com.hjrpc.timedtask.DistributedDelayedQueueListener;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RedissonClient;
import org.redisson.spring.starter.RedissonAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.List;

@Configuration
@AutoConfigureAfter({RedisAutoConfiguration.class, RedissonAutoConfiguration.class})
@RequiredArgsConstructor
public class DistributedDelayedQueueAutoConfiguration {
    private final RedissonClient redissonClient;
    private final RedisTemplate<String, Object> redisTemplate;

    @Bean
    public DistributedDelayedQueue distributedDelayedQueue() {
        return new DistributedDelayedQueue(redissonClient);
    }

    @Bean
    @ConditionalOnBean(DistributedDelayedQueue.class)
    public DistributedDelayedQueueInit distributedDelayedQueueInit(List<DistributedDelayedQueueListener<?>> listeners) {
        return new DistributedDelayedQueueInit(listeners, redissonClient, redisTemplate);
    }
}
