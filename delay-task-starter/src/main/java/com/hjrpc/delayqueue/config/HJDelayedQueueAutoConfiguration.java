package com.hjrpc.delayqueue.config;

import com.alibaba.fastjson.support.spring.GenericFastJsonRedisSerializer;
import com.hjrpc.delayqueue.DelayQueueConstants;
import com.hjrpc.delayqueue.HJDelayedQueue;
import com.hjrpc.delayqueue.HJDelayedQueueCore;
import com.hjrpc.delayqueue.listener.DistributedDelayedQueueListener;
import com.hjrpc.delayqueue.listener.RabbitMqDelayedListener;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.List;

@Configuration
public class HJDelayedQueueAutoConfiguration {
    @Bean
    public RedisTemplate<Object, Object> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<Object, Object> redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(redisConnectionFactory);
        GenericFastJsonRedisSerializer fastJsonRedisSerializer = new GenericFastJsonRedisSerializer();
        redisTemplate.setValueSerializer(fastJsonRedisSerializer);
        redisTemplate.setKeySerializer(RedisSerializer.string());
        redisTemplate.setHashKeySerializer(RedisSerializer.string());
        redisTemplate.setHashValueSerializer(fastJsonRedisSerializer);
        redisTemplate.setDefaultSerializer(fastJsonRedisSerializer);
        return redisTemplate;
    }

    // ----------延时队列配置
    @Bean
    public HJDelayedQueueCore hjDelayedQueueCore(
            @Autowired(required = false) List<DistributedDelayedQueueListener<?>> listeners
            , @Autowired(required = false) RedisTemplate<Object, Object> redisTemplate
            , @Autowired(required = false) RabbitTemplate rabbitTemplate) {
        if (listeners == null || listeners.isEmpty() || redisTemplate == null || rabbitTemplate == null) {
            throw new RuntimeException("分布式延时队列初始化失败");
        }
        return new HJDelayedQueueCore(listeners, redisTemplate, rabbitTemplate);
    }

    @Bean
    public HJDelayedQueue hjDelayedQueue(HJDelayedQueueCore hjDelayedQueueCore) {
        return new HJDelayedQueue(hjDelayedQueueCore);
    }

    // ----------MQ 配置
    @Bean
    public MessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public FanoutExchange fanoutExchanged() {
        // 交换器名称,是否持久化,是否自动删除
        return new FanoutExchange(DelayQueueConstants.FANOUT_EXCHANGE, true, false);
    }

    @Bean
    public Queue fanoutQueue() {
        return new Queue(DelayQueueConstants.FANOUT_QUEUE);
    }

    @Bean
    public Binding fanoutQueueBindingFanoutExchange(@Qualifier("fanoutQueue") Queue fanoutQueue
            , @Qualifier("fanoutExchanged") FanoutExchange fanoutExchanged) {
        return BindingBuilder.bind(fanoutQueue).to(fanoutExchanged);
    }

    @Bean
    public RabbitMqDelayedListener rabbitMqDelayedListener(HJDelayedQueueCore hjDelayedQueueCore) {
        return new RabbitMqDelayedListener(hjDelayedQueueCore);
    }
}
