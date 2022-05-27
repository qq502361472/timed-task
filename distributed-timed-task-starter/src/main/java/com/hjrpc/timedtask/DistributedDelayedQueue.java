package com.hjrpc.timedtask;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RedissonClient;

import java.util.Collection;
import java.util.Date;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DistributedDelayedQueue {
    private final RedissonClient redissonClient;

    public DistributedDelayedQueue(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    /**
     * 添加延时队列任务
     *
     * @param t         演示队列DTO对象
     * @param delay     延时时间
     * @param timeUnit  延时时间的单位
     * @param queueName 队列名称
     * @param <T>       泛型
     */
    public <T> void addDelayedTask(T t, long delay, TimeUnit timeUnit, String queueName) {
        if (log.isDebugEnabled()) {
            log.debug("添加队列:{},delay:{},timeUnit:{},data:{}", queueName, delay, timeUnit, t);
        }
        // 兼容延迟时间负数问题
        if (delay < 0) {
            delay = 0;
        }
        // 默认为公平队列
        RBlockingQueue<T> blockingFairQueue = redissonClient.getBlockingQueue(queueName);
        RDelayedQueue<T> delayedQueue = redissonClient.getDelayedQueue(blockingFairQueue);
        delayedQueue.offer(t, delay, timeUnit);
    }

    /**
     * @param t        演示队列DTO对象
     * @param delay    延时时间
     * @param timeUnit 延时时间的单位
     * @param clazz    取对应监听器的class
     * @param <T>      泛型
     */
    public <T> void addDelayedTask(T t, long delay, TimeUnit timeUnit, Class<?> clazz) {
        addDelayedTask(t, delay, timeUnit, clazz.getName());
    }

    /**
     * @param t     演示队列DTO对象
     * @param dest  指定延迟到的日期
     * @param clazz 取对应监听器的class
     * @param <T>   泛型
     */
    public <T> void addDelayedTask(T t, Date dest, Class<?> clazz) {
        long delay = dest.getTime() - System.currentTimeMillis();
        addDelayedTask(t, delay, TimeUnit.MILLISECONDS, clazz);
    }

    /**
     * 删除延时任务，注意对象必须是同一个，redis采用默认序列化吗，没有采用json序列化。
     * 在业务量不是很大时，推荐在任务弹出时判断任务状态是否发生变化
     *
     * @param t     演示队列DTO对象
     * @param clazz 取对应监听器的class
     * @param <T>   泛型
     */
    public <T> void removeDelayedTask(T t, Class<?> clazz) {
        if (log.isDebugEnabled()) {
            log.debug("移除队列:{},data:{}", clazz.getName(), t);
        }
        // 默认为公平队列
        RBlockingQueue<T> blockingFairQueue = redissonClient.getBlockingQueue(clazz.getName());
        RDelayedQueue<T> delayedQueue = redissonClient.getDelayedQueue(blockingFairQueue);
        try {
            delayedQueue.remove(t);
        } catch (Exception e) {
            log.error("删除任务[{}]异常:", t, e);
        }
    }

    /**
     * 删除延时任务，注意对象必须是同一个，redis采用默认序列化吗，没有采用json序列化。
     * * 在业务量不是很大时，推荐在任务弹出时判断任务状态是否发生变化 * *
     *
     * @param collections 被批量删除的对象集合
     * @param clazz       取对应监听器的class
     * @param <T>         泛型
     */
    public <T> void removeAllDelayedTask(Collection<T> collections, Class<?> clazz) {
        if (log.isDebugEnabled()) {
            log.debug("移除队列:{},data:{}", clazz.getName(), collections);
        }
        // 默认为公平队列
        RBlockingQueue<T> blockingFairQueue = redissonClient.getBlockingQueue(clazz.getName());
        RDelayedQueue<T> delayedQueue = redissonClient.getDelayedQueue(blockingFairQueue);
        try {
            delayedQueue.removeAll(collections);
        } catch (Exception e) {
            log.error("删除任务[{}]异常:", collections, e);
        }
    }
}
