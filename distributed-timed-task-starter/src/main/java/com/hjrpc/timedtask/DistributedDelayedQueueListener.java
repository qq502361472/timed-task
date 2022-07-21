package com.hjrpc.timedtask;

/**
 * 分布式演示队列任务监听接口
 * 底层已经使用异步执行
 * @param <T>
 */
public interface DistributedDelayedQueueListener<T> {
    /**
     * 重启后初始化待监听的数据到分布式延时队列，需要支持幂等
     */
    default void init(){};

    /**
     * 监听到消息执行调用的方法     * @param t 泛型参数
     */
    void invoke(T t);
}