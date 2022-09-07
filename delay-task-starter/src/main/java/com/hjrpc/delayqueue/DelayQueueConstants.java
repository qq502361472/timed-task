package com.hjrpc.delayqueue;

public interface DelayQueueConstants {
    /**
     * MQ的交换器
     */
    String FANOUT_EXCHANGE = "delay-fanout";
    /**
     * MQ的队列
     */
    String FANOUT_QUEUE = "delay-queue2";
    /**
     * MQ推送刷新头前缀
     */
    String REFRESH_HEAD_FLAG = "refreshHead:";
    /**
     * 延时任务初始化标记前缀
     */
    String FLAG_PREFIX = "delay:initialized:";
    /**
     * 延时任务Redis的Key前缀
     */
    String DELAY_QUEUE_KEY_PREFIX = "delay:task:";
}
