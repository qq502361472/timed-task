package com.hjrpc.delayqueue.listener;

import com.hjrpc.delayqueue.DelayQueueConstants;
import com.hjrpc.delayqueue.HJDelayedQueueCore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;

@Slf4j
@RequiredArgsConstructor
public class RabbitMqDelayedListener {
    private final HJDelayedQueueCore hjDelayedQueue;

    @RabbitListener(queues = DelayQueueConstants.FANOUT_QUEUE)
    public void listenerMessage(String message) {
        log.info("监听到MQ广播消息：{}", message);
        if (message.startsWith(DelayQueueConstants.REFRESH_HEAD_FLAG)) {
            String className = message.substring(DelayQueueConstants.REFRESH_HEAD_FLAG.length());
            // 停止且删除时间轮
            hjDelayedQueue.stopAndRemoveHashedTimerWheel(className);
            // 刷新头
            hjDelayedQueue.refreshHeadTask(className);
        }
    }
}
