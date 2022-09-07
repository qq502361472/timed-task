package com.hjrpc.delayqueue;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
@Slf4j
public class HJDelayedQueue {
    private final HJDelayedQueueCore hjDelayedQueueCore;

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
        try {
            hjDelayedQueueCore.remove(t, clazz.getName());
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

        try {
            hjDelayedQueueCore.removeAll(collections, clazz.getName());
        } catch (Exception e) {
            log.error("删除任务[{}]异常:", collections, e);
        }
    }

    public <T> void pushTask(T t, long delayTime, TimeUnit timeUnit, Class<?> clazz) {
        long l = timeUnit.toMillis(delayTime);
        pushTask(t, System.currentTimeMillis() + l, clazz);
    }

    public <T> void pushTask(T t, Date executeDate, Class<?> clazz) {
        pushTask(t, executeDate.getTime(), clazz);
    }

    private <T> void pushTask(T t, long executeTime, Class<?> clazz) {
        DelayTask delayTask = new DelayTask()
                .setTaskId(UUID.randomUUID().toString().replace("-", ""))
                .setClassName(clazz.getName())
                .setTaskDataString(t.toString())
                .setExecuteTime(executeTime)
                .setRetryTimes(0);
        hjDelayedQueueCore.offer(delayTask);
    }

}