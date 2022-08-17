package com.hjrpc.timedtask;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;
import reactor.util.annotation.NonNull;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DistributedDelayedQueueInit implements ApplicationListener<ApplicationStartedEvent> {
    private final List<DistributedDelayedQueueListener<?>> distributedDelayedQueueListenerList;
    private final RedissonClient redissonClient;
    private final RedisTemplate<String, Object> redisTemplate;
    private final ExecutorService executor = Executors.newFixedThreadPool(10, r -> {
        Thread thread = new Thread(r);
        thread.setName("delay-base");
        return thread;
    });
    private final List<Thread> businessThread = new ArrayList<>();
    public static final String LOCK_PREFIX = "DISTRIBUTED_DELAYED_QUEUE_INIT_LOCK";
    public static final String FLAG_PREFIX = "DISTRIBUTED_DELAYED_QUEUE_INIT_FLAG";

    public DistributedDelayedQueueInit(List<DistributedDelayedQueueListener<?>> distributedDelayedQueueListenerList
            , RedissonClient redissonClient, RedisTemplate<String, Object> redisTemplate) {
        this.distributedDelayedQueueListenerList = distributedDelayedQueueListenerList;
        this.redissonClient = redissonClient;
        this.redisTemplate = redisTemplate;
    }

    @PreDestroy
    public void destroy() {
        log.info("开始销毁延时队列任务监听和业务线程...");
        executor.shutdown();
        businessThread.forEach(Thread::interrupt);
        log.info("结束销毁延时队列任务监听和业务线程...");
    }

    /**
     * 启动监听任务线程
     *
     * @param queueName 队列名称
     * @param listener  监听器
     * @param <T>       泛型
     */
    private <T> void startThread(String queueName, DistributedDelayedQueueListener<T> listener) {
        Thread thread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                RBlockingQueue<T> blockingFairQueue = redissonClient.getBlockingQueue(queueName);
                redissonClient.getDelayedQueue(blockingFairQueue);
                try {
                    T t = blockingFairQueue.take();
                    log.info("监听队列线程{},获取到值:{}", queueName, t);
                    executor.execute(() -> {
                        try {
                            listener.invoke(t);
                        } catch (Exception e) {
                            log.error("延迟队列{}发送任务异常：", queueName, e);
                        }
                    });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        thread.setName("delay-business");
        thread.start();
        // 缓存线程，方便停止的时候释放
        businessThread.add(thread);
        log.info("=====>启动监听任务{}成功...", queueName);
    }

    @Override
    public void onApplicationEvent(@NonNull ApplicationStartedEvent applicationStartedEvent) {
        if (CollectionUtils.isEmpty(distributedDelayedQueueListenerList)) {
            return;
        }
        log.info("===> 开始初始化分布式延时队列 ...");
        // 读取所有监听器（实现DistributedDelayedQueueListener接口，且注入到spring的bean）
        for (DistributedDelayedQueueListener<?> listener : distributedDelayedQueueListenerList) {
            // 获取监听器的class名称作为一个延时队列的名称（这里获取的是子类名）,如果是走代理的话，这里获取特殊处理一下
            String className = handlerListenerClassName(listener);
            // 给不同的延时队列名创建对应的延时队列
            startThread(className, listener);
            // 上分布式锁
            RLock lock = redissonClient.getLock(LOCK_PREFIX + ":" + className);
            if (lock.tryLock()) {
                try {
                    String listenerFlag = FLAG_PREFIX + ":" + className;
                    if (Boolean.TRUE.equals(redisTemplate.opsForValue().setIfAbsent(listenerFlag, "已初始化"
                            , 5, TimeUnit.MINUTES))) {
                        // 初始化需要监听的数据
                        listener.init();
                    }
                } finally {
                    lock.unlock();
                }
            }
        }
        log.info("===> 延时队列初始化完成 ...");
    }

    private String handlerListenerClassName(DistributedDelayedQueueListener<?> listener) {
        String name = listener.getClass().getName();
        int index = name.indexOf("$");
        if (index >= 0) {
            name = name.substring(0, index);
        }
        return name;
    }
}
