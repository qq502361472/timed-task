package com.hjrpc.timedtask;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DistributedDelayedQueueInit {
    private final List<DistributedDelayedQueueListener<?>> distributedDelayedQueueListenerList;
    private final RedissonClient redissonClient;
    private final RedisTemplate<String, String> redisTemplate;
    private final Executor executor = Executors.newFixedThreadPool(10, r -> {
        Thread thread = new Thread(r);
        thread.setName("delay-task-");
        return thread;
    });
    private List<Thread> listThread;
    public static final String LOCK_PREFIX = "DISTRIBUTED_DELAYED_QUEUE_INIT_LOCK";
    public static final String FLAG_PREFIX = "DISTRIBUTED_DELAYED_QUEUE_INIT_FLAG";

    public DistributedDelayedQueueInit(List<DistributedDelayedQueueListener<?>> distributedDelayedQueueListenerList
            , RedissonClient redissonClient, RedisTemplate<String, String> redisTemplate) {
        this.distributedDelayedQueueListenerList = distributedDelayedQueueListenerList;
        this.redissonClient = redissonClient;
        this.redisTemplate = redisTemplate;
    }

    @PreDestroy
    public void destroy() {
        log.info("开始销毁延时队列任务监听线程...");
        listThread.forEach(Thread::interrupt);
        log.info("结束销毁延时队列任务监听线程...");
    }

    @PostConstruct
    public void initDistributedDelayedQueueInit() {
        if (CollectionUtils.isEmpty(distributedDelayedQueueListenerList)) {
            return;
        }
        log.info("===> 开始初始化分布式延时队列 ...");
        listThread = new ArrayList<>();
        // 读取所有监听器（实现DistributedDelayedQueueListener接口，且注入到spring的bean）
        for (DistributedDelayedQueueListener<?> listener : distributedDelayedQueueListenerList) {
            // 获取监听器的class名称作为一个延时队列的名称（这里获取的是子类名）
            String className = listener.getClass().getName();
            // 给不同的延时队列名创建对应的延时队列
            Thread thread = startThread(className, listener);
            listThread.add(thread);
            // 上分布式锁
            RLock lock = redissonClient.getLock(LOCK_PREFIX + ":" + className);
            if (lock.tryLock()) {
                try {
                    String listenerFlag = FLAG_PREFIX + ":" + className;
                    if (Boolean.FALSE.equals(redisTemplate.hasKey(listenerFlag))) {
                        // 初始化需要监听的数据
                        listener.init();
                        // 标记当前监听器对应初始化已执行
                        redisTemplate.opsForValue().set(listenerFlag, "已初始化", 5, TimeUnit.MINUTES);
                    }
                } finally {
                    lock.unlock();
                }
            }
        }
        log.info("===> 延时队列初始化完成 ...");
    }

    /**
     * 启动监听任务线程 * * @param queueName 队列名称 * @param listener  监听器 * @param <T>       泛型
     */
    private <T> Thread startThread(String queueName, DistributedDelayedQueueListener<T> listener) {
        //由于此线程需要常驻，可以新建线程，不用交给线程池管理
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
        thread.setName(queueName);
        thread.start();
        log.info("=====>启动监听任务{}成功...", queueName);
        return thread;
    }
}
