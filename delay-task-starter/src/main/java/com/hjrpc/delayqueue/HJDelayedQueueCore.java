package com.hjrpc.delayqueue;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.hjrpc.delayqueue.listener.DistributedDelayedQueueListener;
import io.netty.util.HashedWheelTimer;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class HJDelayedQueueCore implements ApplicationListener<ApplicationStartedEvent> {
    private final List<DistributedDelayedQueueListener<?>> distributedDelayedQueueListenerList;
    private Map<String, DistributedDelayedQueueListener<?>> distributedDelayedQueueListenerMap;
    private final RedisTemplate<Object, Object> redisTemplate;
    private final Map<String, HashedWheelTimer> wheelTimerMap = new ConcurrentHashMap<>();
    private final ExecutorService taskPool = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2
            , Runtime.getRuntime().availableProcessors() * 2, 0, TimeUnit.MILLISECONDS
            , new LinkedBlockingQueue<>(100000));
    private final RabbitTemplate rabbitTemplate;

    @Override
    public void onApplicationEvent(@NonNull ApplicationStartedEvent applicationStartedEvent) {
        init();
    }

    /**
     * 启动后初始化
     */
    private void init() {
        if (distributedDelayedQueueListenerList == null || distributedDelayedQueueListenerList.isEmpty()) {
            return;
        }
        // 初始化数据到redis的zset队列中
        invokeInit();
        // 刷新本地的延时任务
        refreshHeadTasks();
    }

    /**
     * 初始化延时任务的数据（如果有）
     * 推荐启动时删除所有的延时任务，重新offer所有的延时任务到延时队列。（将来会扩展批量推送）
     */
    private void invokeInit() {
        distributedDelayedQueueListenerList.forEach(listener -> {
            // 获取监听器的class名称作为一个延时队列的名称（这里获取的是子类名）,如果是走代理的话，这里获取特殊处理一下
            String className = handlerListenerClassName(listener);
            String listenerFlag = DelayQueueConstants.FLAG_PREFIX + ":" + className;
            if (Boolean.TRUE.equals(redisTemplate.opsForValue().setIfAbsent(listenerFlag, "已初始化"
                    , 5, TimeUnit.MINUTES))) {
                // 初始化需要监听的数据
                listener.init();
            }
        });
    }

    public void refreshHeadTasks() {
        distributedDelayedQueueListenerList.forEach(this::refreshHeadTask);
    }

    /**
     * 通过mq监听或启动时刷新所有的头任务，获取到任务后如果已经到期直接执行，否则推送到时间轮中
     *
     * @param listener 对应的监听器
     * @param <T>      泛型
     */
    public <T> void refreshHeadTask(DistributedDelayedQueueListener<T> listener) {
        // 获取监听器的class名称作为一个延时队列的名称（这里获取的是子类名）,如果是走代理的话，这里获取特殊处理一下
        String className = handlerListenerClassName(listener);
        List<Object> keyList = getKeyList(className);
        // 1. 从ZSET中取出时间最近的一条数据
        String script = "local expiredValues = redis.call('zrange', KEYS[1], 0, -1); " +
                "if #expiredValues > 0 then " +
                "    local taskId, taskDataString, executeTime, retryTimes = struct.unpack('Lc0Lc0li4', expiredValues[1]); " +
                "    return '{\"taskId\":'  .. taskId  .. ',\"taskDataString\":'  .. taskDataString  .. ',\"executeTime\":' " +
                "             .. executeTime  .. ',\"retryTimes\":'  .. retryTimes  .. '}'; " +
                "end ; " +
                "return nil;";
        String dataJson = redisTemplate.execute(new DefaultRedisScript<>(script, String.class)
                , RedisSerializer.string(), RedisSerializer.string(), keyList);
        if (dataJson == null || dataJson.length() == 0) {
            return;
        }
        DelayTask delayTask = JSONObject.parseObject(dataJson, new TypeReference<DelayTask>() {
        });
        delayTask.setClassName(className);
        long currentTimeMillis = System.currentTimeMillis();
        long delayTime = delayTask.getExecuteTime() - currentTimeMillis;
        if (delayTime <= 10) {
            // 尝试执行任务
            tryExecuteTask(delayTask, listener);
        } else {
            // 推送到时间轮
            pushToTimerWheel(delayTask, delayTime, className, listener);
        }
    }

    private <T> void pushToTimerWheel(DelayTask delayTask, long delayTime, String className, DistributedDelayedQueueListener<T> listener) {
        HashedWheelTimer hashedWheelTimer = new HashedWheelTimer();
        hashedWheelTimer.newTimeout(timeout -> tryExecuteTask(delayTask, listener), delayTime, TimeUnit.MILLISECONDS);
        wheelTimerMap.put(className, hashedWheelTimer);
    }

    /**
     * 尝试执行任务
     * 1.获取锁
     * 2.获取成功判断任务是否被删除
     * 3.然后返回任务开始调用listener去执行
     *
     * @param delayTask 尝试执行的任务
     * @param listener  对应的监听器
     */
    @SuppressWarnings("unchecked")
    private <T> void tryExecuteTask(DelayTask delayTask, DistributedDelayedQueueListener<T> listener) {
        String className = delayTask.getClassName();
        List<Object> keyList = getKeyList(className);
        String taskId = delayTask.getTaskId();
        String taskDataString = delayTask.getTaskDataString();
        long executeTime = delayTask.getExecuteTime();
        int retryTimes = delayTask.getRetryTimes();
        // 读取并删除任务
        // 1. 通过参数className读取ZSET中第一个任务，比对ID是否相同，如果有任务且相同读取并删除，
        // 2. 解包得到taskId/taskDataString/executeTime/retryTimes(使用struct.unpack()解包)
        // 3. 根据解包内容taskData和参数className去hash中读取对应的taskId和上一步的taskId比较
        // 4. 如果不同返回null，如果相同，删除hash中的数据，判断hash中是否还有数据，没有删除整个hash，返回解包得到的整个数据
        String script = "local expiredValues = redis.call('zrange', KEYS[1], 0, -1); " +
                "if #expiredValues > 0 then " +
                "    local taskId, taskDataString, executeTime, retryTimes = struct.unpack('Lc0Lc0li4', expiredValues[1]); " +
                "    if taskId == ARGV[1] then " +
                "        redis.call('zrem', KEYS[1], expiredValues[1]); " +
                "        local taskIdInHash = redis.call('hget', KEYS[2], taskDataString); " +
                "        if taskId == taskIdInHash then " +
                "            redis.call('hdel', KEYS[2], taskDataString); " +
                "            return '{\"taskId\":'  .. taskId  .. ',\"taskDataString\":'  .. taskDataString  .. ',\"executeTime\":' " +
                "                     .. executeTime  .. ',\"retryTimes\":'  .. retryTimes  .. '}'; " +
                "        end ; " +
                "    end; " +
                "end ; " +
                "return nil;";
        String dataJson = redisTemplate.execute(new DefaultRedisScript<>(script, String.class)
                , redisTemplate.getValueSerializer(), RedisSerializer.string(), keyList, taskId, taskDataString, executeTime
                , retryTimes, System.currentTimeMillis());

        if (dataJson == null || dataJson.length() == 0) {
            return;
        }
        DelayTask toDoDelayTask = JSONObject.parseObject(dataJson, new TypeReference<DelayTask>() {
        });
        toDoDelayTask.setClassName(className);
        // 如果处理失败可以推送回原任务
        taskPool.execute(() -> {
            try {
                Type tType = ((ParameterizedType) listener.getClass().getGenericInterfaces()[0]).getActualTypeArguments()[0];
                Object tObject = taskDataString;
                if ("java.lang.Integer".equals(tType.getTypeName())) {
                    tObject = Integer.parseInt(taskDataString);
                }

                listener.invoke((T) tObject);
            } catch (Exception e) {
                int currentRetryTimes = toDoDelayTask.getRetryTimes();
                log.error("任务执行失败,当前执行次数：" + currentRetryTimes + "。异常:", e);
                if (currentRetryTimes < 3) {
                    log.error("任务执行失败，推送回redis重新尝试执行");
                    toDoDelayTask.setRetryTimes(currentRetryTimes + 1);
                    offer(toDoDelayTask);
                }
            }

            // 发布刷新头的MQ
            publishRefreshHeadMq(className);
        });
    }

    // 推送消息到MQ
    private void publishRefreshHeadMq(String className) {
        SimpleMessageConverter simpleMessageConverter = new SimpleMessageConverter();
        Message message = simpleMessageConverter.toMessage(DelayQueueConstants.REFRESH_HEAD_FLAG + className, new MessageProperties(), String.class);
        log.info("推送刷新头任务MQ");
        rabbitTemplate.convertAndSend(DelayQueueConstants.FANOUT_EXCHANGE, null, message);
    }

    private String handlerListenerClassName(DistributedDelayedQueueListener<?> listener) {
        String name = listener.getClass().getName();
        int index = name.indexOf("$");
        if (index >= 0) {
            name = name.substring(0, index);
        }
        return name;
    }

    public void stopAndRemoveHashedTimerWheel(String className) {
        log.info("停止并删除时间轮缓存,类型：{}", className);
        HashedWheelTimer hashedWheelTimer = wheelTimerMap.get(className);
        if (hashedWheelTimer != null) {
            hashedWheelTimer.stop();
            wheelTimerMap.remove(className);
        }
    }

    /**
     * 初始化刷新所有延时队列的头信息
     *
     * @param className 对应的队列名
     */
    public void refreshHeadTask(String className) {
        if (distributedDelayedQueueListenerMap == null) {
            distributedDelayedQueueListenerMap = distributedDelayedQueueListenerList
                    .stream()
                    .collect(Collectors.toMap(this::handlerListenerClassName, Function.identity()));
        }
        DistributedDelayedQueueListener<?> distributedDelayedQueueListener = distributedDelayedQueueListenerMap.get(className);
        if (distributedDelayedQueueListener != null) {
            refreshHeadTask(distributedDelayedQueueListener);
        }
    }

    /**
     * 推送新的延时任务
     *
     * @param delayTask 演示任务数据
     */
    public void offer(DelayTask delayTask) {
        String className = delayTask.getClassName();
        String taskId = delayTask.getTaskId();
        String taskDataString = delayTask.getTaskDataString();
        long executeTime = delayTask.getExecuteTime();
        int retryTimes = delayTask.getRetryTimes();
        // 1. 推送到ZSET taskId/taskData/executeTime/retryTimes(使用struct.pack()打包)
        // 2. 推送到hash KEY：taskData VAL: taskId
        String script = "local score = tonumber(ARGV[3]);" +
                "local value = struct.pack('Lc0Lc0LI', string.len(ARGV[1]), ARGV[1], string.len(ARGV[2]), ARGV[2] " +
                ", score, tonumber(ARGV[4])); " +
                "redis.call('zadd', KEYS[1], score, value); " +
                "redis.call('hset', KEYS[2], ARGV[2], ARGV[1]);";
        redisTemplate.execute(new DefaultRedisScript<>(script)
                , getKeyList(className), taskId, taskDataString, executeTime, retryTimes);
        // 推送更新头的广播
        publishRefreshHeadMq(delayTask.getClassName());
    }

    private List<Object> getKeyList(String className) {
        String zsetKey = getKey(className, "zset");
        String hashKey = getKey(className, "hash");
        List<Object> keyList = new ArrayList<>();
        keyList.add(zsetKey);
        keyList.add(hashKey);
        return keyList;
    }

    private String getKey(String className, String concatString) {
        return DelayQueueConstants.DELAY_QUEUE_KEY_PREFIX + concatString + ":" + className;
    }

    /**
     * 删除延时任务
     *
     * @param t         要删除的任务数据
     * @param className 队列名称
     * @param <T>       泛型
     */
    public <T> void remove(T t, String className) {
        removeAll(Collections.singletonList(t), className);
    }

    /**
     * @param collections 要删除的任务数据
     * @param className   队列名称
     * @param <T>泛型
     */
    public <T> void removeAll(Collection<T> collections, String className) {
        String hashKey = getKey(className, "hash");
        redisTemplate.opsForHash().delete(hashKey, collections.toArray());
    }
}
