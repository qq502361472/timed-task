package com.hjrpc.delayqueue;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class DelayTask {

    /**
     * 任务ID，UUID
     */
    private String taskId;
    /**
     * 存储的任务
     */
    private String taskDataString;

    /**
     * 队列的名称
     */
    private String className;

    /**
     * 执行的时间戳
     */
    private long executeTime;

    /**
     * 已经重试的次数
     */
    private int retryTimes;
}
