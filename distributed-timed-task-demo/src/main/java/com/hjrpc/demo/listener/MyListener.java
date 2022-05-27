package com.hjrpc.demo.listener;

import com.hjrpc.timedtask.DistributedDelayedQueue;
import com.hjrpc.timedtask.DistributedDelayedQueueListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

@Slf4j
@Component
public class MyListener implements DistributedDelayedQueueListener<Integer> {
    @Autowired
    private DistributedDelayedQueue distributedDelayedQueue;

    @Override
    public void init() {
        // 项目启动时初始化历史数据到延时队列
        log.info("项目启动时初始化历史数据");
        Set<Integer> todoTaskIds = new HashSet<>();
        distributedDelayedQueue.removeAllDelayedTask(todoTaskIds, this.getClass());
    }

    @Override
    public void invoke(Integer integer) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        log.info("{}监听到定时任务，{}", format.format(new Date()), integer);
    }
}
