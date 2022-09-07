package com.hjrpc.demo.listener;


import com.hjrpc.delayqueue.listener.DistributedDelayedQueueListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;

@Slf4j
@Component
public class MyListener implements DistributedDelayedQueueListener<Integer> {

    @Override
    public void init() {
        // 项目启动时初始化历史数据到延时队列
        log.info("项目启动时初始化历史数据");
    }

    @Override
    public void invoke(Integer integer) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        log.info("{}监听到定时任务，{}", format.format(new Date()), integer);
    }
}
