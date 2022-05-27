package com.hjrpc.demo.controller;

import com.hjrpc.demo.dto.TaskDTO;
import com.hjrpc.demo.listener.MyListener;
import com.hjrpc.timedtask.DistributedDelayedQueue;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.Date;

@RestController
@Slf4j
@Api("测试定时任务")
public class TestController {
    @Autowired
    private DistributedDelayedQueue distributedDelayedQueue;


    @PostMapping("/pushTask")
    @ApiOperation("推送定时任务")
    public void pushTask(@RequestBody TaskDTO taskDTO) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date destDate = taskDTO.getDestDate();
        log.info("推送定时任务{}，在{}执行", taskDTO.getTaskNum(), format.format(destDate));
        distributedDelayedQueue.addDelayedTask(taskDTO.getTaskNum(), destDate, MyListener.class);
    }
}
