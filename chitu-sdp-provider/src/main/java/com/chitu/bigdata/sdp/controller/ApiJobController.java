package com.chitu.bigdata.sdp.controller;

import com.alibaba.fastjson.JSONObject;
import com.chitu.bigdata.sdp.api.bo.ApiJobBO;
import com.chitu.bigdata.sdp.service.ApiJobServiceImpl;
import com.chitu.cloud.model.ResponseData;
import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author chenyun
 * @description: TODO
 * @date 2022/2/24 11:34
 */
@Slf4j
@RestController
@Api(tags="对外接口")
@RequestMapping(value = "/api/job")
public class ApiJobController {

    @Autowired
    private ApiJobServiceImpl apiJobService;

    @PostMapping(value = "/saveJob")
    public ResponseData saveJob(@RequestBody ApiJobBO apiJob) {
        return apiJobService.saveJob(apiJob);
    }

    @PostMapping(value = "/startJob")
    public ResponseData startJob(@RequestBody ApiJobBO apiJob) {
        return apiJobService.startJob(apiJob);
    }

    @PostMapping(value = "/stopJob")
    public ResponseData stopJob(@RequestBody ApiJobBO apiJob) {
        return apiJobService.stopJob(apiJob);
    }

    @PostMapping(value = "/offlineJob")
    public ResponseData offlineJob(@RequestBody ApiJobBO apiJob) {
        return apiJobService.offlineJob(apiJob);
    }

    @PostMapping(value = "/jobStatus")
    public ResponseData jobStatus(@RequestBody ApiJobBO apiJob) {
        return apiJobService.jobStatus(apiJob);
    }

    @PostMapping(value = "/jobRuntimeLogs")
    public ResponseData jobRuntimeLogs(@RequestBody ApiJobBO apiJob) {
        return apiJobService.jobRuntimeLogs(apiJob);
    }

    @PostMapping(value = "/jobMonitorData")
    public ResponseData jobMonitorData(@RequestBody ApiJobBO apiJob) {
        return apiJobService.jobMonitorData(apiJob);
    }

    @PostMapping(value = "/alertMonitorData")
    public ResponseData alertMonitorData(@RequestBody ApiJobBO apiJob) {
        return apiJobService.alertMonitorData(apiJob);
    }
    @PostMapping(value = "/queryFailData")
    public ResponseData queryFailData(@RequestBody ApiJobBO apiJob) {
        return apiJobService.queryFailData(apiJob);
    }

    @PostMapping(value = "/delJobFile")
    public ResponseData delJobFile(@RequestBody ApiJobBO apiJob) {
        log.info("【数据集成】 delJobFile:{} ", JSONObject.toJSONString(apiJob));
        return apiJobService.delJobFile(apiJob);
    }


}
