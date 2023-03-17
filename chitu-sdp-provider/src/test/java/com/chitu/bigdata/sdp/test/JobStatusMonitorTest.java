package com.chitu.bigdata.sdp.test;

import com.chitu.bigdata.sdp.api.enums.AlertIndex;
import com.chitu.bigdata.sdp.api.model.SdpJobAlertRule;
import com.chitu.bigdata.sdp.mapper.SdpJobAlertRuleMapper;
import com.chitu.bigdata.sdp.service.monitor.JobStatusNotifyService;
import com.chitu.bigdata.sdp.service.monitor.RuleIndexMonitorFactory;
import com.chitu.cloud.web.test.BaseTest;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.concurrent.Executor;

public class JobStatusMonitorTest extends BaseTest {
    @Autowired
    private SdpJobAlertRuleMapper sdpJobAlertRuleMapper;

    @Autowired
    private Executor jobStatusExecutor;

    @Autowired
    private RuleIndexMonitorFactory ruleIndexFactory;

    @Test
    public void ruleMonitor() {
        List<SdpJobAlertRule> jobAlertRuleList = sdpJobAlertRuleMapper.getRuleByIndexName(AlertIndex.INTERRUPT_OPERATION.name());
        int ruleListSize = jobAlertRuleList.size();
        JobStatusNotifyService ruleIndexMonitor = (JobStatusNotifyService)ruleIndexFactory.getRuleIndexMonitor(AlertIndex.INTERRUPT_OPERATION.name());
        jobAlertRuleList.forEach(rule->{
                ruleIndexMonitor.monitorJobStatus(rule);
            });
    }


}
