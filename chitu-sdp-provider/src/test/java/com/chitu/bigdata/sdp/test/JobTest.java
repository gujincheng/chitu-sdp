package com.chitu.bigdata.sdp.test;

import com.chitu.bigdata.sdp.api.bo.SdpJobBO;
import com.chitu.bigdata.sdp.api.enums.JobAction;
import com.chitu.bigdata.sdp.api.model.SdpJobInstance;
import com.chitu.bigdata.sdp.job.AutoPullTaskJob;
import com.chitu.bigdata.sdp.job.JobStatusSyncJob;
import com.chitu.bigdata.sdp.job.UatJobAutoOfflineJob;
import com.chitu.bigdata.sdp.mapper.SdpJobInstanceMapper;
import com.chitu.bigdata.sdp.mapper.SdpRawStatusHistoryMapper;
import com.chitu.bigdata.sdp.service.JobService;
import com.chitu.bigdata.sdp.service.monitor.JobStatusNotifyService;
import com.chitu.cloud.web.test.BaseTest;
import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * @author chenyun
 * @description: TODO
 * @date 2021/10/13 15:12
 */
@Slf4j
public class JobTest extends BaseTest {
    @Autowired
    private JobService jobService;
    @Autowired
    private JobStatusSyncJob jobStatusSyncJob;
    @Autowired
    private SdpRawStatusHistoryMapper historyMapper;
    @Autowired
    private SdpJobInstanceMapper instanceMapper;
    @Autowired
    private JobStatusNotifyService jobStatusNotifyService;
//    @Autowired
//    private Executor jobSyncExecutor;
    @Autowired
    private AutoPullTaskJob autoPullTaskJob;
    @Autowired
    private UatJobAutoOfflineJob autoOfflineJob;

    @Autowired
    private KubernetesClient kubernetesClient;

    @Test
    public void testAutoOffLine(){
        autoOfflineJob.jobAutoOfflineJob();
    }

    @Test
    public void testPullupTask(){
        autoPullTaskJob.pullTask();
    }


    @Test
    public void testSyncStatus1(){
        SdpJobInstance param = new SdpJobInstance();
        param.setId(9623L);
        List<SdpJobInstance> list = instanceMapper.queryInstance4Sync(param);
        SdpJobInstance instance = list.get(0);
//        JobStatusSyncThread jobStatusSyncThread = new JobStatusSyncThread(instance,null,instanceMapper,historyMapper,jobService, jobStatusNotifyService,kubernetesClient);
//        jobStatusSyncThread.execute(instance);
    }

    @Test
    public void testK8sSyncStatus() throws Exception{
        // id,project_id,job_id,flink_job_id,application_id,jobmanager_address,
        //raw_status,job_status,expect_status,is_latest,enabled_flag,env
        // (56789,215,1469,'c01087320a436f47c20eb8da6d77bc44','sutao-k8s-test-jobmanager-exception1',
        //'http://******','INITIALIZE','INITIALIZE','INITIALIZE',1,1,'uat'
        //);
        SdpJobInstance param = new SdpJobInstance();
        param.setId(56789L);
//        param.setProjectId(215l);
//        param.setJobId(1469l);
//        param.setFlinkJobId("c01087320a436f47c20eb8da6d77bc44");
//        param.setApplicationId("sutao-k8s-test-jobmanager-exception1");
//        param.setJobManagerAddress("http://******");
//        param.setRawStatus("INITIALIZE");
//        param.setJobStatus("INITIALIZE");
//        param.setExpectStatus("RUNNING");
//        param.setIsLatest(true);
//        param.setEnabledFlag(1l);
        while(true){
//            JobStatusSyncThread jobStatusSyncThread = new JobStatusSyncThread(param,null,instanceMapper,historyMapper,jobService, jobStatusNotifyService,kubernetesClient);
//            jobStatusSyncThread.execute(param);
            Thread.sleep(2000l);
        }

    }

    @Test
    public void testSyncStatus(){
//        jobStatusSyncJob.syncJobInstance();
    }

    @Test
    public void testJobStart(){
        SdpJobBO jobBo = new SdpJobBO();
        String content = "create table flink_test_1 ( \n" +
                "  operateType varchar,\n" +
                "  `old` ROW(),\n" +
                "  ts bigint,\n" +
                "  databaseName varchar,\n" +
                "  tableName varchar,\n" +
                "  `data` ROW(\n" +
                "\tid \t\t\t\t\tbigint, \t\t\t\n" +
                "\tfollow_fly_cost_id\tbigint,\t\t\n" +
                "\tdetail_type\t\t\tint,\t\t\t\t\n" +
                "\tcost\t\t\t\tdecimal(38,10),\t\n" +
                "\trate\t\t\t\tdecimal(38,10),\t\n" +
                "\ttrace_id\t\t\tvarchar,\t\t\t\n" +
                "\tenabled_flag\t\tint,\t\t\t\t\n" +
                "\tcreated_by\t\t\tvarchar,\t\t\t\n" +
                "\tcreation_date\t\tvarchar,\t\t\t\n" +
                "\tupdated_by\t\t\tvarchar,\t\t\t\n" +
                "\tupdation_date\t\tvarchar,\t\t\t\n" +
                "\texpense_number\t\tvarchar,\t\t\t\n" +
                "\tremark\t\t\t\tvarchar,  \t\t\n" +
                "\tmin_cost\t\t\tdecimal(38,10),  \n" +
                "\tdataflow_flag\t\tvarchar,\t\t\t\n" +
                "\tdataflow_time\t\ttimestamp) \t\t  \n" +
                ")\n" +
                " with ( \n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'tms_followfly-follow_fly_cost_detail',\n" +
                "  'properties.bootstrap.servers' = '******', \n" +
                "  'properties.group.id' = 'flink_gp_test1',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'properties.zookeeper.connect' = '******/kafka'\n" +
                " );\n" +
                "\n" +
                "CREATE TABLE chenyun_test_1 (\n" +
                "  id \t\t\t\t\tbigint, \t\t\t\n" +
                "  follow_fly_cost_id\tbigint,\t\t\n" +
                "  detail_type\t\t\tint,\t\t\t\t\n" +
                "  cost\t\t\t\t\tdecimal(38,10),\t\n" +
                "  rate\t\t\t\t\tdecimal(38,10),\t\n" +
                "  trace_id\t\t\t\tstring,\t\t\t\n" +
                "  enabled_flag\t\t\tint,\t\t\t\t\n" +
                "  created_by\t\t\tstring,\t\t\t\n" +
                "  creation_date\t\t\tstring,\t\t\t\n" +
                "  updated_by\t\t\tstring,\t\t\t\n" +
                "  updation_date\t\t\tstring,\t\t\t\n" +
                "  expense_number\t\tstring,\t\t\t\n" +
                "  remark\t\t\t\tstring,  \t\t\n" +
                "  min_cost\t\t\t\tdecimal(38,10),  \n" +
                "  dataflow_flag\t\t\tstring,\t\t\t\n" +
                "  dataflow_time\t\t\ttimestamp,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                " ) WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://******/flink_web?characterEncoding=UTF-8',\n" +
                "   'table-name' = 'chenyun_test_1',\n" +
                "   'username' = 'bdata',\n" +
                "   'password' = '******',\n" +
                " );\n" +
                "\n" +
                "INSERT INTO chenyun_test_1 \n" +
                "SELECT \n" +
                "data.id as id,\n" +
                "data.follow_fly_cost_id as follow_fly_cost_id,\n" +
                "data.detail_type as detail_type,\n" +
                "data.cost as cost,\t\n" +
                "data.rate as rate,\t\n" +
                "data.trace_id as trace_id,\t\n" +
                "data.enabled_flag as enabled_flag,\n" +
                "data.created_by\tas created_by,\n" +
                "data.creation_date as creation_date,\n" +
                "data.updated_by as updated_by,\n" +
                "data.updation_date as updation_date,\n" +
                "data.expense_number as expense_number,\n" +
                "data.remark\tas remark,\n" +
                "data.min_cost as min_cost,\t\n" +
                "data.dataflow_flag as dataflow_flag,\n" +
                "data.dataflow_time as dataflow_time \n" +
                "FROM flink_test_1;";
        jobBo.getVo().setJobContent(content);
        jobBo.getVo().setProjectId(6L);
        jobService.startJob(jobBo,JobAction.START.toString());
    }
}
