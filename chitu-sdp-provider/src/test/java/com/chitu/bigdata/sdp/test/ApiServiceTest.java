package com.chitu.bigdata.sdp.test;

import com.alibaba.fastjson.JSON;
import com.chitu.bigdata.sdp.api.bo.ApiJobBO;
import com.chitu.bigdata.sdp.api.domain.SourceConfig;
import com.chitu.bigdata.sdp.service.ApiJobServiceImpl;
import com.chitu.cloud.model.ResponseData;
import com.chitu.cloud.web.test.BaseTest;
import org.assertj.core.util.Lists;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author chenyun
 * @description: TODO
 * @date 2022/2/25 16:34
 */
public class ApiServiceTest extends BaseTest {

    @Autowired
    private ApiJobServiceImpl jobApiService;

    @Test
    public void testSaveJob(){
        ApiJobBO jobBO = new ApiJobBO();
        jobBO.setFlinkSQL("CREATE TABLE order_log (\n" +
                "  code VARCHAR,\n" +
                "  order_num BIGINT,\n" +
                "  ts TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'properties.bootstrap.servers' = 'szzb-bg-uat-etl-10:9092,szzb-bg-uat-etl-11:9092,szzb-bg-uat-etl-12:9092',\n" +
                "  'topic' = 'order_log',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'scan.topic-partition-discovery.interval' = '10000',\n" +
                "  'format' = 'json'\n" +
                ");\n" +
                "CREATE TABLE map_sink (\n" +
                "  dt VARCHAR,\n" +
                "  code VARCHAR,\n" +
                "  order_num BIGINT,\n" +
                "  PRIMARY KEY (dt) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://******',\n" +
                "  'username' = '******',\n" +
                "  'password' = '******',\n" +
                "  'table-name' = 'map'\n" +
                ");\n" +
                "INSERT INTO map_sink\n" +
                "  SELECT\n" +
                "  DATE_FORMAT(ts, 'yyyy-MM-dd HH:00') dt,\n" +
                "  code,\n" +
                "  sum(order_num) AS order_num\n" +
                "FROM order_log\n" +
                "GROUP BY DATE_FORMAT(ts, 'yyyy-MM-dd HH:00'),code;");
        jobBO.setFlinkYaml("state.backend.incremental: true");
        jobBO.setJobName("job-test-DI");
        jobBO.setFolderName("目录-DI");
        jobBO.setProjectCode("project-DI");
        jobBO.setBusinessFlag("DI");
        SourceConfig config = new SourceConfig();
        config.setParallelism(2);
        jobBO.setSourceConfig(config);
        jobBO.setFileId(641L);

        ResponseData result = jobApiService.saveJob(jobBO);

        System.out.println(JSON.toJSONString(result));

    }

    @Test
    public void testStartJob(){
        ApiJobBO jobBO = new ApiJobBO();
        jobBO.setId(933L);
        jobBO.setStartMode("timestamp");//specific-offsets,latest-offset,earliest-offset,timestamp
        jobBO.setStartTimestamp("1645780894000");
        jobBO.setStartOffsets("partition:0,offset:42;partition:1,offset:300");
        ResponseData result = jobApiService.startJob(jobBO);

        System.out.println(JSON.toJSONString(result));

    }

    @Test
    public void testJobStatus(){
        ApiJobBO jobBO = new ApiJobBO();
        jobBO.setIds(Lists.newArrayList(1L,2L));
        ResponseData result = jobApiService.jobStatus(jobBO);

        System.out.println(JSON.toJSONString(result));

    }

}
