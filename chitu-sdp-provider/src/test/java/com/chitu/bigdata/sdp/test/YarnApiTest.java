package com.chitu.bigdata.sdp.test;

import com.chitu.bigdata.sdp.flink.common.util.HadoopUtils;
import com.chitu.cloud.web.test.BaseTest;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * @author sutao
 * @create 2022-04-12 11:06
 */
public class YarnApiTest extends BaseTest {

    @Autowired
    private RestTemplate restTemplate;

    @Test
    public void testYarn() {
        String format = "%s/ws/v1/cluster/scheduler";
        String schedulerUrl = String.format(format, HadoopUtils.getRMWebAppURL(true));
        String result = restTemplate.getForObject(schedulerUrl, String.class);
        System.out.println(result);
    }

    /**
     * kill job
     * curl --location --request PUT 'http://10.83.192.6:8088/ws/v1/cluster/apps/application_1665940579703_6628/state' \
     * --header 'Content-Type: application/json' \
     * --data-raw '{"state":"KILLED"}'
     */
    @Test
    public void testYarnKillJob() {
        String format = "%s/ws/v1/cluster/apps/%s/state";
        // String schedulerUrl = String.format(format, HadoopUtils.getRMWebAppURL(true,hadoopConfDir),"application_1665940579703_13215");
        String schedulerUrl = String.format(format, "http://******/","application_1665940579703_13215");

        Map<String,Object> param = new HashMap<>();
        param.put("state","KILLED");
        restTemplate.put(schedulerUrl, param);
    }

}
