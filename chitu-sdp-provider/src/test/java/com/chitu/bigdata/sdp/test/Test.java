package com.chitu.bigdata.sdp.test;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.chitu.bigdata.sdp.api.domain.MetricTableValueInfo;
import com.chitu.bigdata.sdp.utils.HdfsUtils;

import java.net.URLEncoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class Test {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "admin");
        HdfsUtils.copyToLocalFile("/opt/apache/hadoop-bigdata/etc/hadoop/", "/sdp/workspace/st_demo_pro/udf/bigdata-flink-udf/v1/bigdata-flink-udf.jar", "/sdp/workspace/st_demo_pro/udf/bigdata-flink-udf/v1/bigdata-flink-udf.jar");


        String imageCreateTime = "2022-11-09T07:48:38.451420736Z\n";
        String subImageCreateTime = imageCreateTime.substring(0, 19).replace("T"," ");
        System.out.println(subImageCreateTime);
        DateTime parseImageCreateTime = DateUtil.parseDateTime(subImageCreateTime);



        HashMap<String, Object> hashMap = new HashMap<>();
        hashMap.put("startTime",1666701387000L);//开始时间
        hashMap.put("endTime",1666702407000L);//结束时间
        hashMap.put("index","erp-java1-k8s-uat-flink-20*");//索引
        hashMap.put("keyword","");//关键字搜索
        HashMap<String, Object> filterMap = new HashMap<>();//高级搜索
        filterMap.put("field","kubernetes.labels_app");//高级搜索字段
        filterMap.put("condition","and");//and包含，not不包含
        filterMap.put("keywords",Arrays.asList("sutao-k8s-test-1025"));//高级搜索多个内容
        hashMap.put("filter", Arrays.asList(filterMap));
        String applicationUrl = JSON.toJSONString(hashMap);
        //压缩
//        String compressToBase64 = LZString.compressToBase64(applicationUrl);
        String compressToBase64 = null;
        //url编码
        String encode = URLEncoder.encode(compressToBase64, "UTF-8")
                .replaceAll("\\+", "%20")
                .replaceAll("\\!", "%21")
                .replaceAll("\\'", "%27")
                .replaceAll("\\(", "%28")
                .replaceAll("\\)", "%29");
        String uri = StrUtil.concat(true, "https://*****/#/ops/log-query/application-log?applog_config=", encode);
        System.out.println(uri);







        MetricTableValueInfo metricTableValueInfo = new MetricTableValueInfo();
        CompletableFuture<Void> consumedTotal = CompletableFuture.runAsync(() -> {
            Map<String,Long> consumedTotalMap = new HashMap<>();
            consumedTotalMap.put("t1", 10L);
            metricTableValueInfo.setConsumedTotalMap(consumedTotalMap);
        });
        CompletableFuture<Void> deserializeFailNum = CompletableFuture.runAsync(() -> {
            Map<String,Long> consumedTotalMap = new HashMap<>();
            consumedTotalMap.put("t2", 10L);
            // System.out.println(1/0);
            metricTableValueInfo.setDeserializeFailNumMap(consumedTotalMap);
        });
        CompletableFuture<Void> pendingRecords = CompletableFuture.runAsync(() -> {
            Map<String,Long> consumedTotalMap = new HashMap<>();
            consumedTotalMap.put("t3", 10L);
            String s = "dfd".split("11")[2];
            metricTableValueInfo.setPendingRecordsMap(consumedTotalMap);
        });

        try {
            CompletableFuture.allOf(consumedTotal, deserializeFailNum, pendingRecords).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        System.out.println(metricTableValueInfo);

    }
}
