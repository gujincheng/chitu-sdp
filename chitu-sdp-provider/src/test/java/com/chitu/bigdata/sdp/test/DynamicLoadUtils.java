package com.chitu.bigdata.sdp.test;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.CompletableFuture;

/**
 * @author cm
 * @description
 * @date 2021/05/21
 */
public class DynamicLoadUtils {

    public static void main(String[] args) throws Exception {

        String sql = "CREATE TABLE `biz_fe_log` (\n" +
                "  `@timestamp` VARCHAR,\n" +
                "  `@metadata` VARCHAR,\n" +
                "  `message` VARCHAR,\n" +
                "  `log` VARCHAR,\n" +
                "  `input` VARCHAR,\n" +
                "  `fields` VARCHAR,\n" +
                "  `ecs` VARCHAR,\n" +
                "  `host` VARCHAR,\n" +
                "  `agent` VARCHAR\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'properties.bootstrap.servers' = '44531120d653930794f7e9b61c0d8233',\n" +
                "  'topic' = 'erp-java2-ops-dorisdb-prd-biz-run',\n" +
                "  'properties.group.id' = 'sdp-starrocks_ops-fe_log',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'scan.topic-partition-discovery.interval' = '10000',\n" +
                "  'format' = 'json',\n" +
                "  'json.fail-on-missing-field' = 'false',\n" +
                "  'json.ignore-parse-errors' = 'false' -- 不可更改，false表示平台开启解析失败监控\n" +
                ");\n" +
                "CREATE TABLE `ods_broker_load_finish_sr_sink` (\n" +
                "  `cluster_name` VARCHAR,\n" +
                "  `finish_time` TIMESTAMP,\n" +
                "  `label` VARCHAR,\n" +
                "  `frontend_ip` VARCHAR,\n" +
                "  `db_id` BIGINT,\n" +
                "  `table_id` BIGINT,\n" +
                "  `callback_id` BIGINT,\n" +
                "  `transcation_id` BIGINT,\n" +
                "  `transcation_status` VARCHAR,\n" +
                "  `prepare_time` TIMESTAMP,\n" +
                "  `commit_time` TIMESTAMP,\n" +
                "  PRIMARY KEY(cluster_name, finish_time, label) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'starrocks',\n" +
                "  'jdbc-url' = '240c6aed81e9fcda6d652298ab0ca655',\n" +
                "  'load-url' = '35e51958c0713cd7e98331739cc77c5d',\n" +
                "  'username' = 'bigdata_biz_rw',\n" +
                "  'password' = '******',\n" +
                "  'database-name' = 'starrocks_dw',\n" +
                "  'table-name' = 'ods_broker_load_finish',\n" +
                "  'sink.buffer-flush.max-rows' = '1000000',\n" +
                "  'sink.buffer-flush.max-bytes' = '300000000',\n" +
                "  'sink.buffer-flush.interval-ms' = '15000',\n" +
                "  'sink.buffer-flush.enqueue-timeout-ms' = '600000',\n" +
                "  'sink.semantic' = 'at-least-once',\n" +
                "  'sink.max-retries' = '1',\n" +
                "  'sink.parallelism' = '1',\n" +
                "  'sink.properties.column_separator' = '\\x01',\n" +
                "  'sink.properties.row_delimiter' = '\\x02'\n" +
                ");\n" +
                "CREATE TABLE `common_fe_log` (\n" +
                "  `@timestamp` VARCHAR,\n" +
                "  `@metadata` VARCHAR,\n" +
                "  `message` VARCHAR,\n" +
                "  `input` VARCHAR,\n" +
                "  `fields` VARCHAR,\n" +
                "  `ecs` VARCHAR,\n" +
                "  `host` VARCHAR,\n" +
                "  `agent` VARCHAR,\n" +
                "  `log` VARCHAR\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'properties.bootstrap.servers' = '44531120d653930794f7e9b61c0d8233',\n" +
                "  'topic' = 'erp-java2-ops-dorisdb-prd-com-run',\n" +
                "  'properties.group.id' = 'sdp-starrocks_ops-fe_log',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'scan.topic-partition-discovery.interval' = '10000',\n" +
                "  'format' = 'json',\n" +
                "  'json.fail-on-missing-field' = 'false',\n" +
                "  'json.ignore-parse-errors' = 'false' -- 不可更改，false表示平台开启解析失败监控\n" +
                ");\n" +
                "CREATE TABLE `vts_fe_log` (\n" +
                "  `@timestamp` VARCHAR,\n" +
                "  `@metadata` VARCHAR,\n" +
                "  `message` VARCHAR,\n" +
                "  `log` VARCHAR,\n" +
                "  `input` VARCHAR,\n" +
                "  `fields` VARCHAR,\n" +
                "  `ecs` VARCHAR,\n" +
                "  `host` VARCHAR,\n" +
                "  `agent` VARCHAR\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'properties.bootstrap.servers' = '44531120d653930794f7e9b61c0d8233',\n" +
                "  'topic' = 'erp-java2-ops-dorisdb-prd-vts-run',\n" +
                "  'properties.group.id' = 'sdp-starrocks_ops-fe_log',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'scan.topic-partition-discovery.interval' = '10000',\n" +
                "  'format' = 'json',\n" +
                "  'json.fail-on-missing-field' = 'false',\n" +
                "  'json.ignore-parse-errors' = 'false' -- 不可更改，false表示平台开启解析失败监控\n" +
                ");\n" +
                "CREATE TABLE `analysis_fe_log` (\n" +
                "  `@timestamp` VARCHAR,\n" +
                "  `@metadata` VARCHAR,\n" +
                "  `fields` VARCHAR,\n" +
                "  `input` VARCHAR,\n" +
                "  `ecs` VARCHAR,\n" +
                "  `host` VARCHAR,\n" +
                "  `agent` VARCHAR,\n" +
                "  `log` VARCHAR,\n" +
                "  `message` VARCHAR\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'properties.bootstrap.servers' = '44531120d653930794f7e9b61c0d8233',\n" +
                "  'topic' = 'erp-java2-ops-dorisdb-prd-analysis-run',\n" +
                "  'properties.group.id' = 'sdp-starrocks_ops-fe_log',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'scan.topic-partition-discovery.interval' = '10000',\n" +
                "  'format' = 'json',\n" +
                "  'json.fail-on-missing-field' = 'false',\n" +
                "  'json.ignore-parse-errors' = 'false' -- 不可更改，false表示平台开启解析失败监控\n" +
                ");\n" +
                "CREATE TABLE `starrocks_extra_kafka_sink` (\n" +
                "cluster_name VARCHAR,\n" +
                "db_id VARCHAR,\n" +
                "label VARCHAR,\n" +
                "type VARCHAR\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'properties.bootstrap.servers' = '435228abcde350c33582c9153421719d',\n" +
                "  'topic' = 'starrocks-extra',\n" +
                "  'sink.partitioner' = 'round-robin',\n" +
                "  'format' = 'json',\n" +
                "  'properties.acks' ='all',\n" +
                "  'properties.batch.size' = '16384',\n" +
                "  'properties.linger.ms' = '50',\n" +
                "  'properties.buffer.memory' ='33554432'\n" +
                ");\n" +
                "\n" +
                "CREATE TABLE `biz_adhoc_fe_log` (\n" +
                "  `@timestamp` VARCHAR,\n" +
                "  `@metadata` VARCHAR,\n" +
                "  `fields` VARCHAR,\n" +
                "  `ecs` VARCHAR,\n" +
                "  `host` VARCHAR,\n" +
                "  `agent` VARCHAR,\n" +
                "  `log` VARCHAR,\n" +
                "  `message` VARCHAR,\n" +
                "  `input` VARCHAR\n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'properties.bootstrap.servers' = '44531120d653930794f7e9b61c0d8233',\n" +
                "'topic' = 'erp-java2-ops-dorisdb-prd-biz-adhoc-run',\n" +
                "'properties.group.id' = 'sdp-starrocks_ops-fe_log',\n" +
                "'scan.startup.mode' = 'timestamp',\n" +
                "'scan.topic-partition-discovery.interval' = '10000',\n" +
                "'format' = 'json',\n" +
                "'json.fail-on-missing-field' = 'false',\n" +
                "'json.ignore-parse-errors' = 'false',\n" +
                "'scan.startup.timestamp-millis' = '1659664800000'\n" +
                ");\n" +
                "CREATE TABLE `tms_fe_log` (\n" +
                "  `@timestamp` VARCHAR,\n" +
                "  `@metadata` VARCHAR,\n" +
                "  `ecs` VARCHAR,\n" +
                "  `host` VARCHAR,\n" +
                "  `log` VARCHAR,\n" +
                "  `message` VARCHAR,\n" +
                "  `input` VARCHAR,\n" +
                "  `fields` VARCHAR,\n" +
                "  `agent` VARCHAR\n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'properties.bootstrap.servers' = '44531120d653930794f7e9b61c0d8233',\n" +
                "'topic' = 'erp-java2-ops-dorisdb-prd-tms-run',\n" +
                "'properties.group.id' = 'sdp-starrocks_ops-fe_log',\n" +
                "'scan.startup.mode' = 'timestamp',\n" +
                "'scan.topic-partition-discovery.interval' = '10000',\n" +
                "'format' = 'json',\n" +
                "'json.fail-on-missing-field' = 'false',\n" +
                "'json.ignore-parse-errors' = 'false',\n" +
                "'scan.startup.timestamp-millis' = '1659657652000'\n" +
                ");\n" +
                "\n" +
                "-- StarRocks Sink\n" +
                "INSERT INTO `ods_broker_load_finish_sr_sink`\n" +
                "SELECT\n" +
                "    cluster_name\n" +
                "  , TO_TIMESTAMP(FROM_UNIXTIME(CAST(split_index( split_index(str, ', ', 11) , ': ' , 1) AS BIGINT) / 1000, 'yyyy-MM-dd HH:mm:ss')) AS finish_time\n" +
                "  , split_index( split_index(str, ', ', 1) , ': ' , 1 ) AS label\n" +
                "  , split_index( split_index(str, ', ', 5) , ': ' , 2 ) AS frontend_ip\n" +
                "  , CAST( split_index( split_index(str, ', ', 2) , ': ' , 1 ) AS BIGINT) AS db_id\n" +
                "  , CAST( split_index( split_index(str, ', ', 3) , ': ' , 1 ) AS BIGINT) AS table_id\n" +
                "  , CAST( split_index( split_index(str, ', ', 4) , ': ' , 1 ) AS BIGINT) AS callback_id\n" +
                "  , CAST( split_index( split_index(str, ', ', 0) , ': ' , 1 ) AS BIGINT) AS transcation_id\n" +
                "  , split_index( split_index(str, ', ', 6) , ': ' , 1 ) AS transcation_status\n" +
                "  , TO_TIMESTAMP(FROM_UNIXTIME(CAST(split_index( split_index(str, ', ', 9) , ': ' , 1) AS BIGINT) / 1000, 'yyyy-MM-dd HH:mm:ss')) AS prepare_time\n" +
                "  , TO_TIMESTAMP(FROM_UNIXTIME(CAST(split_index( split_index(str, ', ', 10) , ': ' , 1) AS BIGINT) / 1000, 'yyyy-MM-dd HH:mm:ss')) AS commit_time\n" +
                "FROM \n" +
                "(\n" +
                "  SELECT\n" +
                "      'DORISDB_BIZ' AS cluster_name\n" +
                "    , split_index(`message`, 'TransactionState. ', 1) as str\n" +
                "  FROM `biz_fe_log`\n" +
                "  WHERE `message` LIKE '%loadingStatus=EtlStatus%'\n" +
                "    AND `message` LIKE '%transaction status: VISIBLE%'\n" +
                "UNION ALL\n" +
                "\n" +
                "  SELECT\n" +
                "      'DORISDB_VTS' AS cluster_name\n" +
                "    , split_index(`message`, 'TransactionState. ', 1) as str\n" +
                "  FROM `vts_fe_log`\n" +
                "  WHERE `message` LIKE '%loadingStatus=EtlStatus%'\n" +
                "    AND `message` LIKE '%transaction status: VISIBLE%'\n" +
                "UNION ALL\n" +
                "\n" +
                "  SELECT\n" +
                "      'DORISDB_COMMON' AS cluster_name\n" +
                "    , split_index(`message`, 'TransactionState. ', 1) as str\n" +
                "  FROM `common_fe_log`\n" +
                "  WHERE `message` LIKE '%loadingStatus=EtlStatus%'\n" +
                "    AND `message` LIKE '%transaction status: VISIBLE%'\n" +
                "UNION ALL\n" +
                "\n" +
                "  SELECT\n" +
                "      'DORISDB_QUICKBI' AS cluster_name\n" +
                "    , split_index(`message`, 'TransactionState. ', 1) as str\n" +
                "  FROM `analysis_fe_log`\n" +
                "  WHERE `message` LIKE '%loadingStatus=EtlStatus%'\n" +
                "    AND `message` LIKE '%transaction status: VISIBLE%'\n" +
                "UNION ALL\n" +
                "\n" +
                "  SELECT\n" +
                "      'DORISDB_TMS' AS cluster_name\n" +
                "    , split_index(`message`, 'TransactionState. ', 1) as str\n" +
                "  FROM `tms_fe_log`\n" +
                "  WHERE `message` LIKE '%loadingStatus=EtlStatus%'\n" +
                "    AND `message` LIKE '%transaction status: VISIBLE%'\n" +
                "UNION ALL\n" +
                "\n" +
                "  SELECT\n" +
                "      'DORISDB_BIZ_NEW' AS cluster_name\n" +
                "    , split_index(`message`, 'TransactionState. ', 1) as str\n" +
                "  FROM `biz_adhoc_fe_log`\n" +
                "  WHERE `message` LIKE '%loadingStatus=EtlStatus%'\n" +
                "    AND `message` LIKE '%transaction status: VISIBLE%'\n" +
                ") v1\n" +
                ";\n" +
                "\n" +
                "\n" +
                "\n" +
                "\n" +
                "\n" +
                "\n" +
                "/**\n" +
                "  发送如下消息到kafka, 供下游到指定集群获取label统计信息\n" +
                "{\"cluster_name\":\"biz-api-starrocks\", \"db_id\": 11591829, \"label\":\"ads_biz_customer_assembly_his_cm_tmp_2022070022223351\"}\n" +
                " */\n" +
                "INSERT INTO `starrocks_extra_kafka_sink`\n" +
                "SELECT\n" +
                "    cluster_name\n" +
                "  , split_index( split_index(str, ', ', 2) , ': ' , 1 ) AS db_id\n" +
                "  , split_index( split_index(str, ', ', 1) , ': ' , 1 ) AS label\n" +
                "  , 'broker_load' as type\n" +
                "FROM \n" +
                "(\n" +
                "  SELECT\n" +
                "      'biz-api-starrocks' AS cluster_name\n" +
                "    , split_index(`message`, 'TransactionState. ', 1) as str\n" +
                "  FROM `biz_fe_log`\n" +
                "  WHERE `message` LIKE '%loadingStatus=EtlStatus%'\n" +
                "    AND `message` LIKE '%transaction status: VISIBLE%'\n" +
                "\n" +
                "UNION ALL\n" +
                "\n" +
                "  SELECT\n" +
                "      '公共报表Doris' AS cluster_name\n" +
                "    , split_index(`message`, 'TransactionState. ', 1) as str\n" +
                "  FROM `common_fe_log`\n" +
                "  WHERE `message` LIKE '%loadingStatus=EtlStatus%'\n" +
                "    AND `message` LIKE '%transaction status: VISIBLE%'\n" +
                "UNION ALL\n" +
                "\n" +
                "  SELECT\n" +
                "      'TMS_dorisdb' AS cluster_name\n" +
                "    , split_index(`message`, 'TransactionState. ', 1) as str\n" +
                "  FROM `tms_fe_log`\n" +
                "  WHERE `message` LIKE '%loadingStatus=EtlStatus%'\n" +
                "    AND `message` LIKE '%transaction status: VISIBLE%'\n" +
                "UNION ALL\n" +
                "\n" +
                "  SELECT\n" +
                "      'biz-adhoc-starrocks' AS cluster_name\n" +
                "    , split_index(`message`, 'TransactionState. ', 1) as str\n" +
                "  FROM `biz_adhoc_fe_log`\n" +
                "  WHERE `message` LIKE '%loadingStatus=EtlStatus%'\n" +
                "    AND `message` LIKE '%transaction status: VISIBLE%'\n" +
                "UNION ALL\n" +
                "\n" +
                "  SELECT\n" +
                "      'QBI_dorisdb' AS cluster_name\n" +
                "    , split_index(`message`, 'TransactionState. ', 1) as str\n" +
                "  FROM `analysis_fe_log`\n" +
                "  WHERE `message` LIKE '%loadingStatus=EtlStatus%'\n" +
                "    AND `message` LIKE '%transaction status: VISIBLE%'\n" +
                ") v1\n" +
                ";\n";


        String path = "D:\\idea_work\\flink-sql-lineage\\target\\flink-sql-lineage-1.0.0.jar";

        loadJar(path);

        Class<?> lineageContextClass = Class.forName("com.dtwave.flink.lineage.LineageContext");

        //创建对象实例
        Object instance = lineageContextClass.newInstance();

        // 获取方法
        Method method = lineageContextClass.getMethod("parseFieldLineage", String.class);

        // 传入实例以及方法参数信息执行这个方法
        Object invoke = method.invoke(instance, sql);

        System.out.println(invoke);





        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {

            try {
                String path114 = "D:\\idea_work\\flink-sql-lineage\\target\\flink-sql-lineage-1.0.0.jar";

                loadJar(path114);

                Class<?> lineageContextClass114 = Class.forName("com.dtwave.flink.lineage.LineageContext");


                //创建对象实例
                Object instance114 = lineageContextClass114.newInstance();

                // 获取方法
                Method method114 = lineageContextClass114.getMethod("parseFieldLineage", String.class);

                // 传入实例以及方法参数信息执行这个方法
                Object invoke114 = method114.invoke(instance114, sql);

                System.out.println(invoke114);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }

        });

        future.get();

        System.out.println("");




    }


    public static void loadJar(String jarPath) {
        System.out.println("====>> into loadJar. jarPath: " + jarPath);
        // 从URLClassLoader类中获取类所在文件夹的方法，jar也可以认为是一个文件夹
        File jarFile = new File(jarPath);

        if (jarFile.exists() == false) {
            System.out.println("jar file not found.");
            return;
        }

        //获取类加载器的addURL方法，准备动态调用
        Method method = null;
        try {
            method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
        } catch (NoSuchMethodException | SecurityException e1) {
            e1.printStackTrace();
        }

        // 获取方法的访问权限，保存原始值
        boolean accessible = method.isAccessible();
        try {
            //修改访问权限为可写
            if (accessible == false) {
                method.setAccessible(true);
            }

            // 获取系统类加载器
            URLClassLoader classLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();

            //获取jar文件的url路径
            URL url = jarFile.toURI().toURL();

            //jar路径加入到系统url路径里
            method.invoke(classLoader, url);



        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //回写访问权限
            method.setAccessible(accessible);
        }

    }

}