package com.chitu.bigdata.sdp.test;

import com.chitu.bigdata.sdp.api.domain.ConnectInfo;
import com.chitu.bigdata.sdp.api.domain.FlinkTableGenerate;
import com.chitu.bigdata.sdp.api.domain.MetadataTableColumn;
import com.chitu.bigdata.sdp.service.datasource.HbaseDataSource;
import com.chitu.cloud.web.test.BaseTest;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * @author chenyun
 * @description: TODO
 * @date 2022/3/28 16:14
 */
public class HbaseTest extends BaseTest {

    @Autowired
    private HbaseDataSource hbaseDataSource;

    @Test
    public void testConnection(){
        try {
            ConnectInfo connectInfo = new ConnectInfo();
            String address = "******";
            connectInfo.setAddress(address);
            connectInfo.setHbaseZnode("/hbase-sdp");
            Connection connection = hbaseDataSource.getConnection(connectInfo);
            System.out.println("=========");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void testTableExists(){
        ConnectInfo connectInfo = new ConnectInfo();
        String address = "******";
        connectInfo.setAddress(address);
        connectInfo.setHbaseZnode("/hbase-sdp");
        connectInfo.setDatabaseName("db01");
        String tableName = "user";
        try {
            boolean result = hbaseDataSource.tableExists(connectInfo,tableName);
            System.out.println("========="+result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    @Test
    public List<MetadataTableColumn> testTableColumns(){
        List<MetadataTableColumn> result = null;
        ConnectInfo connectInfo = new ConnectInfo();
        String address = "******";
        connectInfo.setAddress(address);
        connectInfo.setHbaseZnode("/hbase-sdp");
        connectInfo.setDatabaseName("dim");
        String tableName = "waybill_wide";
        try {
            result = hbaseDataSource.getTableColumns(connectInfo,tableName);
            System.out.println("=========\n"+result);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    @Test
    public void testGenerateDDL(){
        List<MetadataTableColumn> result = testTableColumns();
        FlinkTableGenerate flinkTableGenerate = new FlinkTableGenerate();
        flinkTableGenerate.setFlinkTableName("flink_table");
        flinkTableGenerate.setSourceTableName("hbase_table");
        flinkTableGenerate.setAddress("******");
        flinkTableGenerate.setHbaseZnode("/hbase-sdp");
        flinkTableGenerate.setMetadataTableColumnList(result);
        String hbaseDDL = hbaseDataSource.generateDdl(flinkTableGenerate);
        System.out.println("=========\n"+hbaseDDL);
    }
}
