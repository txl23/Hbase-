package com.yc.WeiBo;

import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.IOException;
import java.util.List;

/**
 * @program: Hbase
 * @description:
 * @author: 汤僖龙
 * @create: 2021-08-12 10:48
 */
public class TestWeiBo {
    public  void testCreateWeiboTable(WeiBo wb) throws IOException {
      wb.createTable(WeiBo.TABLE_CONTENT, Compression.Algorithm.SNAPPY,1,1,new  String[]{"info"});
      wb.createTable(WeiBo.TABLE_RELATIONS, Compression.Algorithm.SNAPPY,1,1,new  String[]{"attends","fans"});
      wb.createTable(WeiBo.TABLE_RECEIVE_CONTENT_EMAIL, Compression.Algorithm.SNAPPY,1,1000,new  String[]{"info"});
    }
    public void testPublishContext(WeiBo wb) throws IOException {
        wb.publishContent("0002","hello");
        wb.publishContent("0002","今天天气不错");
        wb.publishContent("0003","今天天气不错");
        wb.publishContent("0003","今天天气不错");
    }
    public void  testAddAttend(WeiBo wb) throws IOException {
        wb.addAttends("0001","0002","0003");
        wb.addAttends("0002","0001");
    }
    public void  testShowMessage(WeiBo wb) throws IOException {
        List<Message>messages=wb.getAttendContent("0001");
        for (Message message:messages){
            System.out.println(message);
        }
    }
    public void  testRemoveAttend(WeiBo wb) throws IOException {
        wb.removeAttends("0001","0003");
    }
    public static void main(String[] args) throws IOException {
        WeiBo weiBo=new WeiBo();
        TestWeiBo testWeiBo=new TestWeiBo();
        weiBo.initNamespace();
        testWeiBo.testCreateWeiboTable(weiBo);
     // testWeiBo.testPublishContext(weiBo);
        testWeiBo.testAddAttend(weiBo);
        testWeiBo.testRemoveAttend(weiBo);
        testWeiBo.testShowMessage(weiBo);
    }
}
