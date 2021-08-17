package com.yc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: Hbase
 * @description:
 * @author: 汤僖龙
 * @create: 2021-08-09 16:25
 */
public class HBaseUtil {
    public static Configuration conf;
    static {
        conf= HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","node1");
        conf.set("hbase.zookeeper.property.clientPort","2181");
    }
    public static void main(String[] args) throws IOException {
//        System.out.println(isTableExist("student"));
//        createTable("person2","info","address");
//     addRowData("person2","10001","info","age","20");
//        getAllRows("person2");
       getRow("person2","10001");
//        deleteMultiRow("person2","10001","10002");

    }

    public static boolean isTableExist(String tableName)throws MasterNotRunningException, IOException{
        Connection connection =ConnectionFactory.createConnection(conf);
        HBaseAdmin admin=(HBaseAdmin) connection.getAdmin();
        return admin.tableExists(tableName);
    }
    //创建表
    public static void createTable(String tableName,String... columnFamily)throws MasterNotRunningException, IOException{
       HBaseAdmin admin=new HBaseAdmin(conf);
       if (isTableExist(tableName)){
           System.out.println("表"+tableName+"已存在");
           return ;
       }
        HTableDescriptor descriptor=new HTableDescriptor(TableName.valueOf(tableName));
       for (String cf:columnFamily){
           descriptor.addFamily(new HColumnDescriptor(cf));
       }
       admin.createTable(descriptor);
        System.out.println("表"+tableName+"创建成功");
    }
    //删除表
    public  static void dropTable(String tableName) throws IOException {
        HBaseAdmin admin=new HBaseAdmin(conf);
        if (isTableExist(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表"+tableName+"删除成功");
        }else {
            System.out.println("表"+tableName+"不存在");
        }
    }

    //添加一行数据
    public  static  void  addRowData(String tableName,String rowKey,String columnFamily,String column,String value) throws IOException {
        HTable hTable=new HTable(conf,tableName);
        Put put=new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
        System.out.println("插入成功");
    }

    //获取所有数据Scan
    public static  void getAllRows(String tableName)throws  IOException{
        HTable hTable =new HTable(conf,tableName);
        Scan scan=new Scan();
        ResultScanner resultScanner=hTable.getScanner(scan);
        for (Result result:resultScanner){
            Cell[]cells=result.rawCells();
            for (Cell Cell:cells){
                System.out.println("行键："+Bytes.toString(CellUtil.cloneRow(Cell)));
                System.out.println("列表"+ Bytes.toString(CellUtil.cloneFamily(Cell)));
                System.out.println("列:"+Bytes.toString(CellUtil.cloneQualifier(Cell)));
                System.out.println("值:"+Bytes.toString(CellUtil.cloneValue(Cell)));
            }
        }
    }
    //获取行数据
    public static  void getRow(String tableName,String rowKey)throws  IOException{
        HTable table =new HTable(conf,tableName);
        Get get=new Get(Bytes.toBytes(rowKey));
        Result result= table.get(get);
        for (Cell cell :result.rawCells()){
                System.out.println("行键："+Bytes.toString(result.getRow()));
                System.out.println("列表"+ Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列:"+Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值:"+Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("时间戳："+cell.getTimestamp());
                System.out.println("版本"+cell.getMvccVersion());

        }
    }
    //指定列族，列名获取数据
    public static  void getRowQualifier(String tableName,String rowKey,String family,String qualifier)throws  IOException{
        HTable table =new HTable(conf,tableName);
        Get get=new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier));
        Result result= table.get(get);
        for (Cell cell :result.rawCells()){
            Cell[]cells=result.rawCells();
            for (Cell Cell:cells){
                System.out.println("行键："+Bytes.toString(result.getRow()));
                System.out.println("列表"+ Bytes.toString(CellUtil.cloneFamily(Cell)));
                System.out.println("列:"+Bytes.toString(CellUtil.cloneQualifier(Cell)));
                System.out.println("值:"+Bytes.toString(CellUtil.cloneValue(Cell)));
            }
        }
    }
    //删除多行数据
    public  static  void deleteMultiRow(String tableName,String ...rows) throws IOException {
        HTable hTable=new HTable(conf,tableName);
        List<Delete>deleteList=new ArrayList<Delete>();
        for (String row:rows){
            Delete delete=new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        hTable.delete(deleteList);
        hTable.close();
        System.out.println("删除成功");
    }
}
