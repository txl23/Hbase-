package com.yc.WeiBo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @program: Hbase
 * @description:
 * @author: 汤僖龙
 * @create: 2021-08-12 10:34
 */
public class WeiBo {
    private static Configuration conf;
    public static final  byte[]TABLE_CONTENT= Bytes.toBytes("weibo:content");
    public static final  byte[]TABLE_RELATIONS=Bytes.toBytes("weibo:relations");
    public static final byte[]TABLE_RECEIVE_CONTENT_EMAIL=Bytes.toBytes("weibo:receive_content_email");
    static {
        conf= HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","node1:2181,node2:2181,node3:2181");

    }

    public void initNamespace(){
        try (HBaseAdmin admin=new HBaseAdmin(conf);){
            try {
                NamespaceDescriptor rel=admin.getNamespaceDescriptor("weibo");
                System.out.println("获取了weibo的命名空间");
            }catch (NamespaceNotFoundException e){
                NamespaceDescriptor weibo=NamespaceDescriptor
                        .create("weibo")
                        .addConfiguration("create","zy")
                        .addConfiguration("create_time",System.currentTimeMillis()+"")
                        .build();
                admin.createNamespace(weibo);
                System.out.println("微博命名空间创建成功");
            }
        }catch (MasterNotRunningException e){
            e.printStackTrace();
        }catch (ZooKeeperConnectionException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    public  void createTable(byte[] tableName, Compression.Algorithm compression,int minVersions,int maxVersions,String[] families)throws MasterNotRunningException, IOException{
        try (HBaseAdmin admin=new HBaseAdmin(conf);){

                HTableDescriptor content=new HTableDescriptor(TableName.valueOf(tableName));
                if (families!=null&&families.length>0){
                    for (String f:families){
                        HColumnDescriptor info=new HColumnDescriptor(Bytes.toBytes(f));
                                info.setBlockCacheEnabled(true);
                        info.setBlocksize(2097152);
                        if (compression!=null){
                            info.setCompressionType(compression);
                        }
                        info.setMinVersions(minVersions);
                        info.setMaxVersions(maxVersions);
                        content.addFamily(info);
                    }
                }
                admin.createTable(content);
            System.out.println("创建表"+new String( tableName)+"成功");
        }catch (MasterNotRunningException e){
            e.printStackTrace();
        }catch (ZooKeeperConnectionException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    public void publishContent(String uid,String content) throws IOException {
        try (
            HTable hTable=new HTable(conf,TableName.valueOf(TABLE_CONTENT));//内容表
            HTable relationsTBL=new HTable(conf,TableName.valueOf(TABLE_RELATIONS));//查询用户关系表  fans这个列族中我的粉丝
            HTable recTBL=new HTable(conf,TableName.valueOf(TABLE_RECEIVE_CONTENT_EMAIL));//收件表info列族
            ){
            long timestamp=System.currentTimeMillis();
            String rowkey=uid+"_"+timestamp;
            Put put=new Put(Bytes.toBytes(rowkey));
            put.add(Bytes.toBytes("info"),Bytes.toBytes("content"),timestamp, Bytes.toBytes(content));
            hTable.put(put);
            System.out.println("发布微博成功");
            //b、向微博收件箱表中加入发布的Rowkey
            //b.1查询用户关系表，得到当前用户有那些粉丝
            //b.2取出目标数据
            Get get=new Get(Bytes.toBytes(uid));
            get.addFamily(Bytes.toBytes("fans"));
            Result result=relationsTBL.get(get);
            List<byte[]>fans=new ArrayList<>();
            for (Cell cell:result.rawCells()){
                fans.add(CellUtil.cloneQualifier(cell));
            }
            //如果该用户没有粉丝直接返回
            if (fans.size()<=0){
                return;
            }
            List<Put>puts=new ArrayList<>();
            //开始操作收件箱
            for (byte[]fan:fans){
                Put fanPut=new Put(fan);
                fanPut.add(Bytes.toBytes("info"),Bytes.toBytes(uid),timestamp, Bytes.toBytes(rowkey));
                puts.add(fanPut);
            }
            recTBL.put(puts);
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    /**
     * 关注用户逻辑
     * a在微博用户关系表中，对当前主动操作的用户添加型的关注的好友
     * b在微博用户关系中，对被关注的用户添加粉丝（当前操作用户）
     * c当前操作用户的微博收件箱添加所关注的用户发布的微博rowkey
     *
     */
    public void addAttends(String uid,String ...attends) throws IOException {
        //参数过滤
        if (attends==null||attends.length<=0||uid==null||uid.length()<=0){
            return;
        }
        try (
                HTable hTable=new HTable(conf,TableName.valueOf(TABLE_CONTENT));//内容表
                HTable relationsTBL=new HTable(conf,TableName.valueOf(TABLE_RELATIONS));//查询用户关系表  fans这个列族中我的粉丝
                HTable recTBL=new HTable(conf,TableName.valueOf(TABLE_RECEIVE_CONTENT_EMAIL));//收件表info列族
        ){
            List<Put>puts=new ArrayList<>();
            Put attendPut=new Put(Bytes.toBytes(uid));
            for (String attend:attends){
                //为当前用户在attends列族中添加关注的人     rowkey为uid
                attendPut.add(Bytes.toBytes("attends"), Bytes.toBytes(attend), Bytes.toBytes(attend));
                //b 为被关注的人在fans中添加粉丝
                Put fansPut=new  Put(Bytes.toBytes(attend));
                fansPut.add(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(uid));
                //将所有关注的人一个一个的添加到puts集合中
                puts.add(fansPut);
            }
            puts.add(attendPut);
            relationsTBL.put(puts);
            //c.1微博收件箱添加关注的用户发布的微博
            Scan scan=new Scan();
            //用于存放，取出来的关注的人所发布的微博内容的rowkey
            List<byte[]>rowkeys=new ArrayList<>();
            //循环uid关注的所有博主
            for (String attend:attends){
                //过滤扫描的rowkey，即前置位匹配被关注的人的uid
                RowFilter filter=new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(attend+"_"));
                //为扫描对象添加过滤器
                scan.setFilter(filter);
                //通过扫描对象得到scannner
                ResultScanner result=hTable.getScanner(scan);
                //迭代器遍历扫描出来的结果
                Iterator<Result>iterator=result.iterator();
                while (iterator.hasNext()){
                    //取出每一个符合扫描出来的结果集
                    Result r=iterator.next();
                    for (Cell cell:r.rawCells()){
                        //将得到的rowkey放置于集合容器中
                        rowkeys.add(CellUtil.cloneRow(cell));
                    }
                }
            }
            //c.2将取出的微博的rowkey放置于当前操作用户的收件箱中、
            if (rowkeys.size()<=0){
                return;
            }
            //得到微博收件箱表的操作对象
            //用于存放多个关注的用户的发布的多条微博rowkey信息
            List<Put>recPuts=new ArrayList<>();
            for (byte[]rk:rowkeys){
                Put put=new Put(Bytes.toBytes(uid));
                //uid_timestamp
                String rowkey= Bytes.toString(rk);
                //循环uid  发帖人的编号
                String attendUID=rowkey.substring(0,rowkey.indexOf("_"));
                long timestamp=Long.parseLong(rowkey.substring(rowkey.indexOf("_")+1));
                //将微博rowkey添加到指定单元格中
                put.add(Bytes.toBytes("info"), Bytes.toBytes(attendUID),timestamp,rk);
                recPuts.add(put);
            }
            recTBL.put(recPuts);
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    //取消关注
    public void removeAttends(String uid,String ...attends) throws IOException {
        if (uid==null||uid.length()<=0||attends==null||attends.length<0){
            return;
        }try (
                HTable hTable=new HTable(conf,TableName.valueOf(TABLE_CONTENT));//内容表
                HTable relationsTBL=new HTable(conf,TableName.valueOf(TABLE_RELATIONS));//查询用户关系表  fans这个列族中我的粉丝
                HTable recTBL=new HTable(conf,TableName.valueOf(TABLE_RECEIVE_CONTENT_EMAIL));//收件表info列族
        ){
            List<Delete>deletes=new ArrayList<>();
            Delete attenDelete=new Delete(Bytes.toBytes(uid));
            for (String attend:attends){
                attenDelete.deleteColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend));
                Delete fansDelete=new Delete(Bytes.toBytes(attend));
                fansDelete.deleteColumn(Bytes.toBytes("fans"),Bytes.toBytes(uid));
                deletes.add(fansDelete);
            }
            deletes.add(attenDelete);
            relationsTBL.delete(deletes);
            Delete recDelete=new Delete(Bytes.toBytes(uid));
            for (String attend:attends){
                recDelete.deleteColumn(Bytes.toBytes("info"), Bytes.toBytes(attend));
            }
            recTBL.delete(recDelete);

        }catch (IOException e){
            e.printStackTrace();
        }
    }
    //获取内容
    public  List<Message>getAttendContent(String uid) throws IOException {
        try (
                HTable hTable=new HTable(conf,TableName.valueOf(TABLE_CONTENT));//内容表
                HTable relationsTBL=new HTable(conf,TableName.valueOf(TABLE_RELATIONS));//查询用户关系表  fans这个列族中我的粉丝
                HTable recTBL=new HTable(conf,TableName.valueOf(TABLE_RECEIVE_CONTENT_EMAIL));//收件表info列族
        ){
            Get get =new Get(Bytes.toBytes(uid));
            get.setMaxVersions(2);
            List<byte[]>rowkeys=new ArrayList<>();
            Result result=recTBL.get(get);
            for (Cell cell:result.rawCells()){
                rowkeys.add(CellUtil.cloneValue(cell));
            }
            List<Get>gets=new ArrayList<>();
            for (byte[] rk:rowkeys){
                Get g=new Get(rk);
                gets.add(g);
            }
            Result[]results=hTable.get(gets);
            List<Message>messages=new ArrayList<>();
            for (Result res:results){
                for (Cell cell:res.rawCells()){
                    Message message=new Message();
                    String rowkey= Bytes.toString(CellUtil.cloneRow(cell));
                    String userid=rowkey.substring(0,rowkey.indexOf("_"));
                    String timestamp=rowkey.substring(rowkey.indexOf("_"));
                    String content= Bytes.toString(CellUtil.cloneValue(cell));
                    message.setContent(content);
                    message.setTimestamp(timestamp);
                    message.setUid(userid);
                    messages.add(message);
                }
            }
            return messages;
        }catch (IOException e){
            e.printStackTrace();
        }
        return null;
    }
}
