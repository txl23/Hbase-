package com.yc.task2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.management.ImmutableDescriptor;
import java.io.IOException;

/**
 * @program: Hbase
 * @description:
 * @author: 汤僖龙
 * @create: 2021-08-12 16:22
 */
//输入的数据  Longwriteable：偏移量  ，Text这一行数据  输出 rowkey  put
public class ReadFruitFromHDFSMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //从HDFS读取数据
        String lineValue=value.toString();
        //读取出来的每行数据用\t进行风格，存于String数组
        String[]values=lineValue.split("\t");
        String rowKey=values[0];
        String name=values[1];
        String color=values[2];
        //舒适化rowkey
        ImmutableBytesWritable rowKeyWritable=new ImmutableBytesWritable(Bytes.toBytes(rowKey));
        //参数分别：列族，列，值
        Put put=new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(name));
        put.add(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(color));
        context.write(rowKeyWritable,put);
    }
}
