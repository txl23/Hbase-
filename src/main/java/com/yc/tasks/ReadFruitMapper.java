package com.yc.tasks;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @program: Hbase
 * @description:读取hbase中的fruit表的数据，mapper输出的食  IMmutableBytesWritable ->rowkey。Put表述向hbase中添加数据的对象
 * @author: 汤僖龙
 * @create: 2021-08-12 09:11
 */
public class ReadFruitMapper extends TableMapper<ImmutableBytesWritable, Put> {
    @Override//rowkey 就是一行数据hbase中用Result进行包装
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //将fruit的name和color提取出来，相当于每一行数据读取出来放入到Put对象中
        Put put=new Put(key.get());  //key.get() 得到rowkey
        //从result中便利出每个cell，提娜佳column行
        for (Cell cell:value.rawCells()){ //cell的结构：rowkeyt ，列族：列名
            //添加/克隆列族：info
            if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
                if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    put.add(cell);
                }else if ("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    put.add(cell);
                }
            }
        }
        context.write(key,put);
    }
}
