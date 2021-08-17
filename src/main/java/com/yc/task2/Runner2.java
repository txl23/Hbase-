package com.yc.task2;

import com.yc.tasks.Fruit2FruitMRRunner;
import com.yc.tasks.ReadFruitMapper;
import com.yc.tasks.WriteFruitMPReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program: Hbase
 * @description:
 * @author: 汤僖龙
 * @create: 2021-08-12 16:33
 */
public class Runner2 extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        //创建job任务
        Job job=Job.getInstance(conf,this.getClass().getSimpleName());
        job.setJarByClass(Runner2.class);
        Path inPath=new Path("hdfs://yc/input_fruit2/fruit.tsv");
        FileInputFormat.addInputPath(job,inPath);
        //设置Mapper
        job.setMapperClass(ReadFruitFromHDFSMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputKeyClass(Put.class);
        //设置ruducer
        TableMapReduceUtil.initTableReducerJob("fruit2_mr",WriteFruitMRFromTxtReducer.class,job);
        //设置reducer数量最少一个
        job.setNumReduceTasks(1);
        boolean isSuccess =job.waitForCompletion(true);
        if (!isSuccess){
            throw new IOException("Job running with error");
        }
        return  isSuccess?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf= HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","node1:2181,node2:2181,node3:2181");
        int status= ToolRunner.run(conf,new Runner2(),args);
        System.exit(status);
    }
}
