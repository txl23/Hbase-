package com.yc.tasks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


/**
 * @program: Hbase
 * @description:
 * @author: 汤僖龙
 * @create: 2021-08-12 09:19
 */
public class Fruit2FruitMRRunner extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        Job job=Job.getInstance(conf,this.getClass().getSimpleName());
        job.setJarByClass(Fruit2FruitMRRunner.class);
        Scan scan=new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);
        TableMapReduceUtil.initTableMapperJob(
                "fruit2",
                scan,
                ReadFruitMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job
        );
        //设置reducer
        //先在hbase中创建  fruit2_mr的表 create 'fruit2_mr','info'
        TableMapReduceUtil.initTableReducerJob("fruit2_mr",WriteFruitMPReducer.class,job);
        job.setNumReduceTasks(1);
        boolean isSuccess =job.waitForCompletion(true);
        if (!isSuccess){
            throw new IOException("Job running with error");
        }
        return  isSuccess?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf= HBaseConfiguration.create();
        int status= ToolRunner.run(conf,new Fruit2FruitMRRunner(),args);
        System.exit(status);
    }
}
