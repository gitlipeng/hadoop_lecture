package com.test.inputformt;

import com.test.wordcount.WcDriver;
import com.test.wordcount.WcMapper;
import com.test.wordcount.WcReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WholeFileDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        // 1 获取一个Job实例
        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job.setJarByClass(WholeFileDriver.class);

        // 4 设置mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class);

        // 6 设置输入和输出数据
        FileInputFormat.setInputPaths(job, new Path("d:\\input"));
        FileOutputFormat.setOutputPath(job, new Path("d:\\output"));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
