package com.test.join;

import com.test.wordcount.WcMapper;
import com.test.wordcount.WcReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RJDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[] { "D:/学习/大数据/011_Hadoop/testData/order.txt", "d:/学习/大数据/011_Hadoop/testData/pd.txt" };

        Configuration configuration = new Configuration();
        // 1 获取一个Job实例
        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job.setJarByClass(RJDriver.class);

        // 3 设置map和reduce类
		job.setMapperClass(RJMapper.class);
		job.setReducerClass(RJReducer.class);
		// 4 设置mapper输出类型
        job.setMapOutputKeyClass(TableBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(RJComparator.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 6 设置输入和输出数据
        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path("D:/学习/大数据/011_Hadoop/testData/output1"));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}
