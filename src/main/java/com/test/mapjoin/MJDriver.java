package com.test.mapjoin;

import com.test.join.RJComparator;
import com.test.join.RJMapper;
import com.test.join.RJReducer;
import com.test.join.TableBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class MJDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[] { "D:/学习/大数据/011_Hadoop/testData/order.txt", "d:/学习/大数据/011_Hadoop/testData/pd.txt" };

        Configuration configuration = new Configuration();
        // 1 获取一个Job实例
        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job.setJarByClass(MJDriver.class);

        // 3 设置map和reduce类
		job.setMapperClass(MJMapepper.class);
		// 4 设置最终输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(3);

        // 6 加载缓存数据
		job.addCacheFile(URI.create("file:///D:/学习/大数据/011_Hadoop/testData/pd.txt"));
        // 7 Map端Join的逻辑不需要Reduce阶段，设置reduceTask数量为0
        job.setNumReduceTasks(0);

        // 6 设置输入和输出数据
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("D:/学习/大数据/011_Hadoop/testData/output1"));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}
