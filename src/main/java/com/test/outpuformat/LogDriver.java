package com.test.outpuformat;

import com.test.phone.FlowBean;
import com.test.phone.FlowCountMapper;
import com.test.phone.FlowCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {

    public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {

        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[] { "D:/学习/大数据/011_Hadoop/testData/log.txt"};

        String[] output = new String[] {"D:/学习/大数据/011_Hadoop/testData/output2"};

        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 6 指定本程序的jar包所在的本地路径
        job.setJarByClass(LogDriver.class);

        job.setOutputFormatClass(LogOutputFormat.class);


        // 5 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(output[0]));

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}