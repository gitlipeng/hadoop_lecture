package com.test.outpuformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<LongWritable,Text> {
    FSDataOutputStream atguiguOut ;
    FSDataOutputStream otherOut ;

    LogRecordWriter(TaskAttemptContext job) throws IOException {
        FileSystem fs = FileSystem.get(job.getConfiguration());

        atguiguOut = fs.create(new Path("D:/学习/大数据/011_Hadoop/testData/output2/atguigu.log"));
        otherOut = fs.create(new Path("D:/学习/大数据/011_Hadoop/testData/output2/other.log"));
    }


    @Override
    public void write(LongWritable key, Text value) throws IOException, InterruptedException {
        String line = value.toString() + "\n";
        byte [] bytes = line.getBytes();
        if(line.contains("atguigu")){
            atguiguOut.write(bytes);
        }else{
            otherOut.write(bytes);
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        // 关闭资源
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
    }
}
