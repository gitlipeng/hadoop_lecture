package com.test.join;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class RJMapper extends Mapper<LongWritable,Text,TableBean,NullWritable> {
    private Path path;

    TableBean tableBean = new TableBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        path = fileSplit.getPath();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String pathName = path.getName();
        String[] fileds = value.toString().split("\t");
        if(pathName.equals("order.txt")){
            tableBean.setOrder_id(fileds[0]);
            tableBean.setP_id(fileds[1]);
            tableBean.setAmount(Integer.valueOf(fileds[2]));
            tableBean.setPname("");
            tableBean.setFlag("");
        }else{
            tableBean.setP_id(fileds[0]);
            tableBean.setPname(fileds[1]);
            tableBean.setFlag("");
            tableBean.setAmount(0);
            tableBean.setOrder_id("");
        }
        context.write(tableBean,NullWritable.get());
    }
}
