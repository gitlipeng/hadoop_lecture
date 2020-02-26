package com.test.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * LongWritable:偏移量
 * Text 文本
 */
public class WcMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    Text text = new Text();
    IntWritable intWritable = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 拿到一行数据
        String line = value.toString();

        // 切分单词
        String[] words = line.split(" ");

        for(String word : words){
            this.text.set(word);
            context.write(this.text,this.intWritable);
        }
    }
}
