package com.test.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MJMapepper extends Mapper<LongWritable,Text,Text,NullWritable> {

    private Map<String,String> pMap = new HashMap<>();

    private Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFile = context.getCacheFiles();
        String path = cacheFile[0].getPath().toString();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
        String line;
        while(StringUtils.isNotEmpty(line = bufferedReader.readLine())){
            String[] fields = line.split("\t");
            pMap.put(fields[0],fields[1]);
        }

        IOUtils.closeStream(bufferedReader);

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fileds = value.toString().split("\t");
        String pname = pMap.get(fileds[1]);
        k.set(fileds[0] +"\t" + pname + "\t" + fileds[2]);

        // 系统计数器
        context.getCounter("map", "true").increment(1);

        context.write(k,NullWritable.get());
    }
}
