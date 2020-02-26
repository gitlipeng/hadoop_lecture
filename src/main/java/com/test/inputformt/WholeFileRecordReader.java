package com.test.inputformt;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
    处理一个文件；把这个文件读取成一个KV值
 */
public class WholeFileRecordReader extends RecordReader<Text,BytesWritable> {
    private boolean notRead = true;

    private Text key = new Text();
    private BytesWritable value = new BytesWritable();

    private FSDataInputStream inputStream;
    private FileSplit fs;

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        fs = (FileSplit) split;
        Path path = fs.getPath();
        FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
        inputStream = fileSystem.open(path);
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(notRead){
            // 具体读文件的过程
            notRead = false;

            byte[] bytes = new byte[(int) fs.getLength()];
            inputStream.read(bytes);

            key.set(fs.getPath().toString());
            value.set(bytes,0,bytes.length);
            return true;
        }else{
            return false;
        }
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return notRead ? 0 : 1;
    }

    public void close() throws IOException {
        IOUtils.closeStream(inputStream);
    }
}
