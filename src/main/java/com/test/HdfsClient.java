package com.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient{
    @Test
    public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        // configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
        // FileSystem fs = FileSystem.get(configuration);

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "lipeng");

        // 2 创建目录
        fs.mkdirs(new Path("/1109/daxian/banzhang"));

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void put() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "lipeng");

        fs.copyFromLocalFile(new Path("d://软件//jdk-8u231-linux-x64.tar.gz"),new Path("/"));

        fs.close();
    }


    @Test
    public void ls() throws IOException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "lipeng");

        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for(FileStatus fileStatus : fileStatuses){
            if(fileStatus.isFile()){
                System.out.println("文件" + fileStatus.getPath() +"," + fileStatus.getLen());
            }else{
                System.out.println("文件夹" + fileStatus.getPath());
            }
        }
    }

    @Test
    public void testListFiles() throws IOException, InterruptedException, URISyntaxException{
        // 1获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "lipeng");

        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while(listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();

            // 输出详情
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 分组
            System.out.println(status.getGroup());
            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {
                // 获取块存储的主机节点
                System.out.println("块在");
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("-----------班长的分割线----------");
        }

        // 3 关闭资源
        fs.close();
    }

}