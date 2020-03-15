package com.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.Properties;

public class JedisPoolUtils {
    private static JedisPool pool = null;

    static {
        Properties properties = new Properties();
        try {
            properties.load(JedisPoolUtils.class.getClassLoader().getResourceAsStream("redis.properties"));

            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxIdle(Integer.parseInt(properties.getProperty("redis.maxidle").toString()));//最大闲置个数 ，超过这么多闲置的就关
            config.setMinIdle(Integer.parseInt(properties.getProperty("redis.minidle").toString()));//最小闲置个数
            config.setMaxTotal(Integer.parseInt(properties.getProperty("redis.maxtotal").toString()));//最大连接数

            //1.创建一个redis连接池
            pool = new JedisPool(config,properties.getProperty("redis.url").toString(), Integer.parseInt(properties.getProperty("redis.port").toString()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Jedis getJedis(){
        return pool.getResource();
    }


}