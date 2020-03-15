package com.redis;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.*;

public class RedisTest {
    Jedis jedis;
    @Before
    public void init(){
        jedis = JedisPoolUtils.getJedis();
    }

    @After
    public void close(){
        //4.关闭资源
        jedis.close();
    }

    @Test
    public void testKey(){
        //key
        Set<String> keys = jedis.keys("*");
        for (Iterator iterator = keys.iterator(); iterator.hasNext();) {
            String key = (String) iterator.next();
            System.out.println(key);
        }
        System.out.println("jedis.exists====>"+jedis.exists("name"));
        System.out.println(jedis.ttl("name"));
    }


    @Test
    public void testString(){
        System.out.println(jedis.get("k1"));
        jedis.set("k4","k4_Redis");
        System.out.println("----------------------------------------");
        jedis.mset("str1","v1","str2","v2","str3","v3");
        System.out.println(jedis.mget("str1","str2","str3"));
    }


    @Test
    public void testList(){
        jedis.lpush("mylist", "1","2","3");
        List<String> list = jedis.lrange("mylist",0,-1);
        for (String element : list) {
            System.out.println(element);
        }

    }


    @Test
    public void testSet(){
        jedis.sadd("orders","jd001");
        jedis.sadd("orders","jd002");
        jedis.sadd("orders","jd003");
        Set<String> set1 = jedis.smembers("orders");
        for (Iterator iterator = set1.iterator(); iterator.hasNext();) {
            String string = (String) iterator.next();
            System.out.println(string);
        }
        jedis.srem("orders","jd002");

    }


    @Test
    public void testHash(){
        jedis.hset("user_lisi","userName","lisi");
        System.out.println(jedis.hget("user_lisi","userName"));
        Map<String,String> map = new HashMap<String,String>();
        map.put("telphone","13810169999");
        map.put("address","atguigu");
        map.put("email","abc@163.com");
        map.put("userName","zhangsan");
        jedis.hmset("user_zhangsan",map);
        List<String> result = jedis.hmget("user_zhangsan", "telphone","email");
        for (String element : result) {
            System.out.println(element);
        }

    }


    @Test
    public void testZset(){
        jedis.zadd("zset01",60d,"v1");
        jedis.zadd("zset01",70d,"v2");
        jedis.zadd("zset01",80d,"v3");
        jedis.zadd("zset01",90d,"v4");
        Set<String> s1 = jedis.zrange("zset01",0,-1);
        for (Iterator iterator = s1.iterator(); iterator.hasNext();) {
            String string = (String) iterator.next();
            System.out.println(string);
        }

    }

    @Test
    public void testSendCode(){
        String phoneNum = "15050402756";
        // 随机生成6位数字
        String code = "123456";

        // 存验证码 key: phoneNum+":code"
        String key = phoneNum + ":code";
        String countKey = phoneNum +":sendCount";


        jedis.setex(key,10,code);
        // 存发送次数
        jedis.incrBy(countKey, 1);
    }

    @Test
    public void testGetCode(){

    }



}
