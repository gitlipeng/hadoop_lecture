package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class CustomConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        props.put(ConsumerConfig.GROUP_ID_CONFIG,"test3");//消费者组，只要group.id相同，就属于同一个消费者组
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");//自动提交offset

        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,60000000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,100);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("orderTopic"));
        int i = 0;
        int j = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                j++;
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
            if(records.count() > 0){
                i++;
                System.out.println("总循环次数：==========================" + i +",拿到的数据个数：" + j);
                j=0;
            }
            consumer.commitSync();
        }
    }

}
