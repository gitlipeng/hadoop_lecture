package com.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CustomProducerCallBack {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,1);//重试次数
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,163840);//批次大小
        properties.put(ProducerConfig.LINGER_MS_CONFIG,5);//等待时间
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);//RecordAccumulator缓冲区大小
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String,String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 5000; i++) {
            String key = String.valueOf(i);
//            if(i % 2 == 0){
//                key = "1";
//            }else{
//                key = "3";
//            }
            ProducerRecord producerRecord = new ProducerRecord<String, String>("orderTopic", key, Integer.toString(i));
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception == null){
                        System.out.println(Thread.currentThread().getId() +"," + Thread.currentThread().getName() + "," + "success->" + metadata.offset());
                    }else{
                        exception.printStackTrace();
                    }
                }
            });
        }
        producer.close();
        System.out.println(Thread.currentThread().getId() +"," + Thread.currentThread().getName() + "结束了");
    }
}
