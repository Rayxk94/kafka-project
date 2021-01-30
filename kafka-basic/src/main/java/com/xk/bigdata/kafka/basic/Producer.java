package com.xk.bigdata.kafka.basic;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * kafka Producer API
 */
public class Producer {

    public static final String TOPIC = "kafka-test";

    public static void main(String[] args) {
        Properties pro = new Properties();
        // 指定请求的kafka集群列表
        pro.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "bigdatatest01:9092,bigdatatest01:9093,bigdatatest01:94");
        pro.put(ProducerConfig.CLIENT_ID_CONFIG,"test");

        // 当所有的 Kafka borkers 都写入成功之后 返回 ack 包才标识写入成功
        pro.put(ProducerConfig.ACKS_CONFIG, "all");
        // 指定value的序列化方式
        pro.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // 指定key的序列化方式, key是用于存放数据对应的offset
        pro.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // 配置超时时间
        pro.put("request.timeout.ms", "60000");
        // 最大尝试次数
        pro.put("retries",1);
        KafkaProducer<String, String> producer = null;
        try {
            // 创建一个消费者 API的对象
            producer = new KafkaProducer<>(pro);
            for (int i = 0; i < 10000; i++) {
                producer.send(new ProducerRecord<String, String>(TOPIC, String.valueOf(i), "test" + i)).get();
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (null != producer){
                producer.close();
            }
        }


    }

}