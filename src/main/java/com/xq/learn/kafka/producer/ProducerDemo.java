package com.xq.learn.kafka.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * kafka 生产者api
 *
 * @author xiaoqiang
 * @date 2019/9/10 2:04
 */
public class ProducerDemo
{
    public static void main(String[] args)
    {
        Properties props = new Properties();
        // 配置broker地址
        props.put("bootstrap.servers", "learn:9092");
        // 应答策略
        props.put("acks", "all");
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        // key,value序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 指定自定义分区类
        props.put("partitioner.class", "com.xq.learn.kafka.producer.CustomPartition");
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++)
        {
            producer.send(new ProducerRecord<>("first", i + "", i + ""), (metadata, exception) -> System.out.println("partition:offset-->" + metadata.partition() + "-->" + metadata.offset()));
        }
        producer.close();
    }
}
