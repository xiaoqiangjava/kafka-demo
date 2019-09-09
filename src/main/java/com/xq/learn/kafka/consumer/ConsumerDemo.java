package com.xq.learn.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * kafka消费者api
 * @author xiaoqiang
 * @date 2019/9/10 2:35
 */
public class ConsumerDemo
{
    public static void main(String[] args)
    {
        Properties props = new Properties();
        // broker配置
        props.put("bootstrap.servers", "learn:9092");
        // consumer group
        props.put("group.id", "test");
        // 自动提交offset值，在指定的时间
        props.put("enable.auto.commit", "true");
        // 提交延时
        props.put("auto.commit.interval.ms", "1000");
        // key,value序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅那个topic
        consumer.subscribe(Arrays.asList("first", "second"));
        // fetch数据
        while (true)
        {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records)
            {
                System.out.println("topic: " + record.topic() + "--partition：" + record.partition() + "--value: " + record.value());
            }
        }
    }
}
