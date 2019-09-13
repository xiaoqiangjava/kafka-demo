package com.xq.learn.kafka.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * 重复读取数据，需要设置offset的值，在kafka2.x版本中，重新读取数据的api得到了很大的简化，只需要
 * 构建一个TopicPartition类，指定需要重复读取的topic和partition，然后使用assign方法指定一个分区给当前消费者
 * 最后使用seek方法指定offset值。
 * @author xiaoqiang
 * @date 2019/9/13 13:43
 */
public class ReConsumerDemo
{
    public static void main(String[] args)
    {
        // consumer 配置
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "learn:9092");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // 创建consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        // 指定一个partition，让当前消费者只消费指定分区的数据
        consumer.assign(Collections.singletonList(new TopicPartition("first", 0)));
        // 指定offset，用来重复读取已经读取过的数据
        consumer.seek(new TopicPartition("first", 0), 835);
        while (true)
        {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records)
            {
                System.out.println("topic: " + record.topic()
                        + "--partition：" + record.partition()
                        + "--value: " + record.value()
                        + "--offset: " + record.offset());
            }
        }
    }
}
