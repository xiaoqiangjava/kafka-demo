package com.xq.learn.kafka.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
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
        // 指定拦截器，拦截器指定时需要指定类的全限定名称，可以指定多个拦截器，因为拦截器调用是一个调用链
        List<String> interceptors = new ArrayList<>();
        interceptors.add("com.xq.learn.kafka.interceptor.TimeInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
        // 创建生产者
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++)
        {
            String value = "INFO:>>>message: " + i;
            producer.send(new ProducerRecord<>("first", i + "", value), (metadata, exception) -> System.out.println("partition:offset-->" + metadata.partition() + "-->" + metadata.offset()));
        }
        producer.close();
    }
}
