package com.xq.learn.kafka.stream;

import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

/**
 * kafka stream, 构建一个拓扑结构，最终的数据流向是从一个topic订阅数据，然后
 * 使用kafka stream（Processor）清洗数据，再将数据发送到另一个topic供其他消费者消费
 * @author xiaoqiang
 * @date 2019/9/13 15:46
 */
public class StreamDemo
{
    public static void main(String[] args)
    {
        // stream配置
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "learn:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "1");

        // 需要创建拓扑结构，指定source，processor，sink
        Topology topology = new Topology();
        topology.addSource("SOURCE", "first");
        topology.addProcessor("PROCESSOR", LogProcessor::new, "SOURCE");
        topology.addSink("SINK", "second", "PROCESSOR");

        // 创建kafka stream
        KafkaStreams streams = new KafkaStreams(topology, props);

        // 启动kafka stream
        streams.start();
        System.out.println("Kafka stream started...");
    }
}
