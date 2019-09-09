package com.xq.learn.kafka.producer;

import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * 自定义分区
 * @author xiaoqiang
 * @date 2019/9/10 2:26
 */
public class CustomPartition implements Partitioner
{
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster)
    {
        return 0;
    }

    @Override
    public void close()
    {

    }

    @Override
    public void configure(Map<String, ?> configs)
    {

    }
}
