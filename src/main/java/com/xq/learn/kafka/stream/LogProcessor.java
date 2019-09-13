package com.xq.learn.kafka.stream;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * 日志处理器，用于构建kafka stream的拓扑结构时，指定processor
 * @author xiaoqiang
 * @date 2019/9/13 15:59
 */
public class LogProcessor implements Processor<byte[], byte[]>
{
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext)
    {
        this.context = processorContext;
    }

    @Override
    public void process(byte[] dummy, byte[] bytesValue)
    {
        // 在这里处理消息，将处理后的消息输出，输出的流向由拓扑结构决定
        String value = new String(bytesValue, StandardCharsets.UTF_8);
        if (value.contains(">>>"))
        {
            value = value.split(">>>")[1];
        }
        System.out.println("清洗数据。。。");
        context.forward(dummy, value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close()
    {

    }
}
