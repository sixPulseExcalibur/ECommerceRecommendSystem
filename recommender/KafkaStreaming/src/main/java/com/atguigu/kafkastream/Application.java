package com.atguigu.kafkastream;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: ECommerceRecommendSystem
 * Package: com.atguigu.kafkastream
 * Version: 1.0
 * <p>
 * Created by wushengran on 2019/4/28 14:59
 */

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * @ClassName: Application
 * @Description:
 * @Author: wushengran on 2019/4/28 14:59
 * @Version: 1.0
 */
public class Application {
    public static void main(String[] args) {
        String brokers = "hadoop01:9092,hadoop02:9092,hadoop03:9092";
        String zookeepers = "hadoop01:2181,hadoop02:2181,hadoop03:2181";

        // 定义输入和输出的topic
        String from = "log";
        String to = "recommender";

        // 定义kafka stream 配置参数
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);

        // 创建kafka stream 配置对象
        StreamsConfig config = new StreamsConfig(settings);

        // 定义拓扑构建器 ,就是构建kafka的流式处理管道
        TopologyBuilder builder = new TopologyBuilder();
        //from  就是前面定义的log 主题
        //to  就是前面定义的recommender 主题
        //()->new LogProcessor()  lamda表达式
        builder.addSource("SOURCE", from)
                .addProcessor("PROCESSOR", ()->new LogProcessor(), "SOURCE")
                .addSink("SINK", to, "PROCESSOR");

        // 创建kafka stream
        KafkaStreams streams = new KafkaStreams( builder, config );

        streams.start();
        System.out.println("kafka stream started!");
    }
}
