package com.obei.test;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author qiang
 */
public class consume {
  public static void main(String[] args) throws Exception {
    //1.配置flink环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //2.配置Kafka策略，初始化属性
    Properties prop = new Properties();
    prop.setProperty("bootstrap.servers","10.80.79.3:9092");
    prop.setProperty("group.id","test1");
    prop.setProperty("auto.offset.reset","earliest");
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    FlinkKafkaConsumer kafkaSource = new FlinkKafkaConsumer("pay_zg_total", new SimpleStringSchema(), prop);
    //添加Kafka数据源,获取dataStream做etl处理
    DataStreamSource kafkaSteam = env.addSource(kafkaSource);
    kafkaSteam.print();
    env.execute();
  }
}
