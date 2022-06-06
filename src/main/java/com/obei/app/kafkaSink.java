package com.obei.app;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.commons.nullanalysis.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @author qiang
 */
public class kafkaSink {
  public static void main(String[] args) throws Exception {
    //1.获取流的执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    System.setProperty("HADOOP_USER_NAME", "obeiadmin");
    DataStreamSource<String> stringDataStream = env.readTextFile("C:\\Users\\liuqiang\\Desktop\\zhuge.txt");

    SingleOutputStreamOperator<String> processStream = stringDataStream.process(new ProcessFunction<String, String>() {
      @Override
      public void processElement(String s, Context context, Collector<String> out) throws Exception {
        try {
          JSONObject jsonObject = JSON.parseObject(s);
          if (jsonObject.getInteger("app_id") == 5)
            out.collect(s);

        } catch (Exception ignored) {

        }
      }
    });

//    FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>("localhost:9092", "pay_zg_total ", new SimpleStringSchema());

//    processStream.addSink(kafkaProducer);

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "10.80.79.3:9092");
    KafkaSerializationSchema<String> serializationSchema = new KafkaSerializationSchema<String>() {
      @Override
      public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
        return new ProducerRecord<>(
            "pay_zg_total", // target topic
            element.getBytes(StandardCharsets.UTF_8)); // record contents
      }
    };

    FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
        "pay_zg_total",             // target topic
        serializationSchema,    // serialization schema
        properties,             // producer config
        FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // fault-tolerance

    processStream.addSink(myProducer);

    env.execute();
  }
}
