package com.obei.app;

import bean.Zgid;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import commons.ObeiConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * @author qiang
 */
public class flinkETL {
  public static void main(String[] args) throws Exception {

    //1.配置flink环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //2.配置Kafka策略，初始化属性
    Properties prop = new Properties();
    prop.setProperty("bootstrap.servers","10.80.79.3:9092");
    prop.setProperty("group.id","test");
    prop.setProperty("auto.offset.reset","latest");
    prop.setProperty("enable.auto.commit","true");
    FlinkKafkaConsumer kafkaSource = new FlinkKafkaConsumer("pay_zg_total", new SimpleStringSchema(), prop);
    //添加Kafka数据源,获取dataStream做etl处理
    DataStreamSource kafkaSteam = env.addSource(kafkaSource);

    Class.forName(ObeiConfig.PHOENIX_DRIVER);
    Connection  connection = DriverManager.getConnection(ObeiConfig.PHOENIX_SERVER);


    //过滤出不是 abp的事件  ，abp没有zg_uid
    //但是只有  abp 才有page_url
    SingleOutputStreamOperator<String> processStream = kafkaSteam.process(new ProcessFunction<String, String>() {
      @Override
      public void processElement(String str, Context context, Collector<String> out) throws Exception {
        try {
          JSONObject data = JSON.parseObject(str).getJSONArray("data").getJSONObject(0);
          if (data.getString("app_id") != "5")
            out.collect(str);

        } catch (Exception ignored) {

        }
      }
    });

    SingleOutputStreamOperator<Zgid> mapStream = processStream.map(new MapFunction<String, Zgid>() {
      @Override
      public Zgid map(String str) throws Exception {
        JSONObject jsonObject = JSON.parseObject(str);
        JSONObject nObject = jsonObject.getJSONArray("data").getJSONObject(0).getJSONObject("pr");
        String zg_id = nObject.getString("$zg_did");//zg_id
        String session_id = nObject.getString("$sid");//session_id
        String uuid = nObject.getString("$uuid");//uuid
        String zg_eid = nObject.getString("$zg_eid");
        String begin_date = nObject.getString("$ct");
        String device_id = nObject.getString("$zg_did");
        String user_id = nObject.getString("$zg_uid");
        String event_name = nObject.getString("$eid");
        String platform = nObject.getString("plat");
        String useragent = nObject.getString("ua");
        String website = nObject.getString("url");
        String current_url = nObject.getString("url");
        String referrer_url = "";
        String channel = "";
        String app_version = "";
        String ip = jsonObject.getString("ip");
        String country = "";
        String area = "";
        String city = "";
        String os = "";
        String ov = "";
        String bs = "";
        String bv = "";
        String utm_source = "";
        String utm_medium = "";
        String utm_campaign = "";
        String tm_term = "";
        String duration = ""; //会话持续时间暂时未知
        String attr5 = device_id +"_"+ session_id ; //会话持续时间暂时未知

//        connection.prepareStatement()

        return new Zgid();
      }
    });

    //执行
    env.execute();
  }
}
