package com.obei.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.obei.bean.Zgid;
import com.obei.func.DimAsyncFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author qiang
 */
public class flinkETL {
  public static void main(String[] args) throws Exception {

    //1.配置flink环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //2.配置Kafka策略，初始化属性
    //2.配置Kafka策略，初始化属性
    Properties prop = new Properties();
    prop.setProperty("bootstrap.servers","10.80.79.3:9092");
    prop.setProperty("group.id","te2131");
    prop.setProperty("auto.offset.reset","earliest");
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    FlinkKafkaConsumer kafkaSource = new FlinkKafkaConsumer("pay_zg_total", new SimpleStringSchema(), prop);
    //添加Kafka数据源,获取dataStream做etl处理
    DataStreamSource kafkaSteam = env.addSource(kafkaSource);



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
        Integer zg_id = nObject.getInteger("$zg_did");//zg_id
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
        String utm_content = "";
        String utm_term = "";
        String duration = ""; //会话持续时间暂时未知
        String attr5 = device_id +"_"+ session_id ;
        String cus1 = "" ;
        String cus2 = "" ;
        String cus3 = "" ;
        String cus4 = "" ;
        String cus5 = "" ;
        String cus6 = "" ;
        String cus7 = "" ;
        String cus8 = "" ;
        String cus9 = "" ;
        String cus10 = "" ;
        String cus11 = "" ;
        String cus12 = "" ;
        String cus13 = "" ;
        String cus14 = "" ;
        String cus15 = "" ;


        return new Zgid(zg_id,session_id,uuid,zg_eid,begin_date,device_id,user_id,event_name,
                        platform,useragent,website,current_url,referrer_url,channel,app_version,ip,country,city,os,ov,bs,bv,
                        utm_source,utm_medium,utm_campaign,utm_content,utm_term,attr5,duration,attr5,cus1,cus2 ,cus3 ,cus4, cus5, cus6, cus7, cus8, cus9, cus10,cus11,cus12,cus13,cus14,cus15,nObject.toJSONString());
      }
    });



    //异步关联cus
    SingleOutputStreamOperator<Zgid> orderWideSingleOutputStreamOperator =
        AsyncDataStream.unorderedWait(mapStream, new DimAsyncFunction<Zgid>() {
                                        @Override
                                        public String getKey(Zgid input) {
                                          return input.getEVENT_ID();
                                        }

                                        @Override
                                        public void join(Zgid input, List<JSONObject> queryList) throws ParseException {
                                          //存放结果集的map
                                          Map<String, String> map = new HashMap<>();
                                          for (int i = 0; i < queryList.size(); i++) {
                                            JSONObject jsonObject = queryList.get(i);
                                            String attr_name = "&zg_epid#_" + jsonObject.getString("ATTR_NAME");

                                            String key = jsonObject.getString("COLUMN_NAME");
                                            String value = JSONObject.parseObject(input.getJSON_PR()).getString(attr_name);
                                            map.put(key, value);
                                          }
                                          Set<String> set=map.keySet();
                                          for(String key:set){
                                            String value=map.get(key);
                                            switch (key) {
                                              case "cus1":
                                               input.setCUS1(value);
                                                break;
                                              case "cus2":
                                                input.setCUS2(value);
                                                break;
                                              case "cus3":
                                                input.setCUS3(value);
                                                break;
                                              case "cus4":
                                                input.setCUS4(value);
                                                break;
                                              case "cus5":
                                                input.setCUS5(value);
                                                break;
                                              case "cus6":
                                                input.setCUS6(value);
                                                break;
                                              case "cus7":
                                                input.setCUS7(value);
                                                break;
                                              case "cus8":
                                                input.setCUS8(value);
                                                break;
                                              case "cus9":
                                                input.setCUS9(value);
                                                break;
                                              case "cus10":
                                                input.setCUS10(value);
                                                break;
                                              case "cus11":
                                                input.setCUS11(value);
                                                break;
                                              case "cus12":
                                                input.setCUS12(value);
                                                break;
                                              case "cus13":
                                                input.setCUS13(value);
                                                break;
                                              case "cus14":
                                                input.setCUS14(value);
                                                break;
                                              default:
                                                input.setCUS15(value);
                                                break;
                                            }
                                        }

                                        }}, 10L, TimeUnit.SECONDS
        );
    orderWideSingleOutputStreamOperator.print();



    //执行
    env.execute();




    /*

    //列名和值的集合
    Map<String, String> map = new HashMap<>();

    PreparedStatement preparedStatement = connection.prepareStatement("select  EVENT_ID,ATTR_NAME,COLUMN_NAME from ODS_EVENT_attr ");
    ResultSet resultSet = preparedStatement.executeQuery();
//        $zg_epid#_  + 对应的 ATTR_NAME 作为key
    while (resultSet.next()) {
      String column_name = resultSet.getString(3);
      String attr_name = resultSet.getString(2);

      String vaule = nObject.getString("&zg_epid#_" + attr_name);
      map.put(column_name,vaule);
    }
    preparedStatement.close();
    //遍历这个map拿到所有的列名和值
    Set<String> set=map.keySet();
    for(String key:set){
      String value=map.get(key);
      switch (key) {
        case "cus1":
          cus1 = value;
          break;
        case "cus2":
          cus2 = value;
          break;
        case "cus3":
          cus3 = value;
          break;
        case "cus4":
          cus4 = value;
          break;
        case "cus5":
          cus5 = value;
          break;
        case "cus6":
          cus6 = value;
          break;
        case "cus7":
          cus7 = value;
          break;
        case "cus8":
          cus8 = value;
          break;
        case "cus9":
          cus9 = value;
          break;
        case "cus10":
          cus10 = value;
          break;
        case "cus11":
          cus11 = value;
          break;
        case "cus12":
          cus12 = value;
          break;
        case "cus13":
          cus13 = value;
          break;
        case "cus14":
          cus14 = value;
          break;
        default:
          cus15 = value;
          break;
      }



    }*/
  }
}
