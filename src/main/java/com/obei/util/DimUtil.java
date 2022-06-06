package com.obei.util;

import com.alibaba.fastjson.JSONObject;
import com.obei.commons.ObeiConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

public class DimUtil {

    public static List<JSONObject> getDimInfo(Connection connection, String key) throws Exception {

        //查询Redis数据
      /*  Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + table + ":" + key;
        String jsonStr = jedis.get(redisKey);
        if (jsonStr != null) {
            //重置过期时间
            jedis.expire(redisKey, 24 * 60 * 60);
            //归还连接
            jedis.close();
            //返回结果数据
            return JSON.parseObject(jsonStr);
        }*/

        //构建查询语句
        String querySql = "select EVENT_ID,ATTR_NAME,COLUMN_NAME from ODS_EVENT_attr"+" where EVENT_ID=" + key ;
        System.out.println("QuerySql:" + querySql);

        //执行查询
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);

        //将数据写入Redis
//        JSONObject dimInfo = queryList.get(0);
       /* jedis.set(redisKey, dimInfo.toJSONString());
        //设置过期时间
        jedis.expire(redisKey, 24 * 60 * 60);
        //归还连接
        jedis.close();*/

        //返回结果
        return queryList;
    }

    //删除Redis中的数据
/*    public static void deleteDimInfo(String table, String key) {

        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + table + ":" + key;

        jedis.del(redisKey);

        jedis.close();
    }*/

    public static void main(String[] args) throws Exception {

        Class.forName(ObeiConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(ObeiConfig.PHOENIX_SERVER);

        long start = System.currentTimeMillis();
        System.out.println(getDimInfo(connection,  "16"));  //240
        long end = System.currentTimeMillis();
        System.out.println(getDimInfo(connection,  "16"));  //10
        long end2 = System.currentTimeMillis();

        System.out.println(end - start);
        System.out.println(end2 - end);

        connection.close();

    }

}
