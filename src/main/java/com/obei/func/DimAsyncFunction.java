package com.obei.func;

import com.alibaba.fastjson.JSONObject;
import com.obei.commons.ObeiConfig;
import com.obei.util.DimUtil;
import com.obei.util.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;
/*
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }*/

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(ObeiConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(ObeiConfig.PHOENIX_SERVER);

        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {

        threadPoolExecutor.execute(new Runnable() {
            @Override
            public void run() {

                String key = getKey(input);

                try {
                    //读取维度信息
                    List<JSONObject> queryList = DimUtil.getDimInfo(connection,  key);

                    //将维度信息补充至数据
                    if (queryList != null) {
                        join(input, queryList);
                    }

                    //将补充完成的数据写出
                    resultFuture.complete(Collections.singletonList(input));

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut:" + input);
    }
}
