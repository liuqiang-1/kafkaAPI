package com.obei.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;
import java.util.List;

public interface DimJoinFunction<T> {

    String getKey(T input);

    void join(T input, List<JSONObject> dimInfo) throws ParseException;

}
