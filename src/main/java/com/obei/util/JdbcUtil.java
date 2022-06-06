package com.obei.util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import com.obei.commons.ObeiConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {

    public static <T> List<T> queryList(Connection connection, String sql, Class<T> clz, boolean underScoreToCamel) throws Exception {

        //创建集合用于存放查询结果对象
        ArrayList<T> result = new ArrayList<>();

        //编译SQL语句
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        //遍历resultSet,对每行数据封装 T 对象,并将 T 对象添加至集合
        while (resultSet.next()) {

            T t = clz.newInstance();

            for (int i = 1; i < columnCount + 1; i++) {
                String columnName = metaData.getColumnName(i);
                Object value = resultSet.getObject(i);

                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }

                BeanUtils.setProperty(t, columnName, value);
            }

            result.add(t);
        }

        resultSet.close();
        preparedStatement.close();

        //返回结果集
        return result;
    }

    public static void main(String[] args) throws Exception {

        Class.forName(ObeiConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(ObeiConfig.PHOENIX_SERVER);

        System.out.println(queryList(connection,
                "select * from GMALL210625_REALTIME.DIM_BASE_TRADEMARK where id='16'",
                JSONObject.class,
                true));

        connection.close();

    }

}
