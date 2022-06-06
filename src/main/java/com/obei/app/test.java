package com.obei.app;

import commons.ObeiConfig;

import java.sql.*;

/**
 * @author qiang
 */
public class test {
  public static void main(String[] args) throws ClassNotFoundException, SQLException {
    Class.forName(ObeiConfig.PHOENIX_DRIVER);
    Connection connection = DriverManager.getConnection(ObeiConfig.PHOENIX_SERVER);
    PreparedStatement preparedStatement = connection.prepareStatement("select  EVENT_ID,ATTR_NAME,COLUMN_NAME from ODS_EVENT_attr where event_id=3");
    ResultSet resultSet = preparedStatement.executeQuery();
    while (resultSet.next()) {
      String s = resultSet.getString(1) + resultSet.getString(2) + resultSet.getString(3);
      System.out.println(s);
    }
    preparedStatement.close();
    connection.close();
  }
}
