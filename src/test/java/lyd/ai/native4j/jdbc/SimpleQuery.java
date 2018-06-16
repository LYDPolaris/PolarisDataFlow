package lyd.ai.native4j.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 */
public class SimpleQuery {

  public static void main(String[] args) throws Exception {
    Class.forName("lyd.ai.native4j.jdbc.ClickHouseDriver");
    Connection connection = DriverManager.getConnection("jdbc:clickhouse://localhost:9000");

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("select  url,host  from white_grey_all_20180613 limit 10;");

    while (rs.next()) {
      System.out.println(rs.getString(1) + "\t" + rs.getString(2));
    }
  }
}
