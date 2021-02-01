package com.atom.hive.jdbc;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Atom
 */
@Slf4j
public class ListAllTablesUseStatementDemo {

    public static void main(String[] args) throws Exception {
        List<String> tables = new ArrayList<>();
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection connection = DriverManager.getConnection("jdbc:hive2://node1:10000/my_test_hive_db", "", "");
        Statement statement = connection.createStatement();
        String sql = "show tables";
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
            //columnIndex – the first column is 1, the second is 2, ...
            tables.add(resultSet.getString(1));
        }
        tables.forEach(System.out::println);
        resultSet.close();
        connection.close();
    }
}
