package com.atom.hive.jdbc;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Atom
 */
@Slf4j
public class ListAllTablesUsePrepareStatementDemo {

    public static void main(String[] args) throws Exception {
        List<String> tables = new ArrayList<>();
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection connection = DriverManager.getConnection("jdbc:hive2://10.16.118.247:10000/mydb", "", "");
        PreparedStatement ppst = connection.prepareStatement("show tables");
        ResultSet rs = ppst.executeQuery();
        while (rs.next()) {
            tables.add(rs.getString(1));
        }
        tables.forEach(System.err::println);
        rs.close();
        ppst.close();
        connection.close();
    }
}
