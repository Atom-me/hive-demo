package com.atom.hive.jdbc;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author Atom
 */
@Slf4j
public class CreateTableDemo {

    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection connection = DriverManager.getConnection("jdbc:hive2://node1:10000/my_test_hive_db", "", "");
        PreparedStatement ppst = connection.prepareStatement("create table my_test_hive_db.table001(id int,name string,age int )");
        ppst.execute();
        ppst.close();
        connection.close();
    }
}
