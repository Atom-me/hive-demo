package com.atom.hive.jdbc;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author Atom
 */
@Slf4j
public class InsertUseExecuteUpdateDemo {

    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");

        Connection connection = DriverManager.getConnection("jdbc:hive2://node1:10000/my_test_hive_db", "", "");
        PreparedStatement ppst = connection.prepareStatement("insert into my_test_hive_db.table001(id,name,age) values(?,?,?)");
        ppst.setInt(1, 1);
        ppst.setString(2, "atom");
        ppst.setInt(3, 10);
        ppst.executeUpdate();

        ppst.setInt(1, 1);
        ppst.setString(2, "张三");
        ppst.setInt(3, 15);
        ppst.executeUpdate();

        ppst.setInt(1, 1);
        ppst.setString(2, "里斯");
        ppst.setInt(3, 12);
        ppst.executeUpdate();

        ppst.close();
        connection.close();
        ppst.close();
        connection.close();
    }
}
