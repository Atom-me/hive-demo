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

        Connection connection = DriverManager.getConnection("jdbc:hive2://xxxx:10000/default", "root", "123456");
        PreparedStatement ppst = connection.prepareStatement("insert into default.my_table9(aa,bb,cc) values(?,?,?)");

        for (int i = 1; i < 10; i++) {
            ppst.setInt(1, i);
            ppst.setInt(2, 10 + i);
            ppst.setString(3, "atom" + i);
            ppst.executeUpdate();
        }

//        ppst.setInt(1, 1);
//        ppst.setString(2, "atom");
//        ppst.setInt(3, 10);
//        ppst.executeUpdate();
//
//        ppst.setInt(1, 1);
//        ppst.setString(2, "张三");
//        ppst.setInt(3, 15);
//        ppst.executeUpdate();
//
//        ppst.setInt(1, 1);
//        ppst.setString(2, "里斯");
//        ppst.setInt(3, 12);
//        ppst.executeUpdate();

        ppst.close();
        connection.close();
        ppst.close();
        connection.close();
    }
}
