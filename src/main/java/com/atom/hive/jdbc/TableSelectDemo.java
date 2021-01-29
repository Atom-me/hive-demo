package com.atom.hive.jdbc;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author Atom
 */
@Slf4j
public class TableSelectDemo {

    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");

        Connection connection = DriverManager.getConnection("jdbc:hive2://node1:10000/my_test_hive_db", "", "");
        PreparedStatement ppst = connection.prepareStatement("select * from score");
        ResultSet rs = ppst.executeQuery();
        while (rs.next()) {
            String sno = rs.getString("sno");
            String cno = rs.getString("cno");
            int degree = rs.getInt("degree");

            log.info("sno->" + sno + "\tcno->" + cno + "\tdegree->" + degree);

        }
        rs.close();
        ppst.close();
        connection.close();
    }
}
