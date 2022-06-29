package com.atom.hive.jdbc;

import lombok.extern.slf4j.Slf4j;

import java.sql.*;

/**
 * @author Atom
 */
@Slf4j
public class TableSelectDemo {

    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");


        Connection connection = DriverManager.getConnection("jdbc:hive2://xxx:10000/default?mapreduce.job.queuename=default;hive.cli.print.header=false", "", "");

//        Connection connection = DriverManager.getConnection("jdbc:hive2://xxx:10000/default", "root", "123456");
        PreparedStatement ppst = connection.prepareStatement("select 1");
        ResultSet rs = ppst.executeQuery();
        while (rs.next()) {
            String aa = rs.getString("aa");
            String bb = rs.getString("bb");
            String cc = rs.getString("cc");
            log.info("aa->" + aa + "\tbb->" + bb + "\tcc->" + cc);
        }

        DatabaseMetaData metaData = connection.getMetaData();
        rs = metaData.getPrimaryKeys("default", "default", "my_table9");
        while (rs.next()) {
            String pk = rs.getString("COLUMN_NAME");
            System.err.println("PK->" + pk);
        }
        rs.close();
        ppst.close();
        connection.close();
    }
}
