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
//        Connection connection = DriverManager.getConnection("jdbc:hive2://xxx:10000/default?mapreduce.job.queuename=default;hive.cli.print.header=false", "", "");
        Connection connection = DriverManager.getConnection("jdbc:hive2://xx:10000/default", "", "");
        PreparedStatement ppst = connection.prepareStatement("create table default.table003(id int,name string,age int )");
        ppst.execute();
        ppst.close();
        connection.close();
    }
}
