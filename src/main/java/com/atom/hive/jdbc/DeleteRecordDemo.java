package com.atom.hive.jdbc;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * hive 不支持删除和更新
 * <pre>
 * throw exception
 * Caused by: org.apache.hive.service.cli.HiveSQLException: Error while compiling statement: FAILED: SemanticException [Error 10294]:
 * Attempt to do update or delete using transaction manager that does not support these operations.
 * </pre>
 *
 * @author Atom
 */
@Slf4j
public class DeleteRecordDemo {

    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection connection = DriverManager.getConnection("jdbc:hive2://node1:10000/my_test_hive_db", "", "");
        PreparedStatement ppst = connection.prepareStatement("delete from my_test_hive_db.users");
        ResultSet rs = ppst.executeQuery();
        rs.close();
        ppst.close();
        connection.close();
    }
}
