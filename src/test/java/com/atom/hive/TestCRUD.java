package com.atom.hive;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Atom
 */
public class TestCRUD {

    private Connection connection;

    @Before
    public void initConnection() throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        connection = DriverManager.getConnection("jdbc:hive2://node1:10000/my_test_hive_db", "", "");

    }

    @Test
    public void testInsert() throws SQLException {
        PreparedStatement ppst = connection.prepareStatement("create table my_test_hive_db.users(id int,name string,age int )");
        ppst.execute();
        ppst.close();
        connection.close();
    }

    @Test
    public void testBatchInsert() throws SQLException {
        PreparedStatement ppst = connection.prepareStatement("insert into my_test_hive_db.users(id,name,age) values(?,?,?)");
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


    }


    /**
     * hive 不支持删除和更新
     *
     * @throws SQLException
     */
    @Test
    public void testDelete() throws SQLException {
        PreparedStatement ppst = connection.prepareStatement("delete from my_test_hive_db.users");
        ppst.execute();
        ppst.close();
        connection.close();
    }

    /**
     * hive 不支持删除和更新
     *
     * @throws SQLException
     */
    @Test
    public void testUpdate() throws SQLException {
        PreparedStatement ppst = connection.prepareStatement("update my_test_hive_db.users set age = 99 where id = 10");
        ppst.execute();
        ppst.close();
        connection.close();
    }

}
