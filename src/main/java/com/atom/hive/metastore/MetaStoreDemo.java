package com.atom.hive.metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.List;

/**
 * @author Atom
 */
public class MetaStoreDemo {
    public static void main(String[] args) throws TException {

        HiveConf hiveConf = new HiveConf();
        hiveConf.addResource("hive-site.xml");

        HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);

        //get all dbs
        List<String> allDatabases = client.getAllDatabases();
        allDatabases.forEach(System.out::println);

        //get all tabs by specified db
        List<String> tablesList = client.getAllTables("my_test_hive_db");
        System.out.print("my_test_hive_db数据库中所有的表:  ");
        for (String s : tablesList) {
            System.out.print(s + "\t");
        }
        System.out.println();

        //get specified tab by db name and tab name
        System.out.println("tmy_test_hive_db.course 表信息: ");
        Table table = client.getTable("my_test_hive_db", "course");
        List<FieldSchema> fieldSchemaList = table.getSd().getCols();
        for (FieldSchema schema : fieldSchemaList) {
            System.out.println("字段: " + schema.getName() + ", 类型: " + schema.getType());
        }

        client.close();
    }
}
