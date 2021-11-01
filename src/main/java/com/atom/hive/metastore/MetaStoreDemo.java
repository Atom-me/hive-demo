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
        List<String> tablesList = client.getAllTables("default");
        System.out.print("default:  ");
        for (String s : tablesList) {
            System.out.print(s + "\t");
        }
        System.out.println();

        //get specified tab by db name and tab name
        System.err.println("default.my_table8 表信息: ");
        Table table = client.getTable("default", "test001");
        System.err.println("table info:" + table.toString());
        System.err.println("==========");
        System.err.println(table.getSd().getLocation());
        System.err.println(table.getSd().getInputFormat());
        table.getCreateTime();
        System.err.println(table.getParameters().get("comment"));
        List<FieldSchema> fieldSchemaList = table.getSd().getCols();
        for (FieldSchema schema : fieldSchemaList) {
            System.err.println("字段: " + schema.getName() + ", 类型: " + schema.getType());
        }

        client.close();
    }
}
