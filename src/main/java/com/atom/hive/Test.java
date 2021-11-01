package com.atom.hive;

import org.apache.commons.lang3.StringUtils;

/**
 * @author Atom
 */
public class Test {
    public static void main(String[] args) {
        String location = "hdfs://ip-172-31-5-91.cn-northwest-1.compute.internal:8020/user/hive/warehouse/atom_test.db/tb001";
        System.err.println(StringUtils.substring(location, 0, StringUtils.ordinalIndexOf(location, "/", 3)));
        System.err.println(StringUtils.substring(location,  StringUtils.ordinalIndexOf(location, "/", 3)));


    }
}
