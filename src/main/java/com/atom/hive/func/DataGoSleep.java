package com.atom.hive.func;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * 自定义sleep函数实现
 * $hive> create temporary function atom_to_date as 'com.atom.hive.func.DataGoSleep';
 *
 * @author Atom
 */
@Description(name = "datago_sleep",
        value = "datago_sleep(timeout)",
        extended = "For example : select datago_sleep(100) ;")
public class DataGoSleep extends UDF {


    /**
     * sleep
     *
     * @param seconds
     * @return
     */
    public int evaluate(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
            return seconds;
        } catch (Exception e) {
            //ignore
        }
        return 0;
    }


}
