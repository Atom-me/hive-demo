package com.atom.hive.func;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * 将字符串转换成日期
 * $hive> create temporary function atom_to_date as 'com.atom.hive.func.ToDate';
 *
 * @author Atom
 */
@Description(name = "atom_to_date",
        value = "this is my first udf !",
        extended = "For example : select atom_to_date('2022/02/27 12:12:12') ;")
public class ToDate extends UDF {

    private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");


    /**
     * 计算
     *
     * @param dateTimeStr
     * @return
     */
    public Date evaluate(String dateTimeStr) {
        try {
            LocalDateTime localDateTime = LocalDateTime.parse(dateTimeStr, dateTimeFormatter);
            Instant instant = localDateTime.atZone(ZoneId.systemDefault()).toInstant();
            return Date.from(instant);
        } catch (Exception e) {
            //ignore
        }
        return new Date();
    }


}
