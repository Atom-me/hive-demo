package com.atom.hive.func;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;

/**
 * 简单for循环函数
 *
 * @author Atom
 */
public class ForUDTF extends GenericUDTF {


    /**
     * 起始
     */
    IntWritable start;
    /**
     * 结束
     */
    IntWritable end;
    /**
     * 增量
     */
    IntWritable inc;

    private Object[] forwardObj;

    /**
     * 初始化函数
     *
     * @param args
     * @return
     * @throws UDFArgumentException
     */
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        start = ((WritableConstantIntObjectInspector) args[0]).getWritableConstantValue();
        end = ((WritableConstantIntObjectInspector) args[1]).getWritableConstantValue();
        if (args.length == 3) {
            inc = ((WritableConstantIntObjectInspector) args[2]).getWritableConstantValue();
        } else {
            inc = new IntWritable(1);
        }
        this.forwardObj = new Object[1];
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldNames.add("col0");

        fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT));
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }


    /**
     * Give a set of arguments for the UDTF to process.
     *
     * @param args object array of arguments
     */
    @Override

    public void process(Object[] args) throws HiveException {

        for (int i = start.get(); i < end.get(); i = i + inc.get()) {
            this.forwardObj[0] = new Integer(i);
            forward(forwardObj);
        }
    }

    /**
     * Called to notify the UDTF that there are no more rows to process.
     * Clean up code or additional forward() calls can be made here.
     */
    @Override
    public void close() throws HiveException {

    }
}
