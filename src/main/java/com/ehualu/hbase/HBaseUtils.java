package com.ehualu.hbase;

import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

/**
 * Created by Administrator on 2017/9/26.
 */
public class HBaseUtils {
    public static void closeQuietly(Table table){
        try {
            table.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] forWritable(Writable writable) {
        try {
            return Writables.getBytes(writable);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
