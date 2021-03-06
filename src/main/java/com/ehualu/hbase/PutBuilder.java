package com.ehualu.hbase;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/9/26.
 */
public class PutBuilder {
    private final Table hTable;
    private final List<Put> puts = new ArrayList<>();
    private byte[] currentColumnFamily;
    private byte[] currentRowKey;
    private long currentTimeStamp = HConstants.LATEST_TIMESTAMP;

    public PutBuilder(Table hTable) {
        this.hTable = hTable;
    }

    public PutBuilder withColumnFamily(String columnFamily) {
        return withColumnFamily(Bytes.toBytes(columnFamily));
    }

    public PutBuilder withColumnFamily(byte[] columnFamily) {
        this.currentColumnFamily = columnFamily;
        return this;
    }

    public PutBuilder withRowKey(Writable writeableRowKey) {
        try {
            return withRowKey(Writables.getBytes(writeableRowKey));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public PutBuilder withRowKey(byte[] rowKey) {
        this.currentRowKey = rowKey;
        return this;
    }

    public PutBuilder withTimeStamp(long timeStamp) {
        this.currentTimeStamp = timeStamp;
        return this;
    }

    public PutBuilder put(String columnName, byte[] rowValue) {
        return put(Bytes.toBytes(columnName), rowValue);
    }

    public PutBuilder put(String columnName, int rowValue) {
        return put(columnName, Bytes.toBytes(rowValue));
    }

    public PutBuilder put(String columnName, long rowValue) {
        return put(columnName, Bytes.toBytes(rowValue));
    }

    public PutBuilder put(String columnName, String columnValue) {
        return put(columnName, Bytes.toBytes(columnValue));
    }

    public PutBuilder put(String columnName, Writable rowValue) {
        try {
            return put(Bytes.toBytes(columnName), Writables.getBytes(rowValue));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public PutBuilder put(byte[] columnName, byte[] rowValue) {
        if (currentColumnFamily == null) {
            throw new IllegalStateException(
                    "Current Column Family has not been set, please use withColumnFamily(...) before calling this method");
        }
        if (currentRowKey == null) {
            throw new IllegalStateException(
                    "Current Row Key has not been set, please use withRowKey(...) before calling this method");
        }

        Put put = new Put(currentRowKey);
        put.addColumn(currentColumnFamily, columnName, currentTimeStamp, rowValue);
        puts.add(put);
        return this;
    }

    public PutBuilder reset() {
        this.currentColumnFamily = null;
        this.currentRowKey = null;
        this.currentTimeStamp = HConstants.LATEST_TIMESTAMP;
        return this;
    }

    public void putAll()  {
        try {
            hTable.put(puts);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public PutBuilder withRowKey(int rowKey) {
        this.currentRowKey = Bytes.toBytes(rowKey);
        return this;
    }

    public PutBuilder withRowKey(long rowKey) {
        this.currentRowKey = Bytes.toBytes(rowKey);
        return this;
    }

    public PutBuilder withRowKey(String rowKey) {
        this.currentRowKey = Bytes.toBytes(rowKey);
        return this;
    }

    public PutBuilder withRowKey(double rowKey) {
        this.currentRowKey = Bytes.toBytes(rowKey);
        return this;
    }

    public PutBuilder withRowKey(ByteBuffer rowKey) {
        this.currentRowKey = Bytes.toBytes(rowKey);
        return this;
    }
}
