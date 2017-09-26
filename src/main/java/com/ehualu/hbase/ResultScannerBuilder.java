package com.ehualu.hbase;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Administrator on 2017/9/26.
 */
public class ResultScannerBuilder {
    private final List<byte[]> familyNames = Lists.newArrayList();
    private final Table table;

    private byte[] startRow = null;
    private byte[] stopRow = null;

    private Filter filter = null;

    public ResultScannerBuilder(Table table) {
        this.table = table;
    }

    public ResultScannerBuilder withColumnFamilies(String... columnFamilies) {
        return withColumnFamilies(HBaseFunctions.toByteArrays(columnFamilies));
    }

    public ResultScannerBuilder withColumnFamilies(byte[]... columnFamilies) {
        familyNames.addAll(Arrays.asList(columnFamilies));
        return this;
    }

    public ResultScanner build() throws IOException {
        Scan scan = new Scan();
        for (byte[] family : familyNames) {
            scan.addFamily(family);
        }

        if (startRow != null) {
            scan.setStartRow(startRow);
        }
        if (stopRow != null) {
            scan.setStopRow(stopRow);
        }

        if (filter != null) {
            scan.setFilter(filter);
        }

        return table.getScanner(scan);
    }

    public ResultScannerBuilder startAt(Writable startRowKey) {
        return startAt(HBaseUtils.forWritable(startRowKey));
    }

    public ResultScannerBuilder startAt(byte[] startRow) {
        this.startRow = startRow;
        return this;
    }

    public ResultScannerBuilder stopAt(Writable stopRow) {
        return stopAt(HBaseUtils.forWritable(stopRow));
    }

    public ResultScannerBuilder stopAt(byte[] stopRow) {
        this.stopRow = stopRow;
        return this;
    }

    public ResultScannerBuilder withFilter(Filter filter) {
        this.filter = filter;
        return this;
    }
}
