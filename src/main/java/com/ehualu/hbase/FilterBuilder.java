package com.ehualu.hbase;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import static org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

/**
 * Created by Administrator on 2017/9/26.
 */
public class FilterBuilder<KEY,VALUE> {
    private final Function<KEY, byte[]> keyTransformer;
    private final Function<VALUE, byte[]> valueTransformer;

    private byte[] currentColumnFamily;
    private KEY currentColumn;

    private final FilterList filterList = new FilterList();

    public FilterBuilder(Function<KEY, byte[]> keyTransformer,
                         Function<VALUE, byte[]> valueTransformer) {
        this.keyTransformer = keyTransformer;
        this.valueTransformer = valueTransformer;
    }

    public static FilterBuilder<String, String> stringBuilder() {
        return new FilterBuilder<String, String>(HBaseFunctions.STRING_TO_BYTES,
                HBaseFunctions.STRING_TO_BYTES);
    }

    public FilterBuilder<KEY, VALUE> withColumnFamily(String columnFamily) {
        return withColumnFamily(Bytes.toBytes(columnFamily));
    }

    private FilterBuilder<KEY, VALUE> withColumnFamily(byte[] columnFamily) {
        this.currentColumnFamily = columnFamily;
        return this;
    }

    public FilterBuilder<KEY, VALUE> column(KEY column) {
        this.currentColumn = column;
        return this;
    }

    public FilterBuilder<KEY, VALUE> valueMustEqual(VALUE value) {
        filterAdditionPreconditionChecks();

        SingleColumnValueFilter filter = new SingleColumnValueFilter(currentColumnFamily,
                keyTransformer.apply(currentColumn), CompareOp.EQUAL, valueTransformer.apply(value));

        filter.setFilterIfMissing(true);
        filterList.addFilter(filter);

        return this;
    }

    private void filterAdditionPreconditionChecks() {
        Preconditions.checkNotNull(currentColumnFamily);
        Preconditions.checkNotNull(currentColumn);
    }

    public Filter build() {
        return filterList;
    }
}
