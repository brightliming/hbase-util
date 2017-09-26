package com.ehualu.hbase;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;
import java.util.NavigableMap;

/**
 * Created by Administrator on 2017/9/26.
 */
public class ResultTransformer<KEY extends Comparable,VALUE> {
    private final NavigableMap<KEY,VALUE> transformedMap = Maps.newTreeMap();

    public ResultTransformer(Result result, String columnFamily,
                             Function<byte[],KEY> keyTransformer,Function<byte[],VALUE> valueTransformer){

       Map<byte[],VALUE> newMaps = Maps.transformValues(result.getFamilyMap(Bytes.toBytes(columnFamily)),valueTransformer);

       for(Map.Entry<byte[],VALUE> entry:newMaps.entrySet()){
           transformedMap.put(keyTransformer.apply(entry.getKey()),entry.getValue());
       }
    }

    public static ResultTransformer<String, String> forStrings(Result result,String columnFamily){
       return new ResultTransformer<String,String>(result,columnFamily,HBaseFunctions.BYTES_TO_STRING,HBaseFunctions.BYTES_TO_STRING);
    }

    public Map<KEY, VALUE> columnsStartingWith(final String prefix) {

        return Maps.filterKeys(transformedMap, new Predicate<KEY>() {
            @Override
            public boolean apply(KEY key) {
                return key.toString().startsWith(prefix);
            }
        });
    }

    public NavigableMap<KEY, VALUE> transform() {
        return transformedMap;
    }

}
