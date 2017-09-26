package com.ehualu.hbase;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;

/**
 * Created by Administrator on 2017/9/26.
 */
public class HBaseFunctions {
    public static final Function<String,byte[]> STRING_TO_BYTES = new Function<String,byte[]>(){
        @Nullable
        @Override
        public byte[] apply(@Nullable String s) {
            return Bytes.toBytes(s);
        }
    };

    public static final Function<byte[],String> BYTES_TO_STRING = new Function<byte[], String>() {
        @Nullable
        @Override
        public String apply(@Nullable byte[] bytes) {
            return Bytes.toString(bytes);
        }
    };

    public static byte[][] toByteArrays(String... strings) {
        Collection<byte[]> transform = Collections2.transform(Arrays.asList(strings),
                STRING_TO_BYTES);
        byte[][] array = transform.toArray(new byte[0][]);
        return array;
    }

}
