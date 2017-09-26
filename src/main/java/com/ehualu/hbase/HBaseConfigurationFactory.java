package com.ehualu.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.FactoryBean;

import java.util.Map;

/**
 * Created by Administrator on 2017/9/26.
 */
public class HBaseConfigurationFactory implements FactoryBean {

    private final Configuration hbaseConfiguration;

    public HBaseConfigurationFactory(){
        this.hbaseConfiguration = HBaseConfiguration.create();
    }

    public HBaseConfigurationFactory(Map<String, String> propertyMap) {
        this();
        for (Map.Entry<String, String> entry : propertyMap.entrySet()) {
            hbaseConfiguration.set(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public Object getObject() throws Exception {
        return hbaseConfiguration;
    }

    @Override
    public Class<? extends Configuration> getObjectType() {
        return Configuration.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
