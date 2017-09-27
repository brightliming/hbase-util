package com.ehualu.hbase;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Value;

import java.util.Map;

/**
 * Created by Administrator on 2017/9/26.
 */
public class HBaseConfigurationFactory implements FactoryBean<Configuration> {

    private final Configuration hbaseConfiguration;

    @Value("${hbase.zookeeper.quorum}")
    private String clientAddress;
    @Value("${hbase.zookeeper.property.clientPort}")
    private String clientPort;

    public HBaseConfigurationFactory(){
        this.hbaseConfiguration = HBaseConfiguration.create();
    }


    @Override
    public Configuration getObject() throws Exception {
        hbaseConfiguration.set("hbase.zookeeper.quorum", clientAddress);
        if(StringUtils.isNotEmpty(clientPort)){
            hbaseConfiguration.set("hbase.zookeeper.property.clientPort", clientPort);
        }
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
