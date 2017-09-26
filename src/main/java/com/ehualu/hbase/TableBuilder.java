package com.ehualu.hbase;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.List;

/**
 * Created by Administrator on 2017/9/26.
 */
public class TableBuilder {

    private String tableName;
    private int maxVersions = 1;

    private final Configuration configuration;

    private final List<HColumnDescriptor> columnFamilyDescroptors = Lists.newArrayList();

    public TableBuilder(Configuration configuration){
        Preconditions.checkNotNull(configuration);
        this.configuration = configuration;
    }

    public TableBuilder withTableName(String tableName){
        this.tableName = tableName;
        return this;
    }

    public TableBuilder withMaxVersion(int maxVersions){
        this.maxVersions = maxVersions;
        return this;
    }

    public TableBuilder withSimpleColumnFamilies(String... familyNames){
        for(String familyName:familyNames){
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(familyName);
            columnDescriptor.setMaxVersions(maxVersions);
            columnFamilyDescroptors.add(columnDescriptor);
        }
        return this;
    }

    public void deleteAndRecreate(){
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))) {
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));
            }
            create();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(connection != null){
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    public void create(){
        Connection connection = null;
        Table table = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();

            Preconditions.checkState(!admin.tableExists(TableName.valueOf(tableName)));

            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

            for(HColumnDescriptor columnDescriptor:columnFamilyDescroptors){
                tableDescriptor.addFamily(columnDescriptor);
            }
            admin.createTable(tableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            if(connection != null){
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
