import com.ehualu.hbase.PutBuilder;
import com.ehualu.hbase.TableBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


/**
 * Created by wangbaocai on 17-06-26.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:spring-core.xml"})
public class HbaseTest {
    @Autowired
    private Configuration conf;

    @Test
    public void testHbase() throws Exception{
//        TableBuilder builder = new TableBuilder(conf);
//
//        builder.withTableName("limltest").withSimpleColumnFamilies("cf1","cf2").create();

        Connection con = ConnectionFactory.createConnection(conf);
        Table table = con.getTable(TableName.valueOf("limltest"));
        PutBuilder builder = new PutBuilder(table);
        builder.withRowKey(2).withColumnFamily("cf1").put("test2","test2").putAll();

        con.close();
    }

}
