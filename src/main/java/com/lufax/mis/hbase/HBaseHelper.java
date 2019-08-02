package com.lufax.mis.hbase;

import com.lufax.mis.conf.ConfigurationManager;
import com.lufax.mis.constant.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * HBase辅助类
 *
 * @author sherlock
 * @create 2019/5/5
 * @since 1.0.0
 */
public class HBaseHelper {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseHelper.class.getName());
    private static final TableName TABLENAME = TableName.valueOf(ConfigurationManager.getProperty(Constants.HTABLE_NAME));
    private static final String[] COLUMN_FAMILIES  = ConfigurationManager.getProperty(Constants.HTABLE_COLUMNFAMILY).split(",");
    private static final String[] COLUMNS = ConfigurationManager.getProperty(Constants.HTABLE_COLUMN).split(",");

    private static Connection conn = null;
    private static Admin admin = null;
    private static Table table = null;

    static {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
            conf.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));
//            System.setProperty("java.security.krb5.conf", "con/krb5.conf");
//            conf.set("hadoop.security.authentication", "Kerberos");
//            conf.set("hbase.client.ipc.pool.size", "100");
//            conf.set("hbase.ipc.client.fallback-to-simple-auth-allowed", "true");
            ExecutorService executor = Executors.newFixedThreadPool(ConfigurationManager.getIntegerPro(Constants.THEAD_NUM));
            conn = ConnectionFactory.createConnection(conf, executor);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取HBase的Connection
     * @return Connection
     */
    public static Connection getConn(){
        return conn;
    }

    /**
     * list tables
     */
    public static void listTables() {
        try {
            try {
                admin = conn.getAdmin();
                TableName[] tableNames = admin.listTableNames();
                for (TableName tableName : tableNames) {
                    System.out.println(tableName.getNameAsString());
                }
            } finally {
                admin.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表(表名: test_userBehaviourData 列簇:sql )
     */
    public static void createTable(){
        try {
            try {
                admin = conn.getAdmin();
                if (!admin.tableExists(TABLENAME)) {
                    HTableDescriptor hTableDescriptor = new HTableDescriptor(TABLENAME);
                    for (String columnFamily : COLUMN_FAMILIES) {
                        hTableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
                    }
                    admin.createTable(hTableDescriptor);
                }else {
                    LOG.info(" =====table has already existed; please don't duplicate creating! =====");
                }
            } finally {
                admin.close();
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * 插入数据
     * @param sql 查询语句
     * @param timestamp 有效时间
     */
    public static void insertColumnValue(String rowKey, String sql, String timestamp){
        try {
            try {
                admin = conn.getAdmin();
                if(!admin.tableExists(TABLENAME))
                    createTable();
                table = conn.getTable(TABLENAME);
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILIES[0]), Bytes.toBytes(COLUMNS[0]), Bytes.toBytes(sql))
                        .addColumn(Bytes.toBytes(COLUMN_FAMILIES[0]), Bytes.toBytes(COLUMNS[1]), Bytes.toBytes(timestamp));
                table.put(put);
            } finally {
                table.close();
                admin.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取sql/timestamp语句值
     * @param rowKey 键
     * @return cell值
     */
    public static String getColumnValue(String rowKey, int column_index){
        String val = null;
        try {
            try {
                admin = conn.getAdmin();
                table = conn.getTable(TABLENAME);
                Get get = new Get(Bytes.toBytes(rowKey));
                if (!admin.tableExists(TABLENAME)) {
                    LOG.error("===== table does not exists, please check again =====");
                } else if(!table.exists(get)) {
                    LOG.error("===== the value does not exists, please check again =====");
                } else {
                    Result result = table.get(get);
                    byte[] _val = result.getValue(Bytes.toBytes(COLUMN_FAMILIES[0]), Bytes.toBytes(COLUMNS[column_index]));
                    val = new String(_val);
                }
            } finally {
                table.close();
                admin.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return val;
    }


}

class TestHBaseHelper{

    public static void main(String[] args) {

        String[] cfs = new String[]{"cf1", "cf2"};
//        HBaseHelper.createTable();
//        HBaseHelper.listTables();
        String rowKey = "001";
        String sql = "select * from test_userBehaviourData;";
        String timestamp = "1557739568000";
        HBaseHelper.insertColumnValue(rowKey, sql, timestamp);
        System.out.println(HBaseHelper.getColumnValue(rowKey, 0));
        System.out.println(HBaseHelper.getColumnValue(rowKey, 1));

    }
}