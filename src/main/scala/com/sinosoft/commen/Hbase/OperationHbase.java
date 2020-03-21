package com.sinosoft.commen.Hbase;

import com.sinosoft.utils.EnumUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by guo on 2017/12/27.
 */
public class OperationHbase {
    /**
     * 连接HBase地址
     */
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    private static final Object lock = new Object();

    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", EnumUtil.HBASE_ZOOKEEPER_PORT);
        configuration.set("hbase.zookeeper.quorum", EnumUtil.HBASE_ZOOKEEPER_IP);
        configuration.set("hbase.master", EnumUtil.HBASE_MASTER);
        configuration.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 1000000);
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection() {
        if (null == connection) {
            synchronized (lock) {
                if (null == connection) {//空的时候创建，不为空就直接返回；典型的单例模式
                    Configuration conf = HBaseConfiguration.create();
                    String zkHost = "mpc5:2181,mpc6:2181,mpc7:2181";
//                    String zkHost = "mpc5:2181,mpc6:2181,mpc7:2181";
                    conf.set("hbase.zookeeper.quorum", zkHost);
                    ExecutorService pool = Executors.newFixedThreadPool(10);//建立一个数量为10的线程池
                    try {
                        connection = ConnectionFactory.createConnection(conf, pool);//用线程池创建connection
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return connection;
    }

    public static Connection createHbaseClient() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", EnumUtil.HBASE_ZOOKEEPER_PORT);
        configuration.set("hbase.zookeeper.quorum", EnumUtil.HBASE_ZOOKEEPER_IP);
        configuration.set("hbase.master", EnumUtil.HBASE_MASTER);
        configuration.set("hbase.client.scanner.caching", "1");
        configuration.set("hbase.rpc.timeout", "600000");
        configuration.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 1000000);
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 关闭连接
     */
    public static void close() {
        try {
            if (null != admin) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除一张表
     *
     * @param tableName
     */
    public static void dropTable(String tableName) {
        TableName tName = TableName.valueOf(tableName);
        try {
            Admin admin = connection.getAdmin();
            if (admin.tableExists(tName)) {
                admin.disableTable(tName);
                admin.deleteTable(tName);
            } else {
                System.out.println(tableName + " not exists.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        close();
    }

    /**
     * 创建表
     *
     * @param tableName
     */
    public static void createTable(String tableName, String[] cols) {
        TableName tName = TableName.valueOf(tableName);
        try {
            Admin admin = connection.getAdmin();
            if (admin.tableExists(tName)) {
                System.out.println(tableName + " exists.");
            } else {
                HTableDescriptor hTableDesc = new HTableDescriptor(tName);
                for (String col : cols) {
                    HColumnDescriptor hColumnDesc = new HColumnDescriptor(col);
                    hTableDesc.addFamily(hColumnDesc);
                }
                admin.createTable(hTableDesc);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

//        close();
    }

    /**
     * 根据 rowkey删除一条记录
     *
     * @param tableName
     * @param rowkey
     */
    public static void deleteRow(String tableName, String rowkey) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(Bytes.toBytes(rowkey));
            table.delete(del);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
//        close();
    }

    /**
     * 单条件查询,根据rowkey查询唯一一条记录
     *
     * @param tableName
     */
    public static Map<String, String> QueryByCondition1(String tableName, String rowkey) {
        Map<String, String> map = new HashMap<String, String>();
        if (rowkey != null && !rowkey.equals("")) {
            Table table = null;
            try {
                table = OperationHbase.getConnection().getTable(TableName.valueOf(tableName));
                Get get = new Get(Bytes.toBytes(rowkey));
                Result result = table.get(get);
                for (Cell cell : result.rawCells()) {
                    String key = new String(CellUtil.cloneQualifier(cell));
                    String value = new String(CellUtil.cloneValue(cell));
                    map.put(key, value);
                }
                table.close();
//            close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return map;
    }

    public static Map<String, String> QueryByConditionTest(String tableName, String rowkey, Connection connection) {
        Map<String, String> map = new HashMap<String, String>();
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
//            System.out.println(connection);
            Get get = new Get(Bytes.toBytes(rowkey));
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                String key = new String(CellUtil.cloneQualifier(cell));
                String value = new String(CellUtil.cloneValue(cell));
                map.put(key, value);
            }
            table.close();
            //            close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }

    /**
     * 单条件按查询，查询多条记录
     *
     * @param tableName
     */
    public static Map<String, String> QueryByCondition2(String tableName, String column_name, String column_value) {
        Map<String, String> map = new HashMap<String, String>();
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan s = new Scan();
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                    Bytes.toBytes("wa"),
                    Bytes.toBytes(column_name),
                    CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes(column_value));
            filter.setFilterIfMissing(true);

            s.setFilter(filter);
            ResultScanner rs = table.getScanner(s);
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (Cell cell : r.rawCells()) {
                    String key = new String(CellUtil.cloneQualifier(cell));
                    String value = new String(CellUtil.cloneValue(cell));
                    map.put(key, value);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    /**
     * 单条件按查询，判断是否存在这条数据
     *
     * @param tableName
     */
    public static boolean exists(String tableName, String column_name, String column_value) {
        boolean result = false;
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan s = new Scan();
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                    Bytes.toBytes("wa"),
                    Bytes.toBytes(column_name),
                    CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes(column_value));
            filter.setFilterIfMissing(true);
            s.setFilter(filter);
            ResultScanner rs = table.getScanner(s);
            if (rs.next() != null) {
                result = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }


    /**
     * 组合条件查询
     *
     * @param tableName
     */
    public static Map<String, String> QueryByCondition3(String tableName) {
        Map<String, String> map = new HashMap<String, String>();
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            List<Filter> filters = new ArrayList<Filter>();
            SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes
                    .toBytes("wa"), Bytes
                    .toBytes("column_name"), CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("column_value"));
            filter1.setFilterIfMissing(true);
            filters.add(filter1);

            SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes
                    .toBytes("wa"), Bytes
                    .toBytes("column_name"), CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("column_value"));
            filter2.setFilterIfMissing(true);
            filters.add(filter2);

            SingleColumnValueFilter filter3 = new SingleColumnValueFilter(Bytes
                    .toBytes("wa"), Bytes
                    .toBytes("column_name"), CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("column_value"));
            filter3.setFilterIfMissing(true);
            filters.add(filter3);

            //FilterList.Operator.MUST_PASS_ALL，相当于每个filter是与的关系；
            //FilterList.Operator.MUST_PASS_ONE，相当于是或的关系；
            FilterList filterList1 = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);

            Scan scan = new Scan();
            scan.setFilter(filterList1);
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (Cell cell : r.rawCells()) {
                    String key = new String(CellUtil.cloneQualifier(cell));
                    String value = new String(CellUtil.cloneValue(cell));
                    map.put(key, value);
                }
            }
            rs.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    /**
     * 插入数据
     *
     * @param tableName
     */
    public static void insertData(String tableName, Map<String, String> map, String rowkey) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowkey));
            for (Map.Entry entry : map.entrySet()) {
                put.addColumn(Bytes.toBytes("wa"), Bytes.toBytes(entry.getKey().toString()), Bytes.toBytes(entry.getValue().toString()));
            }
            table.put(put);
            table.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        //close();
    }

    public static void insertData(String tableName, Map<String, String> map, String rowkey, Connection conn) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowkey));
            for (Map.Entry entry : map.entrySet()) {
                put.addColumn(Bytes.toBytes("wa"), Bytes.toBytes(entry.getKey().toString()), Bytes.toBytes(entry.getValue().toString()));
            }
            table.put(put);
            table.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        //close();
    }


    public static void main(String[] args) {
        Connection con = OperationHbase.createHbaseClient();
        Map<String, String> map = OperationHbase.QueryByConditionTest("INFO_TABLE",
                "http://cn.rfi.fr/wire/20190822-%E5%AE%9D%E7%89%B9%E7%93%B6%E5%8F%98%E7%8E%B0%E9%87%91-%E5%8E%84%E7%93%9C%E5%A4%9A%E9%80%9A%E5%8B%A4%E6%97%8F%E6%8B%BF%E9%92%B1%E6%90%AD%E5%85%AC%E8%BD%A6"
                , con);
        map.forEach((k, v) -> System.out.println(k + " " + v));
    }
}
