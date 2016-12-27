package com.bonc.hbase.hdfs2hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.bonc.hbase.hdfs2hbase.util.ImprtConfig;
import com.bonc.hbase.hdfs2hbase.util.UUIDGenerator;

/**
 * 
 * @author xiabaike
 * @date 2016年4月18日
 */
public class File2Hbase {
	
	private static String IMPORT_TABLE_NAME = "import.table.name";
	private static String IMPORT_TABLE_FAMILY = "import.table.family";
	private static String IMPORT_TABLE_COLUMNS = "import.table.columns";
	private static String IMPORT_TABLE_ROWKEY = "import.table.rowkey";
	private static String IMPORT_FILE_SEPARATOR = "impot.file.separator";
	private static String IMPORT_TABLE_NUMS = "import.table.nums";
	private String COLUMN_FAMILY = null;
	private List<String> columnList = null;
	private static ImprtConfig config;
	private static long nums;
	
	private static Configuration conf = null;  
    private static HTable table = null;
    
	public File2Hbase(String filepath) {
		config = new ImprtConfig(filepath);
		COLUMN_FAMILY = config.getString(IMPORT_TABLE_FAMILY);
		columnList = config.getList(IMPORT_TABLE_COLUMNS);
		int num = config.getInt(IMPORT_TABLE_NUMS);
		
		conf =  HBaseConfiguration.create();
//		conf.set("fs.defaultFS", config.getString("fs.defaultFS"));
        //与hbase/conf/hbase-site.xml中hbase.zookeeper.quorum配置的值相同   
		conf.set("hbase.zookeeper.quorum", config.getString("hbase.zookeeper.quorum"));
		conf.set("hbase.zookeeper.property.clientPort", "2181");
        //与hbase/conf/hbase-site.xml中hbase.zookeeper.property.clientPort配置的值相同  
		conf.set("dfs.socket.timeout", config.getString("dfs.socket.timeout"));  
        try {
			table = new HTable(conf, config.getString(IMPORT_TABLE_NAME));
			table.setAutoFlush(false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    
    public void addRecord () {
        try {
        	for(int i = 0; i < Long.MAX_VALUE; i++) {
        		String rowKey = UUIDGenerator.getUUID();
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes("hz811"), Bytes.toBytes("test"), Bytes.toBytes("test"));
                put.addColumn("hz811".getBytes(), "test1".getBytes(), "test1".getBytes());
                put.addColumn("hz811".getBytes(), "test2".getBytes(), "test2".getBytes());
                put.addColumn("hz811".getBytes(), "test3".getBytes(), "test3".getBytes());
                put.addColumn("hz811".getBytes(), "test4".getBytes(), "test4".getBytes());
                put.addColumn("hz811".getBytes(), "test5".getBytes(), "test5".getBytes());
                put.addColumn("hz811".getBytes(), "test6".getBytes(), "test6".getBytes());
                put.addColumn("hz811".getBytes(), "test7".getBytes(), "test7".getBytes());
                put.addColumn("hz811".getBytes(), "test8".getBytes(), "test8".getBytes());
                put.setWriteToWAL(false);
                System.out.println("****************");
                table.put(put);
        	}
        } catch (IOException e) {  
            e.printStackTrace();  
        }
    }
    
    public  void close() {
    	try {
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    
    public static void main(String[] args) {
    	File2Hbase file = new File2Hbase(args[0]);
    	file.addRecord();
//    	file.close();
	}
}


	