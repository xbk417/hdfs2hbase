package com.bonc.hbase.hbase2hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * @author xiabaike
 * @date 2016年5月20日
 */
public class HBase2Hdfs {

	public static void main(String[] args) throws Exception {
//		getOneRow(args, "rk03");
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(args[0]));
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		Connection connection = ConnectionFactory.createConnection(conf);
		System.out.println(conf.get("hbase.zookeeper.quorum"));
		Table table = connection.getTable(TableName.valueOf(conf.get("get.table.name")));
//		Get g = new Get(Bytes.toBytes("rk03"));
//        Result r = table.get(g);
//        byte [] value = r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col01"));
//        String valueStr = Bytes.toString(value);
//        System.out.println("GET: " + valueStr);
		
		Scan scan = new Scan();
//		scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col01"));
		ResultScanner scanner = table.getScanner(scan);
		for(Result result : scanner) {
			System.out.println(result);
		}
		scanner.close();
		table.close();
		connection.close();
	}
	
	public static boolean getOneRow(String[]args, String row) {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(args[0]));
//		conf.set("fs.defaultFS", "hdfs://192.168.1.201:9000");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		HTable userTable = null;
		boolean falg = false;
		Get get = new Get(Bytes.toBytes(conf.get("get.table.row")));
		Result result;
		System.out.println("---===");
		try {
			userTable = new HTable(conf, conf.get("get.table.name"));
			userTable.setAutoFlush(false);
			System.out.println("---000");
			result = userTable.get(get);
			System.out.println("---");
			for (Cell cell : result.rawCells()) {
				try {
					System.out.println(cell.getValueArray().toString());
				} catch (Exception e) {
					continue;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return falg;
	}
	
}
