package com.bonc.hbase.maxtimestamp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * 
 * @author xiabaike
 * @date 2016年6月22日	
 */
public class DbWritable implements Writable, DBWritable{

	private static final String CONF_FILED = "max.timestamp.conf.filed";
	
	private class Filed {
		
		
		
		public Filed(Configuration conf) {
			String fileds = conf.get(CONF_FILED);
			String[] filedArr = fileds.split(";", -1);
			if(filedArr != null && filedArr.length > 0) {
				for(String filed : filedArr) {
					
				}
			}
		}
	}
	
	public DbWritable(Configuration conf) {
		
	}
	
	@Override
	public void write(PreparedStatement statement) throws SQLException {
		
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {

	}

	@Override
	public void write(DataOutput out) throws IOException {
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
	}

}