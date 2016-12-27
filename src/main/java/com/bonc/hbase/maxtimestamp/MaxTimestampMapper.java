package com.bonc.hbase.maxtimestamp;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * mapper
 * 具体业务处理
 * @author xiabaike
 * @date 2016年5月17日
 */
public class MaxTimestampMapper extends TableMapper<LongWritable, Text> {
	
	public static void initJob(String table, Scan scan, Class<? extends TableMapper> mapper, Job job) throws IOException {
		TableMapReduceUtil.initTableMapperJob(table, scan, mapper, LongWritable.class, Text.class, job);
	}
	
	public void setup(Context context) throws IOException, InterruptedException {

	}
//	private static long maxTimestamp = 0L;
//	private static Result result = new Result();

	public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		List<Cell> cellList = value.listCells();//.get(0).getTimestamp()
		long maxTimestamp = 0L;
		long timestamp;
		for(Cell cell : cellList) {
			timestamp = cell.getTimestamp();
			if(timestamp > maxTimestamp) {
				maxTimestamp = timestamp;
//				result = value;
//				System.out.println(value.toString());
			}
		}
//		costValue.set(maxTimestamp);
//		if( isbigger ) {
		context.write( new LongWritable(maxTimestamp), new Text(value.toString()));
//		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
//		context.write(new LongWritable(maxTimestamp), result);
	}
}
