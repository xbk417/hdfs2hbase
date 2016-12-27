package com.bonc.hbase.hbase2hdfs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * mapper
 * 具体业务处理
 * @author xiabaike
 * @date 2016年5月17日
 */
public class DataConverMapper extends TableMapper<NullWritable, Text> {

	// 多文件输出 
//	private MultipleOutputs<NullWritable, Text> mos;
//	private static final String OUT_TABLE_NAME = "get.table.name";
	private static final String ID_FAMILY = "get.table.id.family";
	private static final String ID_QUALIFIER = "get.table.id.qualifier";
	private static final String SPECIAL_FAMILY = "get.table.special.family";
	private static final String SPECIAL_QUALIFIER = "get.table.special.qualifier";
	private static final String FILTER_SPEC_ID = "get.table.filter.specid";
	// 分隔符
	private static String separator = null;
	// 表名
//	private static String filterSpecId = null;
	private static Map<String, String> specidMap = null;
	private static String idFamily = null;
	private static String idQualifier = null;
	private static String specialFamily = null;
	private static String specialQualifier = null;

	public static void initJob(String table, Scan scan, Class<? extends TableMapper> mapper, Job job) throws IOException {
		TableMapReduceUtil.initTableMapperJob(table, scan, mapper, NullWritable.class, Text.class, job, true, TableInitializeInputFormat.class);
	}
	
	public void setup(Context context) throws IOException, InterruptedException {
//		mos = new MultipleOutputs<NullWritable, Text>(context);
//		tableName = context.getConfiguration().get(OUT_TABLE_NAME);
		String filterSpecId = context.getConfiguration().get(FILTER_SPEC_ID);
		if(filterSpecId != null && !"".equals(filterSpecId)){
			specidMap = new HashMap<String, String>();
			String[] specidArr = filterSpecId.split(",", -1);
			for(String specid : specidArr) {
				specidMap.put(specid, null);
			}
		}
		separator = context.getConfiguration().get("get.table.out.separator", "\t");
		idFamily = context.getConfiguration().get(ID_FAMILY, "info");
		// id所对应的列限定符
		idQualifier = context.getConfiguration().get(ID_QUALIFIER, "ID");
		// specid所在的列族
		specialFamily = context.getConfiguration().get(SPECIAL_FAMILY, "info");
		// specid所对应的列限定符
		specialQualifier = context.getConfiguration().get(SPECIAL_QUALIFIER, "SPEC_ID");
	}
	
	public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		String specilid = Bytes.toString(value.getValue(Bytes.toBytes(specialFamily), Bytes.toBytes(specialQualifier)));
		specilid = specilid == null ? "" :specilid;
		if(specidMap != null && specidMap.size() != 0) {
			if(!specidMap.containsKey(specilid)) {
				// 如果当前数据的specilid不等于设定的filterSpecId，不进行操作
				return;
			}
		}
		// rowkey
		String rowkey = Bytes.toString(key.get());
		String id = Bytes.toString(value.getValue(Bytes.toBytes(idFamily), Bytes.toBytes(idQualifier)));
		id = id == null ? "" : id;
		String[] columns = null;
		int size = value.size();
		int i = 1;
		for (KeyValue rowKV : value.raw()) {
			// 列族
			sb.append(rowkey).append(separator).append(Bytes.toString(rowKV.getFamily())).append(separator)
				.append(id).append(separator).append(specilid).append(separator);
			// 列限定符
			columns = Bytes.toString(rowKV.getQualifier()).split("\\.", -1);
			if(columns.length == 1) {
				sb.append(columns[0]).append(separator).append("").append(separator);
			}else if(columns.length == 3) {
				sb.append(columns[2]).append(separator).append(columns[1]).append(separator);
			}
			String val = Bytes.toString(rowKV.getValue()).replaceAll("\n|\t|\r|\r\n", "");
			// 列值
			if(size != i) {
				sb.append(val).append(separator).append(rowKV.getTimestamp()).append("\n");
			}else{
				sb.append(val).append(separator).append(rowKV.getTimestamp());
			}
			i++;
        }
		context.write(NullWritable.get(), new Text(sb.toString()));
	}
	
}
