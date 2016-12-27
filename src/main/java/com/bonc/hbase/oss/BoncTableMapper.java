package com.bonc.hbase.oss;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.bonc.hbase.oss.jdbc.ConfJDBC;
import org.slf4j.LoggerFactory;

public class BoncTableMapper extends TableMapper<NullWritable, Text> {
	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(BoncTableMapper.class);

	private ArrayList<String> list;
	private String decollator;
	private String spec_id;
	private String outputPath;
	private String errOutputPath;
	private String confTableName;
	
	
	private MultipleOutputs<NullWritable,Text> mos;

	public static void initJob(String table, Scan scan, Class<? extends TableMapper> mapper, Job job) throws IOException {
		TableMapReduceUtil.initTableMapperJob(table, scan, mapper, NullWritable.class, Text.class, job);
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<NullWritable,Text>(context);
		
		this.spec_id = context.getConfiguration().get("conf.specid");
		this.outputPath = context.getConfiguration().get("conf.outputPath");
		this.errOutputPath = context.getConfiguration().get("conf.errOutputPath");
		this.confTableName = context.getConfiguration().get("conf.confTableName");
		
		this.decollator = new String2Hex("0x05").toString();
		
		String driverClassName = context.getConfiguration().get("driverClassName");
		String dbUrl = context.getConfiguration().get("dbUrl");
		String dbUserName = context.getConfiguration().get("dbUserName");
		String dbPassWord = context.getConfiguration().get("dbPassWord");
		this.list = ConfJDBC.selectConf(driverClassName, dbUrl, dbUserName, dbPassWord,this.confTableName);
	}

	@Override
	protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
		try{
			Map<String, HashMap<String, String>> map = new HashMap<String,HashMap<String,String>>();

			for (Cell cell : result.rawCells()) {
				HashMap<String,String> qualiferMap = new HashMap<String,String>();

				String qualifier =  new String(CellUtil.cloneQualifier(cell));

				qualiferMap.put("qualifer", qualifier);
				qualiferMap.put("value", new String(CellUtil.cloneValue(cell)));
				qualiferMap.put("timestamp", cell.getTimestamp()+"");

				map.put(qualifier, qualiferMap);
			}

			if(map.get("SPEC_ID").get("value").equals(this.spec_id) || this.spec_id.equals("null")) {

				StringBuilder sb = new StringBuilder();
				for (String s : this.list) {

					if (map.get(s) != null) {
						sb.append(map.get(s).get("value")).append(this.decollator);
						if (s.equals("ID") || s.equals("SPEC_ID")) {
							continue;
						}
						map.remove(s);
					} else {
						sb.append("").append(this.decollator);
					}

				}
				this.mos.write(NullWritable.get(), new Text(sb.toString().substring(0, sb.toString().length() - 1)), this.outputPath);

				for (Map.Entry<String, HashMap<String, String>> entry : map.entrySet()) {
					HashMap<String, String> hashMap = entry.getValue();
					String qualifer = hashMap.get("qualifer");
					if (qualifer.equals("ID") || qualifer.equals("SPEC_ID")) {
						continue;
					}
					String value = hashMap.get("value");
					String timestamp = hashMap.get("timestamp");

					String id = map.get("ID").get("value");
					String spec_id = map.get("SPEC_ID").get("value");

					String secondField = "";
					String threeField = "";
					String[] field = qualifer.split("\\.", -1);

					switch (field.length) {
						case 1:
							secondField = qualifer;
							break;
						case 2:
							secondField = field[1];
							break;
						case 3:
							secondField = field[2];
							threeField = field[1];
							break;
					}
					String response = id + this.decollator + spec_id + this.decollator + secondField + this.decollator + threeField + this.decollator + qualifer + this.decollator + value + this.decollator + timestamp;

					mos.write(NullWritable.get(), new Text(response), this.errOutputPath);
				}
			}
		}catch(Exception e) {
			LOG.error("test",  e);
		}
		
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}
