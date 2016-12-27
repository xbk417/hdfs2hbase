package com.bonc.hbase.hdfs2hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonc.hbase.hdfs2hbase.util.ImprtConfig;
import com.bonc.hbase.hdfs2hbase.util.UUIDGenerator;

/**
 * 把关系型数据库中导出的文本文件，导入HBase中
 * @author xiabaike
 * @date 2016-03-31
 */
public class TextFile2HBaseImport {
	
	private static final Logger LOG = LoggerFactory.getLogger(TextFile2HBaseImport.class);
	
	private static String IMPORT_TABLE_NAME = "import.table.name";
	private static String IMPORT_TABLE_FAMILY = "import.table.family";
	private static String IMPORT_TABLE_COLUMNS = "import.table.columns";
	private static String IMPORT_TABLE_ROWKEY = "import.table.rowkey";
	private static String IMPORT_FILE_SEPARATOR = "impot.file.separator";
	private static String IMPORT_TABLE_NUMS = "import.table.nums";
	private static ImprtConfig config;
	private static int nums;
	public class ImportMapper extends Mapper<LongWritable, Text, NullWritable, Mutation> {
		
		private String COLUMN_FAMILY = null;
		private List<String> columnList = null;
		protected void setup(Context context) throws IOException, InterruptedException {
			config = new ImprtConfig(context.getConfiguration().get("config.filepath"));
			COLUMN_FAMILY = config.getString(IMPORT_TABLE_FAMILY);
			columnList = config.getList(IMPORT_TABLE_COLUMNS);
			nums = config.getInt(IMPORT_TABLE_NUMS, 1000);
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Mutation>.Context context)
	            throws IOException, InterruptedException {
			String line = value.toString();
			System.out.println(new ImmutableBytesWritable().get().toString());
			try{
				String[] datas = line.split(config.getString(IMPORT_FILE_SEPARATOR, "\t"), -1);
				String rowKey = config.getString(IMPORT_TABLE_ROWKEY);
				for(int i = 0; i < nums; i++) {
					if(rowKey == null || "".equals(rowKey)) {
						rowKey = UUIDGenerator.getUUID();
					}
					//
					Put put = new Put(rowKey.getBytes());
					int num = columnList.size() >= datas.length ? datas.length : columnList.size();
					for(int j = 0; j < num; j++) {
						 put.addColumn(COLUMN_FAMILY.getBytes(), columnList.get(j).getBytes(), datas[j].getBytes());
					}
					context.write(NullWritable.get(), put);
				}
				
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static Job createSubmitJob(Configuration conf, String[] args) {
		Job job = null;
		try {
			config = new ImprtConfig(args[0]);
			if(config.getString("fs.defaultFS") != null && !"".equals(config.getString("fs.defaultFS"))) {
				conf.set("fs.defaultFS", config.getString("fs.defaultFS"));
			}else{
				conf.addResource("hdfs-site.xml");
			}
			conf.set("config.filepath", args[0]);
//			conf.set(IMPORT_TABLE_FAMILY, config.getString(IMPORT_TABLE_FAMILY));
//			conf.set(IMPORT_TABLE_ROWKEY, config.getString(IMPORT_TABLE_ROWKEY));
//			conf.set(IMPORT_TABLE_COLUMNS, config.get(IMPORT_TABLE_COLUMNS));
		    //设置zookeeper
			conf.set("hbase.zookeeper.quorum", config.getString("hbase.zookeeper.quorum"));
		    System.out.println(config.getString("hbase.zookeeper.quorum"));
		    //设置hbase表名称
		    conf.set(TableOutputFormat.OUTPUT_TABLE, config.getString(IMPORT_TABLE_NAME));
		    //将该值改大，防止hbase超时退出
		    conf.set("dfs.socket.timeout", config.getString("dfs.socket.timeout", "180000"));
        
			job = Job.getInstance(conf, "Import" + "_" + config.getString(IMPORT_TABLE_NAME));
			//当打包成jar运行时，必须有以下2行代码  
	        TableMapReduceUtil.addDependencyJars(job);
	        job.setJarByClass(TextFile2HBaseImport.class);
	        job.setMapperClass(ImportMapper.class);
	        job.setNumReduceTasks(0);
	        job.setMapOutputKeyClass(NullWritable.class);
	        job.setMapOutputValueClass(Mutation.class);
	        job.setInputFormatClass(TextInputFormat.class);
	        //不再设置输出路径，而是设置输出格式类型
	        job.setOutputFormatClass(TableOutputFormat.class);

	        FileInputFormat.setInputPaths(job, args[1]);
		} catch (IOException e) {
			e.printStackTrace();
		}
        
		return job;
	}
	
	private static void usage(final String errorMsg) {
	    if (errorMsg != null && errorMsg.length() > 0) {
	      System.err.println("ERROR: " + errorMsg);
	    }
	    System.err.println("Usage: Import [options] <configfile> <inputdir>");
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = HBaseConfiguration.create();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      usage("Wrong number of arguments: " + otherArgs.length);
	      System.exit(-1);
	    }
	    Job job = createSubmitJob(conf, otherArgs);
	    boolean isJobSuccessful = job.waitForCompletion(true);
	    long inputRecords = job.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
	    long outputRecords = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
    	System.err.println("	Map Input Records : " + inputRecords);
    	System.err.println("	Map Output Records : " + outputRecords);
	    if (outputRecords < inputRecords) {
	    	System.err.println("Warning, not all records were imported (maybe filtered out).");
	    	if (outputRecords == 0) {
	    		System.err.println("If the data was exported from HBase 0.94 "+
	    				"consider using -Dhbase.import.version=0.94.");
	    	}
	    }

	    System.exit(isJobSuccessful ? 0 : 1);
	}
}
