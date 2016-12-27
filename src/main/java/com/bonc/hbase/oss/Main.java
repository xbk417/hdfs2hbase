package com.bonc.hbase.oss;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class Main implements Tool{

	private static final Logger LOG = LoggerFactory.getLogger(Main.class);
	private static final String CONF_OUTPUT_ROOT = "hbase2hdfs.output.root";

	private Configuration conf;

	@Override
	public void setConf(Configuration conf) {
		if(this.conf == null){
			this.conf = conf;
		}
	}

	@Override
	public Configuration getConf() {
		if(this.conf == null){
			return HBaseConfiguration.create();
		}
		return this.conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		String hbaseTableName = "";
		String confTableName = "";
		String spec_id = "null";
		String outputPath = "";
		String errOutputPath = "";
		String confPath = "";
		String jobName = "";

		if(args.length > 0){

			for(int i = 0; i<= args.length-1;i++){
				switch(args[i]){
					case "-hbaseTableName":
						hbaseTableName = args[i+1];
						break;
					case "-confTableName":
						confTableName = args[i+1];
						break;
					case "-spec_id":
						spec_id = args[i+1];
						break;
					case "-outputPath":
						outputPath = args[i+1];
						break;
					case "-errOutputPath":
						errOutputPath = args[i+1];
						break;
					case "-confPath" :
						confPath = args[i+1];
						break;
					case "-jobName":
						jobName = args[i+1];
						break;
				}
			}

			if(hbaseTableName.equals("") || confTableName.equals("") || outputPath.equals("") || confPath.equals("") || jobName.equals("")){
				LOG.error("请输入参数");
				System.exit(-1);
			}

			Path path = new Path(confPath);
			conf.addResource(path);
		}else{
			LOG.error("请输入参数");
			System.exit(-1);
		}

		conf.set("conf.confTableName", confTableName);
		conf.set("conf.hbaseTableName", hbaseTableName);
		conf.set("conf.specid", spec_id);
		conf.set("conf.outputPath", outputPath);
		conf.set("conf.errOutputPath", errOutputPath);
		conf.set(CONF_OUTPUT_ROOT, outputPath);
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(Main.class);

		Scan scan = new Scan();
		scan.setMaxVersions(1);
		scan.setCacheBlocks(false);
		TableMapReduceUtil.initTableMapperJob(hbaseTableName, scan, BoncTableMapper.class, NullWritable.class, Text.class, job);

		job.setNumReduceTasks(0);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(outputPath));


		return  job.waitForCompletion(true)? 0 : 1;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Main(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


}
