package com.bonc.hbase.hdfs2hbase;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonc.hbase.hdfs2hbase.util.Hdfs2HBaseConfiguration;
import com.bonc.hbase.hdfs2hbase.util.UUIDGenerator;

public class HBaseImport {
	
	private static final Logger LOG = LoggerFactory.getLogger(HBaseImport.class);
	
    //读取HDFS中的数据源，解析产生row key
    static class BatchImportMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
        Text v2 = new Text();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        @Override  
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, LongWritable, Text>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            // 产生行键
            String rowKeyString = UUIDGenerator.getUUIDEasy();
            v2.set(rowKeyString+"|"+line);
            context.write(key, v2);
        }
    }
    
    //把数据插入到HBase表中
    static class BatchImportReducer extends TableReducer<LongWritable, Text, NullWritable>{  
        public static String COLUMN_FAMILY = "cf";  
        
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	COLUMN_FAMILY = config.get("hdfs2hbase.column.family");
        }
        
        protected void reduce(LongWritable arg0, Iterable<Text> v2s,
                Reducer<LongWritable, Text, NullWritable, Mutation>.Context context)
                throws IOException, InterruptedException {
        	 Put put = null;
            for (Text v2 : v2s) {
                String[] splited = v2.toString().split("\\|");
                String rowKey = splited[0];

                put = new Put(rowKey.getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "IMSI".getBytes(), splited[1].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "MSISDN".getBytes(), splited[2].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "IMEI".getBytes(), splited[3].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "APN".getBytes(), splited[4].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "DestinationIP".getBytes(), splited[5].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "DestinationPort".getBytes(), splited[6].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "SourceIP".getBytes(), splited[7].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "SourcePort".getBytes(), splited[8].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "SGWIP".getBytes(), splited[9].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "MMEIP".getBytes(), splited[10].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "PGWIP".getBytes(), splited[11].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "SAI".getBytes(), splited[12].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "TAI".getBytes(), splited[13].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "VisitedPLMNId".getBytes(), splited[14].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "RATType".getBytes(), splited[15].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "ProtocolID".getBytes(), splited[16].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "ServiceType".getBytes(), splited[17].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "StartTime".getBytes(), splited[18].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "EndTime".getBytes(), splited[19].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "Duration".getBytes(), splited[20].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "InputOctets".getBytes(), splited[21].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "OutputOctets".getBytes(), splited[22].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "InputPacket".getBytes(), splited[23].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "OutputPacket".getBytes(), splited[24].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "PDNConnectionI".getBytes(), splited[25].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "BearerID".getBytes(), splited[26].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "BearerQoS".getBytes(), splited[27].getBytes());
                put.addColumn(COLUMN_FAMILY.getBytes(), "RecordCloseCause".getBytes(), splited[28].getBytes());

                context.write(NullWritable.get(), put);
            }
        }
    }

    public static void main(String[] args) throws Exception {
		String usage = "Usage:AuditFileNameTopology [-configPath <configPath> -inputPath <inputPath> -tableName <tableName> -columnFamily <columnFamily>]";
		String configPath = null;
		String tableName = null;
		String inputPath = null;
		String columnFamily = null;
		for(int i = 0 ; i < args.length ; i++){
			if("-configPath".equals(args[i])){
				configPath = args[++i];
			}else if("-tableName".equals(args[i])){
				tableName = args[++i];
			}else if("-inputPath".equals(args[i])){
				inputPath = args[++i];
			}else if("-columnFamily".equals(args[i])){
				columnFamily = args[++i];
			}
		}
		
		if( "".equals(tableName) || tableName == null ) {
			System.err.println("参数 tableName 不能为空");
			System.err.println(usage);
			LOG.error("参数 tableName 不能为空");
			System.exit(-1);
		}
		
		if( "".equals(columnFamily) || columnFamily == null ) {
			System.err.println("参数 columnFamily 不能为空");
			System.err.println(usage);
			LOG.error("参数 columnFamily 不能为空");
			System.exit(-1);
		}
		
		if( "".equals(inputPath) || inputPath == null ) {
			System.err.println("参数 inputPath 不能为空");
			System.err.println(usage);
			LOG.error("参数 inputPath 不能为空");
			System.exit(-1);
		}
		
		Hdfs2HBaseConfiguration config = null;
		if( !"".equals(configPath) && configPath != null ) {
			config = new Hdfs2HBaseConfiguration(configPath);
		}else{
			config = new Hdfs2HBaseConfiguration();
		}
		
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.set("fs.defaultFS", config.get("hdfsUrl"));
        configuration.set("hdfs2hbase.column.family", columnFamily);
        //设置zookeeper
        configuration.set("hbase.zookeeper.quorum", config.get("hbase.zookeeper.quorum"));
        System.out.println(config.get("hbase.zookeeper.quorum"));
        //设置hbase表名称
        configuration.set(TableOutputFormat.OUTPUT_TABLE, tableName);
        //将该值改大，防止hbase超时退出
        configuration.set("dfs.socket.timeout", config.get("dfs.socket.timeout", "180000"));

        Job job = Job.getInstance(configuration, "HBaseBatchImport");

        //当打包成jar运行时，必须有以下2行代码  
        TableMapReduceUtil.addDependencyJars(job);
        job.setJarByClass(HBaseImport.class);
        
        job.setMapperClass(BatchImportMapper.class);
        job.setReducerClass(BatchImportReducer.class);
        //设置map的输出，不设置reduce的输出类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Mutation.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        //不再设置输出路径，而是设置输出格式类型
        job.setOutputFormatClass(TableOutputFormat.class);

        FileInputFormat.setInputPaths(job, inputPath);

        job.waitForCompletion(true);
    }
}
