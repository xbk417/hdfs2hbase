package com.bonc.hbase.maxtimestamp;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.LoggerFactory;

/**
 * 获取 HBase 表中的时间戳最新的一条数据
 * @author xiabaike
 * @date 2016年6月22日
 */
public class MaxTimestamp {
	
	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(MaxTimestamp.class);
	
	final static String NAME = "export";
	final static String RAW_SCAN = "hbase.mapreduce.include.deleted.rows";
	final static String EXPORT_BATCHING = "hbase.export.scanner.batch";
	private static final String CONF_OUTPUT_ROOT = "hbase2hdfs.output.root";
	private static final String OUT_TABLE_NAME = "get.table.name";
	private static final String OUT_FORMAT = "max.out.format";
	private static String separator = System.getProperties().getProperty("file.separator");
	
	public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
		conf.addResource(new Path(args[2]));
//		HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));
		String tableName = args[0];
		conf.set(OUT_TABLE_NAME, tableName);
		String filepath = args[1];
		filepath = filepath.endsWith(separator) ? filepath : (filepath + separator);
		conf.set("output.filepath", filepath + tableName);
		
		Path outputDir = new Path(conf.get("hdfs.tmp.path"));
		String format = conf.get(OUT_FORMAT, "0");
//		if("0".equals(format)) {
//			FileSystem dst = FileSystem.getLocal(conf);
//			dst.delete(outputDir, true);
//		}else{
//			FileSystem dst = FileSystem.get(conf);
//			dst.delete(outputDir, true);
//		}
	    conf.setStrings("io.serializations", conf.get("io.serializations"),
	            MutationSerialization.class.getName(), ResultSerialization.class.getName(),
	            KeyValueSerialization.class.getName());
		conf.set(CONF_OUTPUT_ROOT, args[1]);
		// Set optional scan parameters
		Scan s = getConfiguredScanForJob(conf, args);
		System.out.println(tableName);
		conf.set(TableInputFormat.INPUT_TABLE, tableName);
	    conf.set(TableInputFormat.SCAN, convertScanToString(s));
		Job job = Job.getInstance(conf, args[3]);
		job.setJarByClass(MaxTimestamp.class);
//		MaxTimestampMapper.initJob(tableName, s, MaxTimestampMapper.class, job);
	    job.setInputFormatClass(TableInputFormat.class);
		job.setMapperClass(MaxTimestampMapper.class);
//		job.setCombinerClass(MaxTimestampReducer.class);
		job.setSortComparatorClass(DefinedComparator.class);
		job.setReducerClass(MaxTimestampReducer.class);
//		job.setSortComparatorClass(TimestampComparator.class);
		// No reducers.  Just write straight to output files.
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
		if("0".equals(format)) {
			job.setOutputFormatClass(LocalTextOutputFormat.class);
		}else{
			job.setOutputFormatClass(TextOutputFormat.class);
		}
		
		job.setNumReduceTasks(1);
//		FileOutputFormat.setCompressOutput(job, true);
//		FileOutputFormat.setOutputCompressorClass(job, (Class<? extends CompressionCodec>) conf.getClass("get.table.out.compress", GzipCodec.class));
		FileOutputFormat.setOutputPath(job, outputDir); // job conf doesn't contain the conf so doesn't have a default fs.
		
		return job;
	}
	
	static String convertScanToString(Scan scan) throws IOException {
		ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
		return Base64.encodeBytes(proto.toByteArray());
	}
	
	private static Scan getConfiguredScanForJob(Configuration conf, String[] args) throws IOException {
		Scan s = new Scan();
		// Optional arguments.
		// Set Scan Versions
//		int versions = args.length > 3? Integer.parseInt(args[3]): 1;
		s.setMaxVersions();
		long startTime = 0L;
		long endTime = Long.MAX_VALUE;
		for(int i = 0 ; i < args.length ; i++){
			if("-starttime".equals(args[i])){
				startTime = Long.parseLong(timestamp(args[++i]));
			}else if("-endtime".equals(args[i])){
				endTime = Long.parseLong(timestamp(args[++i]));
			}
		}
		// Set Scan Range
//		long startTime = args.length > 4? Long.parseLong(args[4]): 0L;
//		long endTime = args.length > 5? Long.parseLong(args[5]): Long.MAX_VALUE;
		s.setTimeRange(startTime, endTime);
		// Set cache blocks
		s.setCacheBlocks(false);
		// set Start and Stop row
		if (conf.get(TableInputFormat.SCAN_ROW_START) != null) {
			s.setStartRow(Bytes.toBytes(conf.get(TableInputFormat.SCAN_ROW_START)));
		}
		if (conf.get(TableInputFormat.SCAN_ROW_STOP) != null) {
			s.setStopRow(Bytes.toBytes(conf.get(TableInputFormat.SCAN_ROW_STOP)));
		}
		// Set Scan Column Family
		boolean raw = Boolean.parseBoolean(conf.get(RAW_SCAN));
		if (raw) {
			s.setRaw(raw);
		}

		if (conf.get(TableInputFormat.SCAN_COLUMN_FAMILY) != null) {
			s.addFamily(Bytes.toBytes(conf.get(TableInputFormat.SCAN_COLUMN_FAMILY)));
		}
		// Set RowFilter or Prefix Filter if applicable.
//		Filter exportFilter = getExportFilter(args);
//		if (exportFilter!= null) {
//			LOG.info("Setting Scan Filter for Export.");
//			s.setFilter(exportFilter);
//		}
		
//		int batching = conf.getInt(EXPORT_BATCHING, -1);
//		if (batching !=  -1){
//			try {
//				s.setBatch(batching);
//			} catch (IncompatibleFilterException e) {
//				LOG.error("Batching could not be set", e);
//			}
//		}
		LOG.info("versions=" + 1 + ", starttime=" + startTime +
				", endtime=" + endTime + ", keepDeletedCells=" + raw);
		
		return s;
	}
	
	// 字符串转为时间戳
	public static String timestamp(String time) {
		try {
			String format = "";
			if(time.length() == 8) {
				format = "yyyyMMdd";
			}else if(time.length() == 10) {
				format = "yyyyMMddhh";
			}else if(time.length() == 12) {
				format = "yyyyMMddhhmm";
			}else if(time.length() == 14) {
				format = "yyyyMMddhhmmss";
			}
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            return String.valueOf(sdf.parse(time).getTime());
        } catch (Exception e) {
            e.printStackTrace();
        }
		return null;
	}
	
	private static Filter getExportFilter(String[] args) {
		Filter exportFilter = null;
		String filterCriteria = (args.length > 6) ? args[6]: null;
		if (filterCriteria == null) return null;
		if (filterCriteria.startsWith("^")) {
			String regexPattern = filterCriteria.substring(1, filterCriteria.length());
			exportFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(regexPattern));
		} else {
			exportFilter = new PrefixFilter(Bytes.toBytes(filterCriteria));
		}
		
		return exportFilter;
	}
	
	private static void usage(final String errorMsg) {
		if (errorMsg != null && errorMsg.length() > 0) {
			System.err.println("ERROR: " + errorMsg);
		}
		System.err.println("Usage: Export [-D <property=value>]* <tablename> <outputdir> <configpath> " +
				"-starttime <starttime> -endtime <endtime>]\n");
		System.err.println("  Note: -D properties will be applied to the conf used. ");
		System.err.println("  For example: ");
		System.err.println("   -D mapreduce.output.fileoutputformat.compress=true");
		System.err.println("   -D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec");
		System.err.println("   -D mapreduce.output.fileoutputformat.compress.type=BLOCK");
		System.err.println("  Additionally, the following SCAN properties can be specified");
		System.err.println("  to control/limit what is exported..");
		System.err.println("   -D " + TableInputFormat.SCAN_COLUMN_FAMILY + "=<familyName>");
		System.err.println("   -D " + RAW_SCAN + "=true");
		System.err.println("   -D " + TableInputFormat.SCAN_ROW_START + "=<ROWSTART>");
		System.err.println("   -D " + TableInputFormat.SCAN_ROW_STOP + "=<ROWSTOP>");
		System.err.println("For performance consider the following properties:\n"
				+ "   -Dhbase.client.scanner.caching=100\n"
				+ "   -Dmapreduce.map.speculative=false\n"
				+ "   -Dmapreduce.reduce.speculative=false");
		System.err.println("For tables with very wide rows consider setting the batch size as below:\n"
				+ "   -D" + EXPORT_BATCHING + "=10");
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
//		conf.set("hbase.zookeeper.quorum", "NM-304-SA5212M4-BIGDATA-101,NM-304-SA5212M4-BIGDATA-102,NM-304-SA5212M4-BIGDATA-103,NM-304-SA5212M4-BIGDATA-104,NM-304-SA5212M4-BIGDATA-105");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3) {
			usage("Wrong number of arguments: " + otherArgs.length);
			System.exit(-1);
		}
		Job job = createSubmittableJob(conf, otherArgs);
		int wait = job.waitForCompletion(true)? 0 : 1;
		HDFSUtil.copyToLocalFileSystem(conf);
		System.exit(wait);
	}

}
