package com.bonc.hbase.maxtimestamp;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author xiabaike
 * @date 2016年6月27日
 */
public class LocalTextOutputFormat<K, V> extends TextOutputFormat<K, V>{

	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LocalTextOutputFormat.class);
	
	public synchronized static String getUniqueFile(TaskAttemptContext context,
            String name,
            String extension) {
		String tablename = context.getConfiguration().get("get.table.name");
//		tablename = tablename + extension;
		return tablename;
	}
	
	  @Override
	  public void checkOutputSpecs(JobContext job
              ) throws FileAlreadyExistsException, IOException{
		// Ensure that the output directory is set and not already there
		Path outDir = getOutputPath(job);
		if (outDir == null) {
		throw new InvalidJobConfException("Output directory not set.");
		}
		
		// get delegation token for outDir's file system
		TokenCache.obtainTokensForNamenodes(job.getCredentials(),
		new Path[] { outDir }, job.getConfiguration());
		
//		if (outDir.getFileSystem(job.getConfiguration()).exists(outDir)) {
//		throw new FileAlreadyExistsException("Output directory " + outDir + 
//		                          " already exists");
//		}
	}
	  
	  public Path getDefaultWorkFile(TaskAttemptContext context,
              String extension) throws IOException{
		FileOutputCommitter committer = 
		(FileOutputCommitter) getOutputCommitter(context);
		return new Path(committer.getWorkPath(), getUniqueFile(context, 
		getOutputName(context), extension));
	}
	
	  public RecordWriter<K, V> 
      getRecordWriter(TaskAttemptContext job
                      ) throws IOException, InterruptedException {
		 Configuration conf = job.getConfiguration();
		 boolean isCompressed = getCompressOutput(job);
		 String keyValueSeparator= conf.get(SEPERATOR, "\t");
		 CompressionCodec codec = null;
		 String extension = "";
		 if (isCompressed) {
		   Class<? extends CompressionCodec> codecClass = 
		     getOutputCompressorClass(job, GzipCodec.class);
		   codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
		   extension = codec.getDefaultExtension();
		 }
		 Path file = getDefaultWorkFile(job, extension);
		 FileSystem fs = file.getFileSystem(conf);
		 if (!isCompressed) {
		   FSDataOutputStream fileOut = fs.create(file, false);
		   return new LineRecordWriter<K, V>(fileOut, keyValueSeparator);
		 } else {
		   FSDataOutputStream fileOut = fs.create(file, false);
		   return new LineRecordWriter<K, V>(new DataOutputStream
		                                     (codec.createOutputStream(fileOut)),
		                                     keyValueSeparator);
		 }
	}
	  
	public RecordWriter<K, V>  getRecordWriter22(TaskAttemptContext job) throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		boolean isCompressed = getCompressOutput(job);
		String keyValueSeparator= conf.get(SEPERATOR, "\t");
		CompressionCodec codec = null;
		String extension = "";
		if (isCompressed) {
		 Class<? extends CompressionCodec> codecClass = 
		   getOutputCompressorClass(job, GzipCodec.class);
		 codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
		 extension = codec.getDefaultExtension();
		}
		String filepath = conf.get("output.filepath");
		Path file = new Path(filepath);//getDefaultWorkFile(job, extension);
		
		FileSystem fs = file.getFileSystem(conf).getLocal(conf);
//		fs.lol
		if (!isCompressed) {
		 FSDataOutputStream out = fs.create(file, false);
//			File filee =	createfile(filepath);
//		 FileOutputStream out = new FileOutputStream(filee);
		 return new LineRecordWriter<K, V>(new DataOutputStream(out), keyValueSeparator);
		} else {
		 FSDataOutputStream out = fs.create(file, false);
//			createfile(filepath+"."+ extension);
//			FileOutputStream out = new FileOutputStream(filepath+"."+ extension);
		 return new LineRecordWriter<K, V>(new DataOutputStream
		                                   (codec.createOutputStream(out)),
		                                   keyValueSeparator);
		}
	}
	
	private static File createfile(String filename) {
		File file = new File(filename);
		if(!file.exists()){
			try {
				file.createNewFile();
				LOG.info(filename + "创建......");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return file;
	}
	
}
