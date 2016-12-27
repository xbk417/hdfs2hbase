package com.bonc.hbase.hbase2hdfs;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 
 * @author xiabaike
 * @date 2016年5月22日
 */
public class HBase2HdfsOutputFormat<K, V> extends TextOutputFormat<K, V>{
	
	private static final String CONF_OUTPUT_ROOT = "hbase2hdfs.output.root";
	
    private static Path outputRoot;
    
//    static{
//    	try {
//            conf.setBoolean("fs." + outputRoot.toUri().getScheme() + ".impl.disable.cache", true);
//            outputFs = FileSystem.get(outputRoot.toUri(), conf);
//          } catch (IOException e) {
//            throw new IOException("Could not get the output FileSystem with root="+ outputRoot, e);
//          }
//    }
    
    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
    	Configuration conf = job.getConfiguration();
    	outputRoot = new Path(conf.get(CONF_OUTPUT_ROOT));
    	conf.setBoolean("fs." + outputRoot.toUri().getScheme() + ".impl.disable.cache", true);
    	
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
        FileSystem fs = FileSystem.get(outputRoot.toUri(), conf);//file.getFileSystem(conf);
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

}


	