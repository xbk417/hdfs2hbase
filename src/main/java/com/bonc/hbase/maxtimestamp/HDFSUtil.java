package com.bonc.hbase.maxtimestamp;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 
 * @author xiabaike
 * @date 2016年6月28日
 */
public class HDFSUtil {
	
	private static String separator = System.getProperties().getProperty("file.separator");

	public static void copyToLocalFileSystem(Configuration conf) {
		FileSystem fs = null;
		FSDataInputStream fsdi = null;
		OutputStream output = null;
		try {
			String src = conf.get("hdfs.tmp.path");
			if(src.endsWith(separator)) {
				src = src + conf.get("get.table.name");
			}else{
				src = src + separator + conf.get("get.table.name");
			}
			System.out.println("src: "+ src);
			System.out.println("dst: "+ conf.get("output.filepath"));
			fs = FileSystem.get(conf);
//			fs.copyToLocalFile(true, new Path(src), new Path(conf.get("output.filepath")));
			
			fsdi = fs.open(new Path(src));
			output = new FileOutputStream(conf.get("output.filepath"));
			IOUtils.copyBytes(fsdi,output,4096,true);
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				if(fs != null) {
					fs.close();
				}
				if(fsdi != null) {
					fsdi.close();
				}
				if(output != null) {
					output.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
}