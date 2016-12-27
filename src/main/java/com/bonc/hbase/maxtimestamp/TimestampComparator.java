package com.bonc.hbase.maxtimestamp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * @author xiabaike
 * @date 2016年6月8日
 */
public class TimestampComparator extends WritableComparator{

	protected TimestampComparator() {
        super(IntWritable.class,true);
    }
	
    public int compare(WritableComparable a, WritableComparable b) {
        return -super.compare(a, b);
    }
	
}


	