package com.bonc.hbase.maxtimestamp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * @author xiabaike
 * @date 2016年6月22日
 */
public class DefinedComparator extends WritableComparator{

	public DefinedComparator(){
		super(LongWritable.class, true);
	}
	
	@Override
    public int compare(WritableComparable combinationKeyOne, WritableComparable CombinationKeyOther) {
		LongWritable firstKey = (LongWritable) combinationKeyOne;
		LongWritable sencondKey = (LongWritable) CombinationKeyOther;
		return sencondKey.compareTo(firstKey);
	}
}


	