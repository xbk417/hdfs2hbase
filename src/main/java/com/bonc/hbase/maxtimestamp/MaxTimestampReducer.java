package com.bonc.hbase.maxtimestamp;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author xiabaike
 * @date 2016年6月8日
 */
public class MaxTimestampReducer extends Reducer<LongWritable, Text, LongWritable, Text>{
	
	private long outNum = 1L;
	
	  protected void setup(Context context) throws IOException, InterruptedException {
		  outNum = context.getConfiguration().getLong("max.out.num", 1L);
	  }
	
	protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Iterator<Text> iterator = values.iterator();
		while(iterator.hasNext()) {
			context.write(key, iterator.next());
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
	}
	
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
	    try {
	    	long i = 1;
	    	while (context.nextKey()) {
		        reduce(context.getCurrentKey(), context.getValues(), context);
		        // If a back up store is used, reset it
		        Iterator<Text> iter = context.getValues().iterator();
		        if(iter instanceof ReduceContext.ValueIterator) {
		          ((ReduceContext.ValueIterator<Text>)iter).resetBackupStore();        
		        }
		        if( i >= outNum ) {
		        	break;
		        }else{
		        	i++;
		        }
	    	}
	    } finally {
	      cleanup(context);
	    }
	}
	
}
