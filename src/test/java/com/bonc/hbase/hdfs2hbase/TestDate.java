package com.bonc.hbase.hdfs2hbase;

import java.text.SimpleDateFormat;

/**
 * 
 * @author xiabaike
 * @date 2016年5月30日
 */
public class TestDate {

	public static String timestamp(String time, String format) {
		try {  
            SimpleDateFormat sdf = new SimpleDateFormat(format);  
            return String.valueOf(sdf.parse(time).getTime());  
        } catch (Exception e) {  
            e.printStackTrace();  
        }
		return null;
	}
	
	public static void main(String[] args) {
		int[] arr = {1,2,3,4,5};
		int[] tmp = arr;
		int index = 0;
		for(int i = 0; i < arr.length; i++) {
			for(int j = 0; j < arr.length; j++) {

			}
		}
	}
	
}
