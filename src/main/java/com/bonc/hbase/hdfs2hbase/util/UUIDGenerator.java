package com.bonc.hbase.hdfs2hbase.util;

import java.util.UUID;

/**
 * 生成全局唯一ID
 * */
public class UUIDGenerator {

	public static String getUUID() {  
        UUID uuid = UUID.randomUUID();  
        String str = uuid.toString().toUpperCase();  
        
        return str;
    }
	
	// 去掉UUID中的‘-’符号
	public static String getUUIDEasy() {  
        UUID uuid = UUID.randomUUID();  
        String str = uuid.toString().toUpperCase();  
        // 去掉"-"符号  
        String temp = str.replaceAll("-", "");  
        return temp;
    }
	
	public static void main(String[] args) {
		System.out.println(getUUID());
		System.out.println(getUUIDEasy());
	}
}
