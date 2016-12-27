package com.bonc.hbase.hdfs2hbase.util;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

public class Hdfs2HBaseConfiguration {
    
    public Hdfs2HBaseConfiguration() {
    	// 初始化
    	addResource("/hdfs2hbase.properties");
    }
    
    public Hdfs2HBaseConfiguration(String configPath) {
    	// 初始化
    	addResource(configPath);
    }
    
    private static Map<String, String> map = new HashMap<String, String>();
    
    public void addResource(String configPath) {
    	InputStream in = null;
    	Properties prop = new Properties();
    	try {
    		in = new BufferedInputStream (new FileInputStream(configPath));
    		prop.load(in);
    	    Set<Entry<Object, Object>> set = prop.entrySet();
    	    for(Entry<Object, Object> entry : set) {
    	    	map.put((String)entry.getKey(), (String)entry.getValue());
    	    }
    	} catch (FileNotFoundException e) {
    		in = new BufferedInputStream(Hdfs2HBaseConfiguration.class.getClass().getResourceAsStream(configPath));
    		try {
				prop.load(in);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
    	    Set<Entry<Object, Object>> set = prop.entrySet();
    	    for(Entry<Object, Object> entry : set) {
    	    	map.put((String)entry.getKey(), (String)entry.getValue());
    	    }
    	} catch (IOException e) {
    		e.printStackTrace();
    	}finally{
			if(in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
    	}
    }
    
    public String get(String key){
    	return map.get(key);
    }
    
    public String get(String key, String defaultValue) {
    	String value = map.get(key);
    	if( "".equals(value) && value == null ) {
    		return defaultValue;
    	}
    	return value;
    }

	public static void main(String[] args) {
		Hdfs2HBaseConfiguration config = new Hdfs2HBaseConfiguration();
		config.addResource("D:\\hdfs2hbase.properties");
		System.out.println("------ :  "+config.get("app.retry.times"));
		System.out.println("------ :  "+config.get("xbkxbxkbkxbkxbk"));
	}
}
