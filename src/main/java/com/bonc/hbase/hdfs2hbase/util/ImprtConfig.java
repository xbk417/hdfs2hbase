package com.bonc.hbase.hdfs2hbase.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

/**
 * 解析XML文件
 * @author xiabaike
 * @date 2016年4月1日
 */
public class ImprtConfig {
	
	private static String IMPORT_TABLE_NAME = "import.table.name";
	private static String IMPORT_TABLE_FAMILY = "import.table.family";
	private static String IMPORT_TABLE_COLUMNS = "import.table.columns";
	private static String IMPORT_TABLE_ROWKEY = "import.table.rowkey";
	
	private Map<String, Object> xmlMap = new HashMap<String, Object>();
	
	public ImprtConfig(String filepath) {
		parseXml(filepath);
	}
	
	public Map<String, Object> parseXml(String filepath) {
		SAXBuilder builder = new SAXBuilder();
        Document doc;
        Element root;
		try {
			doc = builder.build(new File(filepath));
			root = doc.getRootElement();
			List<Element> importList = root.getChildren("table");
			for(Element ele : importList) {
				String name = ele.getAttributeValue("name");
				xmlMap.put(IMPORT_TABLE_NAME, name);
				String family = ele.getAttributeValue("family");
				xmlMap.put(IMPORT_TABLE_FAMILY, family);
				List<Element> columnList = ele.getChildren();
				if(columnList != null && columnList.size() > 0) {
					List<String> columns = new ArrayList<String>();
					for(int i = 0; i < columnList.size(); i++) {
						Element column = columnList.get(i);
						String indexStr = column.getAttributeValue("index");
						if(indexStr != null && !"".equals(indexStr)) {
							columns.add(Integer.parseInt(indexStr), column.getTextTrim());
						}else{
							columns.add(i, column.getTextTrim());
						}
						String isPrimarykey = column.getAttributeValue("isPrimarykey");
						if(isPrimarykey != null && !"".equals(isPrimarykey) && "true".equals(isPrimarykey)) {
							xmlMap.put(IMPORT_TABLE_ROWKEY, column.getTextTrim());
						}
					}
					xmlMap.put(IMPORT_TABLE_COLUMNS, columns);
				}
				break;
			}
			
			List<Element> configList = root.getChildren("property");
			for(Element ele : configList) {
				xmlMap.put(ele.getChildText("name"), ele.getChildText("value"));
			}
			
			return xmlMap;
		} catch (JDOMException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public Object get(String key) {
		return xmlMap.get(key);
	}
	
	public String getString(String key) {
		return (String) xmlMap.get(key);
	}
	
	public String getString(String key, String defaultValue) {
		String value = getString(key);
		if(value == null || "".equals(value)) {
			return defaultValue;
		}
		return value;
	}
	
	public int getInt(String key) {
		String value = getString(key);
		return (value == null || "".equals(value)) ? 0 : Integer.parseInt(value);
	}
	
	public int getInt(String key, int defaultValue) {
		String value = getString(key);
		if(value == null || "".equals(value)) {
			return defaultValue;
		}
		return Integer.parseInt(value);
	}
	
	public List<String> getList(String key) {
		return (List<String>) xmlMap.get(key);
	}
	
	public Map<String, Object> getMap() {
		return xmlMap;
	}
	
	public static void main(String[] args) {
		ImprtConfig xml = new ImprtConfig("src\\main\\resources\\import.xml");
		System.out.println(xml.getMap());
	}
	
}
