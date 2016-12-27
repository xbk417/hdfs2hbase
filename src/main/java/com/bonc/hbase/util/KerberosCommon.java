package com.bonc.hbase.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * 添加 kerberos 认证
 * @author xiabaike
 * @date 2016年10月17日
 */
public class KerberosCommon {
	
	public static FileSystem kerberos(Configuration conf) {
		boolean KRB_USE = conf.getBoolean("krb.use", false);
		String KRB_KEYSTORE = conf.get("krb.ketsotre");
		String KRB_PRINCPAL = conf.get("krb.princpal");
		FileSystem dfs = null;
		try {
			if(!KRB_USE){
				System.out.println("........................login from sample ......................................");
				dfs = (DistributedFileSystem) FileSystem.get(conf);
			}else {
				System.out.println("........................login from kerberos ......................................");
				conf.set("hadoop.security.authentication", "Kerberos");
				UserGroupInformation.setConfiguration(conf);
				UserGroupInformation.loginUserFromKeytab(KRB_PRINCPAL,KRB_KEYSTORE);
				System.out.println(String.format("hdfs uri : %s ----------", conf.get("fs.defaultFS")));
				dfs = (DistributedFileSystem) FileSystem.get(conf);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		 return dfs;
	}
	
}
