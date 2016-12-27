package com.bonc.hbase.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.LoggerFactory;

/**
 * 获取数据库连接
 * Created by xiabaike on 2016/10/31.
 */
public class DBUtil {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DBUtil.class);
    private static final String DRIVER_NAME = "hbase2hdfs.db.driver.class";
    private static final String URL = "hbase2hdfs.db.url";
    private static final String USER = "hbase2hdfs.db.user";
    private static final String PASSWORD = "hbase2hdfs.db.password";
    private static String driverName;
    private static String url;
    private static String user;
    private static String password;

    public DBUtil(Configuration conf) {
        try {
            // 从配置文件中获取数据库连接驱动
            driverName = conf.get(DRIVER_NAME);
            // 从配置文件中获取数据库连接地址
            url = conf.get(URL);
            // 从配置文件中获取数据库用户名
            user = conf.get(USER);
            // 从配置文件中获取数据库用户名密码
            password = conf.get(PASSWORD);
            logger.info("数据库连接驱动：{}, 连接地址：{}, 数据库用户名：{}, 数据库密码：{} ", driverName, url, user, password);
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            logger.error("请检查连接驱动 {} 是否正确或者相应jar包是否存在", driverName, e);
        }
    }

    // 获得Connection对象所消耗资源会占到整个jdbc操作的85%以上
    // 批处理除外
    // 尽量减少获得Connection对象
    public Connection getConnection() {
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void close(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
