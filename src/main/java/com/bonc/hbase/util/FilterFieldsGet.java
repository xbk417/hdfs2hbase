package com.bonc.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 查询数据库
 * Created by xiabaike on 2016/10/31.
 */
public class FilterFieldsGet {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(FilterFieldsGet.class);
    private static final String TABLE_NAME = "get.table.name";
    private DBUtil dbUtil = null;
    private String tableName = null;

    public FilterFieldsGet(Configuration conf) {
        dbUtil = new DBUtil(conf);
        tableName = conf.get(TABLE_NAME);
    }

    /**
     * 查询出指定表中需要过滤的字段
     */
    public String getFilterFields() {
        String sql = "select attribute_code from conf_oss_table where table_code= ? ";
        Connection conn = null;
        StringBuilder sb = new StringBuilder();
        String fields = null;
        try {
            conn = dbUtil.getConnection();
            PreparedStatement prep = conn.prepareStatement(sql);
            prep.setString(1, tableName);
            ResultSet rs = prep.executeQuery();
            while(rs.next()) {
                sb.append(rs.getString("attribute_code")).append("\t");
            }
            fields = sb.toString();
            fields = fields.substring(0, fields.length() - 1);
        } catch (SQLException e) {
            logger.error("查询失败，请检查");
            e.printStackTrace();
        } finally{
            // 关闭数据库连接
            dbUtil.close(conn);
        }
        return fields;
    }

    public static void main(String[] args) {
        String str = "A\tB\tC\tD\tAA";
        System.out.println(str.substring(0, str.length() - 1));
    }

}
