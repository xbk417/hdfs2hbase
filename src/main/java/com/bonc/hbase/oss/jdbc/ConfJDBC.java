package com.bonc.hbase.oss.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfJDBC {
	private static final Logger LOG = LoggerFactory.getLogger(ConfJDBC.class);

	public static ArrayList<String> selectConf(String driverClassName, String dbUrl, String dbUserName, String dbPassWord,String congTableName) {

		ArrayList<String> list = new ArrayList<String>();

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			Class.forName(driverClassName);
			conn = DriverManager.getConnection(dbUrl, dbUserName, dbPassWord);

			ps = conn.prepareStatement("select attribute_code from conf_oss_table where table_code=?");
			ps.setString(1, congTableName);
			rs = ps.executeQuery();

			while (rs.next()) {
				list.add(rs.getString(1));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (conn != null) {
					conn.close();
				}
				if (ps != null) {
					ps.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return list;
	}

}
