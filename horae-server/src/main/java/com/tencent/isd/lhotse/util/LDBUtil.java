package com.tencent.isd.lhotse.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author cpwang
 * @since 2012-3-23
 */
public class LDBUtil {
	public static final String ORACLE_DIRVER = "oracle.jdbc.driver.OracleDriver";
	public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	public static final String HIVE_DRIVER = "org.apache.hadoop.hive.jdbc.HiveDriver";
	public static final String POSTGRE_DRIVER = "org.postgresql.Driver";

	public static Connection getConnection(String type, String host, int port, String service, String user, String pswd)
			throws ClassNotFoundException, SQLException {
		String driver = null;
		String url = null;
		// String type = server.getType().toLowerCase();
		if (type.equalsIgnoreCase("oracle")) {
			driver = ORACLE_DIRVER;
			url = "jdbc:oracle:thin:@" + host + ":" + port + ":" + service;
		}
		else if (type.equalsIgnoreCase("oracle_rac")) {
			driver = ORACLE_DIRVER;
			url = "jdbc:oracle:thin:@" + host;
		}
		else if (type.equalsIgnoreCase("mysql")) {
			driver = MYSQL_DRIVER;
			url = "jdbc:mysql://" + host + ":" + port + "/" + service
					+ "?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull";
		}
		else if (type.startsWith("tdw") || type.startsWith("hive")) {
			driver = HIVE_DRIVER;
			url = "jdbc:hive://" + host + ":" + port + "/" + service;
		}
		else if (type.startsWith("postgre")) {
			driver = POSTGRE_DRIVER;
			url = "jdbc:postgresql://" + host + ":" + port + "/" + service;
		}
		else {
			throw new SQLException("Unsupported database type:" + type);
		}
		return getConnection(driver, url, user, pswd);
	}

	public static Connection getConnection(String driver, String url, String user, String pswd)
			throws ClassNotFoundException, SQLException {
		Class.forName(driver);
		return DriverManager.getConnection(url, user, pswd);
	}
}
