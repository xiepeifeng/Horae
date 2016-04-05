package com.tencent.isd.lhotse.runner.util;

import org.apache.commons.lang.StringUtils;

import java.sql.SQLException;

public class DBUtil {

	public static String getConnectionString(String dbType, String host, int port, String service) throws SQLException {
		if (StringUtils.isBlank(dbType))
			return null;
		dbType = dbType.trim().toLowerCase();

		String url = null;
		if (dbType.equals("oracle_rac")) {
			url = "jdbc:oracle:thin:@" + host;
		}
		else if (dbType.equals("oracle_oci")) {
			url = "jdbc:oracle:oci:@" + host;
		}
		else if (dbType.contains("oracle")) {
			url = "jdbc:oracle:thin:@" + host + ":" + port + "/" + service;
		}
		else if (dbType.contains("oracle2")) {
			url = "jdbc:oracle:thin:@" + host + ":" + port + ":" + service;
		}
		else if (dbType.contains("mysql")) {
			String tmp = "?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true";
			url = "jdbc:mysql://" + host + ":" + port + "/" + service + tmp;
		}
		else if (dbType.startsWith("tdw") || dbType.startsWith("hive")) {
			url = "jdbc:hive://" + host + ":" + port + "/" + service;
		}
		else if (dbType.contains("postgre") || dbType.contains("postgresql")) {
			url = "jdbc:postgresql://" + host + ":" + port + "/" + service;
		}
		else if (dbType.contains("sqlserver")) {
			// jdbc: sqlserver: // 10.145.172.45:1433;DatabaseName=BlGame_dr
			url = "jdbc:sqlserver://" + host + ":" + port + ";DatabaseName=" + service;
		}
		else {
			throw new SQLException("Unsupported database type:" + dbType);
		}
		return url;
	}

	public static String getDriverString(String dbType) {
		if (StringUtils.isBlank(dbType))
			return null;
		dbType = dbType.trim().toLowerCase();

		String driverStr = null;
		if (dbType.contains("oracle")) {
			driverStr = "oracle.jdbc.driver.OracleDriver";
		}
		else if (dbType.contains("mysql")) {
			driverStr = "com.mysql.jdbc.Driver";
		}
		else if (dbType.contains("postgre") || dbType.contains("postgresql")) {
			driverStr = "org.postgresql.Driver";
		}
		else if (dbType.startsWith("tdw") || dbType.startsWith("hive")) {
			driverStr = "org.apache.hadoop.hive.jdbc.HiveDriver";
		}
		else if (dbType.contains("sqlserver")) {
			driverStr = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		}
		return driverStr;
	}
}
