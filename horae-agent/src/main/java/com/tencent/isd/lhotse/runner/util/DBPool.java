package com.tencent.isd.lhotse.runner.util;

import com.tencent.isd.lhotse.runner.AbstractTaskRunner;
import org.apache.log4j.Logger;
import snaq.db.ConnectionPool;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;

public class DBPool {

	private static final Logger log = Logger.getLogger(DBPool.class);

	private static ConnectionPool biPools = null;
	private static boolean init = false;

	public static void init(String pgURL,String pgUser, String pgPasswd, int minPool, int maxPool, int maxSize, int idleTime, AbstractTaskRunner runner) {
		if (!init) {
			try {
				Class<?> bic = Class.forName("org.postgresql.Driver");
				Driver biDriver = (Driver) bic.newInstance();
				DriverManager.registerDriver(biDriver);
			}
			catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			runner.writeLocalLog(Level.INFO, "totalURL:" + pgURL);
			biPools = new ConnectionPool("BIRunnerPool", minPool, maxPool, maxSize, idleTime, pgURL, pgUser,
					pgPasswd);
			init = true;

		}
	}

	private DBPool() {
	}

	/* Establish a connection to BI database. */
	public synchronized static Connection getBIConnection() throws SQLException,
			ClassNotFoundException {
		return biPools.getConnection();
	}

}
