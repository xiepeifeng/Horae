package com.tencent.isd.lhotse.base.logger;

import java.sql.SQLException;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.tencent.isd.lhotse.base.LDBBroker;
import com.tencent.isd.lhotse.dao.DBLog;

/**
 * 
 * @author: cpwang
 * @date: 2012-4-17 cpwang,20120824, Wildcard type
 */
public abstract class DBLogger<T extends DBLog> implements Runnable {
    Log logger=LogFactory.getLog(DBLogger.class);
	public static final String INFO = "info";
	public static final String WARN = "warn";
	public static final String ERROR = "error";
	private LDBBroker broker = null;
	private Thread thread;
	private final long LOG_FLUSH_INTERVAL = 10000L;
	private static final long MAX_ALIVE_TIME = 3600000;// 1 hour

	private ConcurrentLinkedQueue<T> logsToFlush = new ConcurrentLinkedQueue<T>();
	private ConcurrentLinkedQueue<T> logsToWrite = new ConcurrentLinkedQueue<T>();

	public void start(String name) {
		if (this.thread == null) {
			this.thread = new Thread(this, name);
			this.thread.start();
		}
	}

	public void stop() {
		if (this.thread != null)
			this.thread.interrupt();
	}

	protected synchronized void appendLog(T sl) {
		// change to ConcurrentLinkedQueue
		logsToWrite.offer(sl);
	}

	protected abstract void insertLogs(LDBBroker broker, ConcurrentLinkedQueue<T> logs) throws SQLException;

	private synchronized int flushLogs(LDBBroker broker) throws SQLException, ClassNotFoundException {
		ConcurrentLinkedQueue<T> tmpLogs = logsToWrite;
		logsToWrite = logsToFlush;
		logsToFlush = tmpLogs;
		int logCnt = 0;
		broker.refreshConnection();
		insertLogs(broker, logsToFlush);
		logsToFlush.clear();
		return logCnt;
	}

	public void run() {
		final int MAX_CHECK_COUNT = (int) (MAX_ALIVE_TIME / LOG_FLUSH_INTERVAL);

		int checkCount = 0;
		while (true) {
			if (logsToWrite.size() <= 0) {
				// no log, increase count
				++checkCount;
			}
			else {
				// reset counter
				checkCount = 0;
				try {
					if ((broker == null) || (broker.isClosed())) {
						this.broker = new LDBBroker(false);// standby
					}
					flushLogs(this.broker);
				}
				catch (SQLException e) {
				    logger.error(e);
				}
				catch (ClassNotFoundException e) {
				    logger.error(e);
				}
				catch (Exception e) {
					e.printStackTrace();
					logger.error(e);
				}
			}

			if (checkCount >= MAX_CHECK_COUNT) {
				// too long not to record log, close connection
				if (broker != null) {
					broker.close();
					broker = null;
				}
				checkCount = 0;
			}

			try {
				Thread.sleep(LOG_FLUSH_INTERVAL);
			}
			catch (InterruptedException e) {
			}
		}
	}
}
