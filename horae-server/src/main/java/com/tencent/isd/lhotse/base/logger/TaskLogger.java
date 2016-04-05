package com.tencent.isd.lhotse.base.logger;

import java.sql.SQLException;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.tencent.isd.lhotse.base.LDBBroker;
import com.tencent.isd.lhotse.dao.TaskLog;
import com.tencent.isd.lhotse.dao.TaskRun;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;

/**
 * 
 * @author: cpwang
 * @date: 2012-4-17
 */
public class TaskLogger extends DBLogger<TaskLog> {
	private static TaskLogger instance = null;

	public static void init() {
		if (instance == null) {
			instance = new TaskLogger();
			instance.start("TaskLogger");
		}
	}

	private static synchronized void appendLog(String type, LTask task, String broker) {
		TaskLog tl = new TaskLog(type, task, broker);
		instance.appendLog(tl);
	}

	public static synchronized void info(LTask task, String broker) {
		appendLog("info", task, broker);
	}

	public static synchronized void warn(LTask task, String broker) {
		appendLog("warn", task, broker);
	}

	public static synchronized void error(LTask task, String broker) {
		appendLog("error", task, broker);
	}

	private static synchronized void appendLog(String type, TaskRun tr, String desc) {
		TaskLog tl = new TaskLog(type, tr, desc);
		instance.appendLog(tl);
	}

	public static synchronized void warn(TaskRun tr, String desc) {
		appendLog("warn", tr, desc);
	}

	public static synchronized void info(TaskRun tr, String desc) {
		appendLog("info", tr, desc);
	}

	public static synchronized void error(TaskRun tr, String desc) {
		appendLog("error", tr, desc);
	}

	@Override
	protected void insertLogs(LDBBroker broker, ConcurrentLinkedQueue<TaskLog> logs) throws SQLException {
		broker.insertTaskLog(logs);
	}
}
