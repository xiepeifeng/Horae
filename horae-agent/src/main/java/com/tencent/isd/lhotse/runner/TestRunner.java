package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.AbstractTaskRunner;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.util.CommonUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.logging.Level;

public class TestRunner extends AbstractTaskRunner {
	public static final String MYSQL_CONN_STR_USP = "jdbc:mysql://10.130.72.99:3306/uspdb?useUnicode=true&characterEncoding=utf-8";
	public static final String MYSQL_USER_USP = "test";
	public static final String MYSQL_PASSWORD_USP = "123456";
	public static Connection conn_usp = null;

	public static final String MYSQL_CONN_STR_LHOTSE = "jdbc:mysql://10.153.130.82:3306/lhotse?useUnicode=true&characterEncoding=utf-8";
	public static final String MYSQL_USER_LHOTSE = "test";
	public static final String MYSQL_PASSWORD_LHOTSE = "123456";

	public static Connection conn_lhotse = null;
	private static QueryRunner query = new QueryRunner();

	static {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn_usp = DriverManager.getConnection(MYSQL_CONN_STR_USP, MYSQL_USER_USP, MYSQL_PASSWORD_USP);
			conn_lhotse = DriverManager.getConnection(MYSQL_CONN_STR_LHOTSE, MYSQL_USER_LHOTSE, MYSQL_PASSWORD_LHOTSE);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		String taskType = args[0];
		int taskTypeId = Integer.parseInt(taskType);
		TaskRunnerLoader.startRunner(TestRunner.class, (byte) taskTypeId);
	}

	@Override
	public void execute() throws IOException {
		LTask task = getTask();
		String taskId = task.getId();
		long taskTime = -1;
		boolean exceptionFound = false;
		try {
			this.writeLocalLog(Level.INFO, "=====================");
			String startLogString = "Start running TestRunner:" + task.getId() + ", date:"
					+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(new Date(task.getCurRunDate()));
			this.writeLocalLog(Level.INFO, startLogString);

			if (conn_usp == null || conn_usp.isClosed()) {
				conn_usp = DriverManager.getConnection(MYSQL_CONN_STR_USP, MYSQL_USER_USP, MYSQL_PASSWORD_USP);
				this.writeLocalLog(Level.INFO, "conn_usp reconnected...");
			}

			if (conn_lhotse == null || conn_lhotse.isClosed()) {
				conn_lhotse = DriverManager.getConnection(MYSQL_CONN_STR_LHOTSE, MYSQL_USER_LHOTSE,
						MYSQL_PASSWORD_LHOTSE);
				this.writeLocalLog(Level.INFO, "conn_lhotse reconnected...");
			}

			Map<String, Object> r = query.query(conn_usp, "select task_time from lhotse_task_time where task_id=?",
					new MapHandler(), new Object[] { taskId });

			Object taskTimeObj = null;
			if (r != null && r.size() > 0)
				taskTimeObj = r.get("task_time");
			this.writeLocalLog(Level.INFO, "lhotse_task_time.task_time=" + taskTimeObj);

			String taskTimeStr = null;
			if (taskTimeObj == null) {
				Map<String, Object> r2 = query.query(conn_lhotse, "select old_id from lb_task where task_id=?",
						new MapHandler(), new Object[] { taskId });
				if (r2 != null && r2.size() > 0) {
					Object oldId = r2.get("old_id");
					Map<String, Object> r3 = query.query(conn_usp,
							"select task_time from lhotse_task_time where task_id=?", new MapHandler(),
							new Object[] { oldId });
					if (r3 != null && r3.size() > 0) {
						Object taskTimeObjNew = r3.get("task_time");
						taskTimeStr = taskTimeObjNew.toString();
					}
				}
			} else {
				taskTimeStr = taskTimeObj.toString();
			}

			if (taskTimeStr == null)
				taskTime = 1;
			else
				taskTime = Integer.parseInt(taskTimeStr);

			if (taskTime <= 0)
				taskTime = 1;

			commitTask(LState.RUNNING, null, "Task waiting time=" + taskTime);
			this.writeLocalLog(Level.INFO, "Task waiting time=" + taskTime);
			Thread.sleep(taskTime * 60 * 1000);
			this.writeLocalLog(Level.INFO, "=====================\n");
		} catch (Exception e) {
			String st = CommonUtils.stackTraceToString(e);
			this.writeLocalLog(Level.SEVERE, "Exception stackTrace: " + st);
			this.writeLocalLog(Level.INFO, "=====================\n");
			exceptionFound = true;
		} finally {
			if (exceptionFound)
				commitTask(LState.FAILED, null, "Failed with exception.");
			else
				commitTask(LState.SUCCESSFUL, null, "Completed.");
		}
	}

	@Override
	public void kill() throws IOException {
		// TODO Auto-generated method stub

	}

}
