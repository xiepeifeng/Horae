package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.AbstractTaskRunner;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.util.CommonUtils;
import com.tencent.isd.lhotse.runner.util.RunnerUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.logging.Level;

public class PGProcedureRunner extends AbstractTaskRunner {

	private Connection conn = null;
	protected static final SimpleDateFormat RUN_DATE_FORMAT = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");
	protected static final String HOUR = "H";
	protected static final String DAY = "D";
	protected static final String MONTH = "M";
	protected static final String WEEK = "W";

	public void init(LTask task, String dbName) throws ClassNotFoundException,
			InstantiationException, IllegalAccessException, SQLException {
		LServer server = task.getSourceServers(0);
		String taskId = task.getId();
		Date curRunDate = new Date(task.getCurRunDate());
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		String applicationNumber = taskId + "_" + sdf.format(curRunDate);
		String pgHost = server.getHost();
		int pgPort = server.getPort();
		String pgUser = server.getUserName();
		String pgPsw = server.getPassword();
		String dbURL = String
				.format("jdbc:postgresql://%s:%d/%s?useUnicode=true&characterEncoding=UTF-8&ApplicationName=%s",
						pgHost, pgPort, dbName, applicationNumber);
		Class.forName("org.postgresql.Driver");
		conn = DriverManager.getConnection(dbURL, pgUser, pgPsw);
	}

	protected Date getRealDataDate(LTask task) throws IOException {

		Date runDate = new Date(task.getCurRunDate());

		/* Start running, return a status to base. */
		String logString = "Start running Runner:" + task.getId() + ", run date:"
				+ RUN_DATE_FORMAT.format(runDate);
		writeLocalLog(Level.INFO, logString);

		/* Calculate the real data date. */
		String offRangeExpr = this.getExtPropValue("offRange");
		String offRangeType = this.getExtPropValue("offRangeType");
		String periodDayExpr = null;
		if (WEEK.equals(task.getCycleUnit())) {
			periodDayExpr = this.getExtPropValue("dayOfWeek");
		}
		else if (MONTH.equals(task.getCycleUnit())) {
			periodDayExpr = this.getExtPropValue("dayOfMonth");
		}

		if ((offRangeExpr == null) || (offRangeType == null)) {
			logString = "The data date for Runner: " + task.getId() + " is: "
					+ RUN_DATE_FORMAT.format(runDate);
			writeLocalLog(Level.INFO, logString);
			return runDate;
		}

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(runDate);
		int offRange = new Integer(offRangeExpr);
		int periodDay = 0;
		if (periodDayExpr != null) {
			periodDay = new Integer(periodDayExpr);
		}

		if (HOUR.equals(task.getCycleUnit())) {
			if ((offRange == 0) && !RunnerUtils.DAY_EXPR.equals(offRangeType)) {
				calendar.add(Calendar.HOUR_OF_DAY, 1);
			}
		}
		else if (DAY.equals(task.getCycleUnit())) {
			if (offRange == 0) {
				calendar.add(Calendar.DAY_OF_YEAR, 1);
			}
		}
		else if (WEEK.equals(task.getCycleUnit())) {
			calendar.add(Calendar.WEEK_OF_YEAR, 1);
			calendar.add(Calendar.DAY_OF_YEAR, periodDay - 1);
			if (offRange != 0) {
				calendar.add(Calendar.DAY_OF_YEAR, -offRange);
			}
		}
		else if (MONTH.equals(task.getCycleUnit())) {
			if (offRange == 0) {
				calendar.add(Calendar.MONTH, 1);
				calendar.add(Calendar.DAY_OF_YEAR, periodDay - 1);
			}
			else {
				if (RunnerUtils.MONTH_EXPR.equals(offRangeType)) {
					calendar.add(Calendar.DAY_OF_YEAR, periodDay - 1);
				}
				else if (RunnerUtils.DAY_EXPR.equals(offRangeType)) {
					calendar.add(Calendar.MONTH, 1);
					calendar.add(Calendar.DAY_OF_YEAR, periodDay - 1);
					calendar.add(Calendar.DAY_OF_YEAR, -offRange);
				}
			}
		}

		logString = "The data date for Runner: " + task.getId() + " is: "
				+ RUN_DATE_FORMAT.format(calendar.getTime());
		writeLocalLog(Level.INFO, logString);

		return calendar.getTime();
	}

	@Override
	public void execute() throws IOException {
		LTask task = this.getTask();
		String dbName = this.getExtPropValue("dbName");
		String runtimeId = this.getTask().getId() + this.getTask().getCurRunDate();
		try {
			this.init(task, dbName);
		}
		catch (Exception e) {
			JSONObject desc = new JSONObject();
			putJson(desc, "desc", "init db info error");
			this.writeLocalLog(Level.INFO,"exception:" + e.getMessage());
			this.commitTask(LState.FAILED, runtimeId, desc.toString());
			return;
		}
		PreparedStatement ps = null;
		try {
			Date runDate = getRealDataDate(task);
			String procedureType = this.getExtPropValue("procType");
			if (procedureType.equals("0")) {
				// sql
				String procSql = RunnerUtils.formatStrDateParam(this.getExtPropValue("procParams"),
						runDate);
				ps = conn.prepareStatement(procSql);
				this.writeLocalLog(Level.INFO, "Type:" + procedureType + "-" + procSql);
			}
			else {
				String procedureName = this.getExtPropValue("procName");
				String procedureParams = this.getExtPropValue("procParams");
				String[] paramTuple = procedureParams.split(",");
				String params = "";
				String[] realParamTuple = new String[paramTuple.length];
				for (int ind = 0; ind < paramTuple.length; ind++) {
					realParamTuple[ind] = RunnerUtils.formatStrDateParam(paramTuple[ind], runDate);
					params = params + "?,";
				}
				if (params.endsWith(","))
					params = params.substring(0, params.length() - 1);
				String procCmd = String.format("{call %s(%s)}", procedureName, params);
				this.writeLocalLog(Level.INFO, "Type:" + procedureType + "-" + procCmd);
				ps = conn.prepareCall(procCmd);
				for (int ind = 0; ind < realParamTuple.length; ind++) {
					this.writeLocalLog(Level.INFO, "parameters:" + realParamTuple[ind]);
					ps.setString(ind + 1, realParamTuple[ind]);
				}
			}
			if (ps != null) {
				JSONObject desc = new JSONObject();
				putJson(desc, "desc", "executing procedure or sql....");
				this.commitTaskAndLog(LState.RUNNING, runtimeId, desc.toString());
				this.writeLocalLog(Level.INFO, desc.getString("desc"));
				ps.execute();
			}
			else {
				if (conn != null){
					SQLWarning warn = conn.getWarnings();
					if(warn != null)
					    this.writeLocalLog(Level.INFO, warn.getLocalizedMessage());
					conn.close();
				}
				JSONObject desc = new JSONObject();
				putJson(desc, "desc", "getting pg db info error");
				this.writeLocalLog(Level.INFO, desc.getString("desc"));
				this.commitTask(LState.FAILED, runtimeId, desc.toString());
				return;
			}
			JSONObject desc = new JSONObject();
			putJson(desc, "desc", "execute successfully");
			this.writeLocalLog(Level.INFO, desc.getString("desc"));
			this.commitTask(LState.SUCCESSFUL, runtimeId, desc.toString());
		}
		catch (Exception e) {
			JSONObject desc = new JSONObject();
			putJson(desc, "desc", e.getMessage());
			this.writeLocalLog(Level.INFO, e.getMessage());
			this.commitTask(LState.FAILED, runtimeId, desc.toString());
		}
		finally {
			if (conn != null) {
				try {
					SQLWarning warn = conn.getWarnings();
					if(warn != null)
					    this.writeLocalLog(Level.INFO, warn.getLocalizedMessage());
					conn.close();
				}
				catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

	}

	private void putJson(JSONObject json, String key, String value) {
		try {
			json.put(key, value);
		}
		catch (JSONException e) {
			this.writeLocalLog(Level.SEVERE, e.getMessage());
		}
	}

	private void commitTaskAndLog(LState state, String runtimeId, String desc) {
		try {
			commitTask(state, runtimeId, desc);
		}
		catch (Exception e) {
			String st = CommonUtils.stackTraceToString(e);
			this.writeLocalLog(Level.INFO, "Log_desc :" + desc);
			this.writeLocalLog(Level.INFO, "Log_desc length:" + desc.length());
			this.writeLocalLog(Level.SEVERE, "Commit task failed, StackTrace: " + st);
		}
	}

	@Override
	public void kill() throws IOException {
		// TODO Auto-generated method stub

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("fuckall");
		TaskRunnerLoader.startRunner(PGProcedureRunner.class, (byte) 111);
	}

}
