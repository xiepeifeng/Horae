package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.util.CommonUtils;
import com.tencent.isd.lhotse.runner.util.RunnerUtils;
import com.tencent.isd.lhotse.runner.util.RunnerUtils.LoadDataResult;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

public class HdfsToTDWRunner extends TDWDDCTaskRunner {
	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(HdfsToTDWRunner.class, (byte) 85);
	}

	@Override
	public void execute() 
		throws IOException {

		LTask task = getTask();
		Map<String, String> keyValues = new HashMap<String, String>();
		/* Calculate the real data date, for tasks transferred from USP. */
		runDate = getRealDataDate(task);

		/* Check the server tags configuration for this task. */
		if (task.getSourceServersCount() < 2) {
			keyValues.put("exit_code", RunnerUtils.SERVER_CONFIG_ERROR_CODE);
			keyValues.put("task_desc", "Task server configuration is incorrect, expect at least 2"
					+ " source servers configured, but " + task.getSourceServersCount()
					+ " server configured");
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return;
		}

		if (task.getTargetServersCount() < 3) {
			keyValues.put("exit_code", RunnerUtils.SERVER_CONFIG_ERROR_CODE);
			keyValues.put("task_desc",
					"Task server configuration is incorrect, expect at least 3 target "
							+ "servers configured, but " + task.getTargetServersCount()
							+ " server configured");
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return;
		}

		/*
		 * Check whether we need to skip the weekly task for those USP weekly
		 * and monthly tasks.
		 */
		if (skipWeeklyTaskExecution(task, runDate)) {
			return;
		}

		/* Get the servers configurations. */
		LServer hdfsServer = task.getSourceServers(0);
		LServer hiveServer = task.getTargetServers(0);
		LServer hiveNameNodeServer = task.getTargetServers(1);

		/* Get the task configurations. */
		String loadMode = this.getExtPropValue("loadMode");
		String tableName = this.getExtPropValue("tableName");
		String delimiter = this.getExtPropValue("delimiter");
		String charSet = this.getExtPropValue("charSet");
		String databaseName = this.getExtPropValue("databaseName");
		String sourceColumnNames = this.getExtPropValue("sourceColumnNames");
		String targetColumnNames = this.getExtPropValue("targetColumnNames");
		String specialParam = this.getExtPropValue("special_para");

		String failedOnZeroWrited = this.getExtPropValue("failedOnZeroWrited");
		// Special parameter for user selfdefine;Need hive support!
		// String selfPlcParam = this.getExtPropValue("selfDefineParameter");

		String partitionName = null;
		String whereClause = null;
		String checkFileName = null;
		String sourceFilePath = null;
		String sourceFileNames = null;
		try {
			partitionName = RunnerUtils.formatStrDateParam(this.getExtPropValue("partitionType"),
					runDate);
			whereClause = RunnerUtils.formatStrDateParam(this.getExtPropValue("whereClause"),
					runDate);
			checkFileName = RunnerUtils.formatStrDateParam(this.getExtPropValue("checkFileName"),
					runDate);
			sourceFilePath = RunnerUtils.formatStrDateParam(this.getExtPropValue("sourceFilePath"),
					runDate);
			sourceFileNames = RunnerUtils.formatStrDateParam(
					this.getExtPropValue("sourceFileNames"), runDate);
			if (sourceFileNames == null) {
				sourceFileNames = "*";
			}
		}
		catch (Exception e) {
			keyValues.put("exit_code", RunnerUtils.DATE_FORMAT_ERROR_CODE);
			keyValues.put("task_desc", "Task failed becuase date format is not correct: "
					+ CommonUtils.stackTraceToString(e));
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return;
		}

		/* Get the global parameter. */
		final String plcProgram = this.requestGlobalParameters().get(PLC_PROGRAM);
		final String sourceHadoopCommand = getHadoopCommandOnVersion(hdfsServer);
		final String targetHadoopCommand = getHadoopCommandOnVersion(hiveNameNodeServer);
		final String tmpPathPrefix = this.requestGlobalParameters().get(NAME_NODE_TMP_PATH);
		final String externalTableDb = this.requestGlobalParameters().get(EXTERNAL_TABLE_CREATE_DB);		
		plcParameter = getPLCParameter(task.getId(), runDate);
		String externalTableName = "ext_" + task.getId() + "_" + HOUR_FORMAT.format(runDate) + "_" + System.currentTimeMillis();
		String externalTableDataPath = sourceFilePath;

		boolean success = false;
		boolean committed = false;
		Connection conn = null;
		try {
			/* Get database connection and do the check. */
			conn = getBIConnection();
			
			if (conn == null) {
				this.writeLocalLog(Level.INFO, "Can't get connection from BI database " + 
			                                   "to check db, table, partition existence.");						
				keyValues.put("exit_code", "1");
				keyValues.put("task_desc", "Task failed because can't get BI database connection");
				keyValues.put("run_date", HOUR_FORMAT.format(runDate));
				return;
			}
			
			/* Check to see whether the database exists or not. */
			if (!checkDbWithPG(conn, databaseName)) {
				committed = true;
				return;
			}

			/* Check to see whether the table exists or not. */
			if (!checkTableWithPG(conn, databaseName, tableName)) {
				committed = true;
				return;
			}

			/* Create the partition if the partition doesn't exist. */
			if (!createPartitionWithPG(plcProgram, conn, hiveServer, 
					                   databaseName, tableName, partitionName)) {
				committed = true;
				return;
			}

			/* Return if there is no source files at all. */
			if (checkHdfsEmptySource(sourceHadoopCommand, hdfsServer, sourceFilePath,
					sourceFileNames, failedOnZeroWrited)) {
				committed = true;
				return;
			}

			/* Before loading data, clean the old data first if it exists. */
			if (!clearOldData(loadMode, plcProgram, hiveServer, databaseName, tableName,
					partitionName, task)) {
				committed = true;
				return;
			}

			/*
			 * If source HDFS is actually the TDW cluster, create external table
			 * on the source file path, otherwise copy data from source HDFS to
			 * a temporary path on TDW name node.
			 */
			if (!hiveNameNodeServer.getHost().equals(hdfsServer.getHost())) {

				/* Create temporary path on TDW name node. */
				externalTableDataPath = tmpPathPrefix + File.separator + task.getId()
						+ File.separator + HOUR_FORMAT.format(runDate) + File.separator
						+ System.currentTimeMillis();

				/*
				 * First check whether the temporary path for saving data
				 * exists.
				 */
				if (!createTmpPath(targetHadoopCommand, hiveNameNodeServer, externalTableDataPath)) {
					committed = true;
					return;
				}

				/* Copy data to the target TDW. */
				if (!mrCopyData(task, hiveServer, hdfsServer, hiveNameNodeServer, sourceFilePath,
						externalTableDataPath, true)) {
					committed = true;
					return;
				}
			}

			/*
			 * Delete the check file if it exists in the external table data
			 * path.
			 */
			if (checkFileName != null) {
				RunnerUtils.deleteHdfsFile(targetHadoopCommand, hiveNameNodeServer,
						externalTableDataPath, checkFileName, this);
			}

			/* Create the external table. */
			if (!createExternalTable(plcProgram, hiveServer, hiveNameNodeServer,
					externalTableDataPath, externalTableDb, externalTableName, sourceColumnNames,
					delimiter, charSet)) {
				committed = true;
				return;
			}

			/* Load data from external table into TDW. */
			LoadDataResult result = loadData(plcProgram, hiveServer, databaseName,
					externalTableName, tableName, targetColumnNames, runDate, whereClause,
					externalTableDb, specialParam);
			if (result.getExitVal() != 0) {
				committed = true;
				return;
			}

			/*
			 * the number of records is 0. if failedOnZeroWrited is 1 then
			 * failed @2013-5-28(jessicajin)
			 */
			if ("1".equals(failedOnZeroWrited) && result.getSuccessWrite() == 0) {
				this.writeLocalLog(Level.INFO, "failedOnZeroWrited is " + failedOnZeroWrited
						+ " and success_writed is " + result.getSuccessWrite());
				keyValues.put("exit_code", "1");
				keyValues.put("task_desc", "have setted that task will be failed if "
						+ "success_writed is 0");
				keyValues.put("run_date", HOUR_FORMAT.format(runDate));
				return;
			}

			/* Commit the successful message. */
			keyValues.put("exit_code", "0");
			keyValues.put("success_writed", String.valueOf(result.getSuccessWrite()));
			keyValues.put("failed_writed", String.valueOf(result.getFailWrite()));
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			success = true;		
		} catch (SQLException e) {
			keyValues.put("exit_code", RunnerUtils.BI_DB_ERROR_CODE);
			keyValues.put("task_desc",
					      "Connect to BI database failed: " + CommonUtils.stackTraceToString(e));
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
		} catch (Exception e) {
			keyValues.put("exit_code", RunnerUtils.UNKOWN_ERROR_CODE);
			keyValues.put("task_desc",
					      "Load data from Hdfs to TDW failed: " + CommonUtils.stackTraceToString(e));
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			// throw new IOException(e);
		} finally {
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (Exception e) {
				this.writeLocalLog(Level.SEVERE, CommonUtils.stackTraceToString(e));
			}
			
			try {
				doCleanWork(plcProgram, hiveServer, externalTableDb, externalTableName,
						targetHadoopCommand, hiveNameNodeServer, externalTableDataPath);
			} catch (Exception e) {
				this.writeLocalLog(Level.SEVERE, CommonUtils.stackTraceToString(e));
			}

			try {
				if (!committed) {
					commitJsonResult(keyValues, success, plcParameter);
				}
			} catch (Exception e) {
				this.writeLocalLog(Level.SEVERE, CommonUtils.stackTraceToString(e));
				throw new IOException(e);
			}
		}
	}

	@Override
	public void kill() throws IOException {
		// TODO Auto-generated method stub
	}
}
