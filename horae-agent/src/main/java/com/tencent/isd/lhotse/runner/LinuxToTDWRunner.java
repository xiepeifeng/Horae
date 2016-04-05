package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.util.CommonUtils;
import com.tencent.isd.lhotse.runner.util.RegFileNameFilter;
import com.tencent.isd.lhotse.runner.util.RunnerUtils;
import com.tencent.isd.lhotse.runner.util.RunnerUtils.HadoopResult;
import com.tencent.isd.lhotse.runner.util.RunnerUtils.LoadDataResult;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

public class LinuxToTDWRunner extends TDWDDCTaskRunner {	
	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(LinuxToTDWRunner.class, (byte) 82);
	}

	@Override
	public void execute() 
		throws IOException {

		LTask task = getTask();
		Map<String, String> keyValues = new HashMap<String, String>();		

		if (task.getTargetServersCount() != 3) {			
        	keyValues.put("exit_code", RunnerUtils.SERVER_CONFIG_ERROR_CODE);
        	keyValues.put("task_desc", "Task server configuration is incorrect, expect 3 target " +
    				                   "servers configured, but " + task.getTargetServersCount() +
    				                   " server configured");
        	commitJsonResult(keyValues, false, "");
			return;
		}
		
		/* Calculate the real data date, for tasks transferred from USP. */
		runDate = getRealDataDate(task);

		/* 
		 * Check whether we need to skip the weekly task for those USP 
		 * weekly and monthly tasks. 
		 */
		if (skipWeeklyTaskExecution(task, runDate)) {
			return;
		}
		
		/* Configure the hive server parameters. */
		LServer hiveServer = task.getTargetServers(0);

		/* Configure the name node server parameters for TDW. */
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
		
		String sourceFilePath = null;
		String sourceFileNames = null;
		String partitionName = null;
		String whereClause = null;
		try {
		    sourceFilePath = 
		    	RunnerUtils.replaceDateExpr(this.getExtPropValue("sourceFilePath"), runDate);
		    sourceFileNames = 
		    	RunnerUtils.replaceDateExpr(this.getExtPropValue("sourceFileNames"), runDate);
			partitionName = 
				RunnerUtils.replaceDateExpr(this.getExtPropValue("partitionType"), runDate);
		    whereClause = 
		    	RunnerUtils.replaceDateExpr(this.getExtPropValue("whereClause"), runDate);
		} catch (Exception e) {
			keyValues.put("exit_code", RunnerUtils.DATE_FORMAT_ERROR_CODE);
        	keyValues.put("task_desc", "Task failed becuase date format is not correct: " + 
        	                           CommonUtils.stackTraceToString(e));
        	keyValues.put("run_date", HOUR_FORMAT.format(runDate));
        	commitJsonResult(keyValues, false, "");
        	return;
		}

		/* Global parameter. */
		final String plcProgram = this.requestGlobalParameters().get(PLC_PROGRAM);
		final String hadoopCommand = getHadoopCommandOnVersion(hiveNameNodeServer);
		final String tmpPathPrefix = this.requestGlobalParameters().get(NAME_NODE_TMP_PATH);	
		final String externalTableDb = this.requestGlobalParameters().get(EXTERNAL_TABLE_CREATE_DB);
		plcParameter = getPLCParameter(task.getId(), runDate);
		
		String externalTableName = "ext_" + task.getId() + "_" + HOUR_FORMAT.format(runDate) + "_" + System.currentTimeMillis();
		String externalTableDataPath = tmpPathPrefix + File.separator + task.getId() + 
				                       File.separator + HOUR_FORMAT.format(runDate) + 
				                       File.separator + System.currentTimeMillis();	
		
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
			File sourcePath = new File(sourceFilePath);
			File[] sourceFiles = 
				sourcePath.listFiles(new RegFileNameFilter(sourceFileNames));
			if (!sourcePath.exists() || (sourceFiles.length == 0)) {
	        	keyValues.put("exit_code", "0");
	        	keyValues.put("task_desc", "Task succeeded because no files in the source path");
	        	keyValues.put("run_date", HOUR_FORMAT.format(runDate));
	        	commitJsonResult(keyValues, false, "");
	        	committed = true;
				return;
			}

			/* Before loading data, clean the old data first if it exists. */
			if (!clearOldData(loadMode, plcProgram, hiveServer, databaseName, 
			                  tableName, partitionName, task)) {
				committed = true;
		        return;
	        }
			
			/* Delete the target directory before copy. */
			if (!createTmpPath(hadoopCommand, hiveNameNodeServer, externalTableDataPath)) {
				committed = true;
				return;
			}

			/* Copy all the files in the local directory to remote HDFS. */
			HadoopResult copyResult = 
				RunnerUtils.writeLinuxFileToHdfs(hadoopCommand, hiveNameNodeServer, externalTableDataPath, 
                                                 sourceFilePath, sourceFileNames, this);
			if (copyResult.getExitVal() != 0) {
	        	keyValues.put("exit_code", copyResult.getHadoopReturnCode());
	        	keyValues.put("task_desc", "Copy the source files on Linux to Hdfs failed with " +
					                       "error code: " + copyResult.getHadoopReturnCode());
	        	keyValues.put("run_date", HOUR_FORMAT.format(runDate));
	        	commitJsonResult(keyValues, false, "");	
	        	committed = true;
				return; 
			}

			/* Create the external table. */
			if (!createExternalTable(plcProgram, hiveServer, hiveNameNodeServer, 
					                 externalTableDataPath, externalTableDb,
					                 externalTableName, sourceColumnNames, delimiter, charSet)) {
				committed = true;
				return;
			}
			
			/* Load data from external table into TDW. */
			LoadDataResult result = 
				loadData(plcProgram, hiveServer, databaseName, 
						 externalTableName, tableName, targetColumnNames,	
						 runDate, whereClause, externalTableDb, specialParam);
			if (result.getExitVal() != 0) {	
				committed = true;
				return;
			}

			/* Log the successful message. */
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
        	keyValues.put("task_desc", "Load data from Linux to TDW failed: " + 
        	                           CommonUtils.stackTraceToString(e));
        	keyValues.put("run_date", HOUR_FORMAT.format(runDate));
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
			   		        hadoopCommand, hiveNameNodeServer, externalTableDataPath);
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
