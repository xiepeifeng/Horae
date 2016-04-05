package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.util.CommonUtils;
import com.tencent.isd.lhotse.runner.util.RunnerUtils;
import com.tencent.isd.lhotse.runner.util.RunnerUtils.HadoopResult;
import com.tencent.isd.lhotse.runner.util.RunnerUtils.LoadDataResult;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

public class TDWToHdfsRunner extends TDWDDCTaskRunner {
	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(TDWToHdfsRunner.class, (byte) 86);
	}

	@Override
	public void execute() 
		throws IOException {

		LTask task = getTask();
		Map<String, String> keyValues = new HashMap<String, String>();

		/* Check the server tags configuration for this task. */
		if (task.getSourceServersCount() < 3) {			
        	keyValues.put("exit_code", RunnerUtils.SERVER_CONFIG_ERROR_CODE);
        	keyValues.put("task_desc", "Task server configuration is incorrect, expect at least 3 source " +
    				                   "HDFS servers configured, but " + task.getSourceServersCount() +
    				                   " server configured");
        	commitJsonResult(keyValues, false, "");
			return;
		}
		
		if (task.getTargetServersCount() < 2) {
        	keyValues.put("exit_code", RunnerUtils.SERVER_CONFIG_ERROR_CODE);
        	keyValues.put("task_desc", "Task server configuration is incorrect, expect at least 2" +
    				                   " target servers configured, but " + task.getTargetServersCount() +
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

		/* Get the hive server configuration. */
		LServer hiveServer = task.getSourceServers(0);

		/* Get the name node configuration for this TDW. */
		LServer nameNodeServer = task.getSourceServers(1);

		/* Get the target HDFS cluster configuration. */
		LServer hdfsServer = task.getTargetServers(0);

		/* Get the task configuration. */
		String databaseName = this.getExtPropValue("databaseName");
		String destFileDelimiter = this.getExtPropValue("destFileDelimiter");
		String specialParam = this.getExtPropValue("special_para");
		
		String filterSQL = null;
		String destFilePath = null;
		String destCheckFilePath = null;
		String destCheckFileName = null;
		try {
		    filterSQL = 
		    	RunnerUtils.formatStrDateParam(this.getExtPropValue("filterSQL"), runDate);
		    destFilePath = 
		    	RunnerUtils.formatStrDateParam(this.getExtPropValue("destFilePath"), runDate);
		    destCheckFilePath = 
		    	RunnerUtils.formatStrDateParam(this.getExtPropValue("destCheckFilePath"), runDate);
		    destCheckFileName = 
		    	RunnerUtils.formatStrDateParam(this.getExtPropValue("destCheckFileName"), runDate);
		} catch (Exception e) {
			keyValues.put("exit_code", RunnerUtils.DATE_FORMAT_ERROR_CODE);
        	keyValues.put("task_desc", "Task failed becuase date format is not correct: " + 
        	                           CommonUtils.stackTraceToString(e));
        	keyValues.put("run_date", HOUR_FORMAT.format(runDate));
        	commitJsonResult(keyValues, false, "");
        	return;
		}

		/* Get the global parameter. */
		final String plcProgram = this.requestGlobalParameters().get(PLC_PROGRAM);
		final String sourceHadoopCommand = getHadoopCommandOnVersion(nameNodeServer);
		final String targetHadoopCommand = getHadoopCommandOnVersion(hdfsServer);
		
		this.writeLocalLog(Level.INFO, "source hadoop command: " + sourceHadoopCommand +
				                       "target hadoop command: " + targetHadoopCommand);
		final String tmpPathPrefix = this.requestGlobalParameters().get(NAME_NODE_TMP_PATH);
		final String externalTableDb = this.requestGlobalParameters().get(EXTERNAL_TABLE_CREATE_DB);
		plcParameter = getPLCParameter(task.getId(), runDate);

		String externalTableName = "ext_" + task.getId() + "_" + HOUR_FORMAT.format(runDate) + "_" + System.currentTimeMillis();
		String externalTableDataPath = destFilePath;
		
		boolean success = false;
		boolean committed = false;
		try {
			/* Check to see whether the database exists or not. */
			if (!checkDbExistence(plcProgram, hiveServer, databaseName)) {
				committed = true;
				return;
			}

			/* Clean the old target file and check file directory. */
			HadoopResult result = 
				RunnerUtils.removeOldPathAndCreateNewPath(targetHadoopCommand, hdfsServer, destFilePath, this);
			if (result.getExitVal() != 0) {
	        	keyValues.put("exit_code", result.getHadoopReturnCode());
	        	keyValues.put("task_desc", 
	        			      "Create the new directory on target HDFS cluster for saving results failed");
	        	keyValues.put("run_date", HOUR_FORMAT.format(runDate));
	        	commitJsonResult(keyValues, false, "");
	        	committed = true;
				return;
			}
			
			result = 
				RunnerUtils.removeOldPathAndCreateNewPath(targetHadoopCommand, hdfsServer, destCheckFilePath, this);
			if (result.getExitVal() != 0) {
	        	keyValues.put("exit_code", result.getHadoopReturnCode());
	        	keyValues.put("task_desc", 
	        			      "Create new directory on target HDFS cluster for saving results failed");
	        	keyValues.put("run_date", HOUR_FORMAT.format(runDate));
	        	commitJsonResult(keyValues, false, "");
	        	committed = true;
				return;
			}			
			writeLocalLog(Level.INFO, "Clean the target files and check file successfully");

			/* Need to create temporary directory for external table data. */
			if (!nameNodeServer.getHost().equalsIgnoreCase(hdfsServer.getHost())) {
				externalTableDataPath = 
					tmpPathPrefix + File.separator + task.getId() + 
					File.separator + HOUR_FORMAT.format(runDate) +
					File.separator + System.currentTimeMillis();

				/* First check whether the temporary path for saving data exists. */
				if (!createTmpPath(sourceHadoopCommand, nameNodeServer, externalTableDataPath)) {
					committed = true;
					return;
				}
			}

			/* Create external table using the filter SQL. */
			if (!createExternalTableWithSQL(plcProgram, hiveServer, nameNodeServer, 
					                        externalTableDataPath, databaseName, externalTableDb, 
					                        externalTableName, filterSQL, destFileDelimiter)) {
				committed = true;
				return;
			}
			
			/* Load data to the external table. */
			LoadDataResult loadDataResult = 
				loadDataWithSQL(plcProgram, hiveServer, databaseName, 
						        externalTableDb, externalTableName, filterSQL, specialParam);
			if (loadDataResult.getExitVal() != 0) {	
				committed = true;
				return;
			}

			/* Copy the external table data to the target HDFS cluster. */
			if (!nameNodeServer.getHost().equalsIgnoreCase(hdfsServer.getHost())) {
				if (!mrCopyData(task, hiveServer, nameNodeServer, hdfsServer, 
						        externalTableDataPath, destFilePath, false)) {
					committed = true;
					return;
				}
				
				/* Remove the dist_cp log files on the target HDFS cluster. */
				RunnerUtils.removeHadoopCopyLogFiles(targetHadoopCommand, hdfsServer, destFilePath, this);
			}

			/* Delete the old check file if it exists. */
			HadoopResult deleteResult = 
				RunnerUtils.deleteHdfsFile(targetHadoopCommand, hdfsServer, 
				   	                       destCheckFilePath, destCheckFileName, this);
			if (deleteResult.getExitVal() != 0) {
				keyValues.put("exit_code", deleteResult.getHadoopReturnCode());
	        	keyValues.put("task_desc", "Task failed because the deletion of old check file failed"); 
	        	keyValues.put("run_date", HOUR_FORMAT.format(runDate));
	        	commitJsonResult(keyValues, false, "");
	        	committed = true;
				return;
			}

			/* Write the check file to the target HDFS cluster. */
			HadoopResult createCheckFileResult = 
				RunnerUtils.createHdfsCheckFile(task.getId(), HOUR_FORMAT.format(runDate), 
						                        targetHadoopCommand, hdfsServer, destFilePath, 
						                        this.requestGlobalParameters().get(CHECK_FILE_TMP_DIR),
						                        destCheckFilePath, destCheckFileName, this);
			if (createCheckFileResult.getExitVal() != 0) {
	        	keyValues.put("exit_code", createCheckFileResult.getHadoopReturnCode());
	        	keyValues.put("task_desc", "Task failed because the check file creation failed"); 
	        	keyValues.put("run_date", HOUR_FORMAT.format(runDate));
	        	commitJsonResult(keyValues, false, "");
	        	committed = true;
				return;
			}
			writeLocalLog(Level.INFO, "Write check file successfully");
			
			keyValues.put("exit_code", String.valueOf(RunnerUtils.SUCCESS_CODE));
			keyValues.put("success_writed", String.valueOf(loadDataResult.getSuccessWrite()));
			keyValues.put("failed_writed", String.valueOf(loadDataResult.getFailWrite()));
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			success = true;
		} catch (Exception e) {
        	keyValues.put("exit_code", RunnerUtils.UNKOWN_ERROR_CODE);
        	keyValues.put("task_desc", "TDW exported: " + CommonUtils.stackTraceToString(e));
        	keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			throw new IOException(e);
		} finally {
			try {
				doCleanWork(plcProgram, hiveServer, externalTableDb, externalTableName, 
		    		        sourceHadoopCommand, nameNodeServer, externalTableDataPath);
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
	}
}