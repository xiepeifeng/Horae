package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.util.CommonUtils;
import com.tencent.isd.lhotse.runner.util.RunnerUtils;
import com.tencent.isd.lhotse.runner.util.RunnerUtils.LoadDataResult;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

public class TDWToLinuxRunner extends TDWDDCTaskRunner {
	private static final String PLC_PROGRAM = "plcProgram";
	private final String NAME_NODE_TMP_PATH = "nameNodeTmpPath";

	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(TDWToLinuxRunner.class, (byte) 83);
	}

	@Override
	public void execute() 
		throws IOException {
		
		LTask task = getTask();
		Map<String, String> keyValues = new HashMap<String, String>();

		/* Make sure that the task server configuration is correct. */
		if (task.getSourceServersCount() < 3) {			
        	keyValues.put("exit_code", RunnerUtils.SERVER_CONFIG_ERROR_CODE);
        	keyValues.put("task_desc", "Task server configuration is incorrect, expect at least 3 source " +
    				                   "HDFS servers configured, but " + task.getSourceServersCount() +
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
		
		/* Get TDW server configuration. */
		LServer hiveServer = task.getSourceServers(0);

		/* Get TDW name node configuration. */
		LServer nameNodeServer = task.getSourceServers(1);

		/* Get out the task configuration. */
		String databaseName = this.getExtPropValue("databaseName");
		String destFileDelimiter = this.getExtPropValue("destFileDelimiter");
		String specialParam = this.getExtPropValue("special_para");
		
		String filterSQL = null;
		String destFilePath = null;
		String destCheckFilePath = null;
		String destCheckFileName = null;
		try {
		    filterSQL = 
		    	RunnerUtils.replaceDateExpr(this.getExtPropValue("filterSQL"), runDate);
		    destFilePath = 
		    	RunnerUtils.replaceDateExpr(this.getExtPropValue("destFilePath"), runDate);
			destCheckFilePath = 
				RunnerUtils.replaceDateExpr(this.getExtPropValue("destCheckFilePath"), runDate);
		    destCheckFileName = 
		    	RunnerUtils.replaceDateExpr(this.getExtPropValue("destCheckFileName"), runDate);
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
		final String hadoopCommand = getHadoopCommandOnVersion(nameNodeServer);
		final String tmpPathPrefix = this.requestGlobalParameters().get(NAME_NODE_TMP_PATH);
		final String externalTableDb = this.requestGlobalParameters().get(EXTERNAL_TABLE_CREATE_DB);
		plcParameter = getPLCParameter(task.getId(), runDate);

		String externalTableName = "ext_" + task.getId() + "_" + HOUR_FORMAT.format(runDate) + "_" + System.currentTimeMillis();
		String externalTableDataPath = tmpPathPrefix + File.separator + task.getId() + 
				                       File.separator + HOUR_FORMAT.format(runDate) + 
				                       File.separator + System.currentTimeMillis();

		boolean success = false;
		boolean committed = false;
		try {
			/* Check to see whether the database exists or not. */
			if (!checkDbExistence(plcProgram, hiveServer, databaseName)) {
				committed = true;
				return;
			}

			/* Clean the destination path and create it. */
			File destFileDir = new File(destFilePath);
			RunnerUtils.deleteFile(destFileDir);
			destFileDir.mkdirs();

			/* Create the temporary path for creating external table. */
//			HadoopResult createResult = 
//				RunnerUtils.createHDFSPath(hadoopCommand, nameNodeServer, 
//                                           externalTableDataPath, this);
//			if (createResult.getExitVal() != 0) {
//	        	keyValues.put("exit_code", createResult.getHadoopReturnCode());
//	        	keyValues.put("task_desc", "Task failed because creating temporary path for " +
//					                       "external table fails with error code: " + 
//	        			                   createResult.getHadoopReturnCode());
//	        	keyValues.put("run_date", HOUR_FORMAT.format(runDate));
//	        	commitJsonResult(keyValues, false, "");
//	        	committed = true;
//				return;
//			}
//			this.writeLocalLog(Level.INFO, "Temporary path: " + externalTableDataPath + " created");
			
			/* Create the temporary path for creating external table. */
			if( !createTmpPath(hadoopCommand, nameNodeServer,
					externalTableDataPath) ){
				committed = true;
				return;
			}

			/* Create the external table. */
			if (!createExternalTableWithSQL(plcProgram, hiveServer, nameNodeServer, 
     	                                    externalTableDataPath, databaseName, externalTableDb, 
     	                                    externalTableName, filterSQL, destFileDelimiter)) {
				committed = true;
				return;
			}
			
			/* Execute the filtering SQL to write the data into external table. */
			LoadDataResult loadDataResult = 
				loadDataWithSQL(plcProgram, hiveServer, databaseName, 
						        externalTableDb, externalTableName, filterSQL, specialParam);
			if (loadDataResult.getExitVal() != 0) {
				committed = true;
				return;
			}

			/* Copy output files from HDFS to target Linux system and write the check file. */
			if (!writeLogAndCheckFilesToLinux(hadoopCommand, nameNodeServer, destFilePath, 
				                              externalTableDataPath, "*",
				                              destCheckFilePath, destCheckFileName, this)) {
				committed = true;
				return;
			}

			/* Composite the JSON message. */			
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
		    		        hadoopCommand, nameNodeServer, externalTableDataPath);
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