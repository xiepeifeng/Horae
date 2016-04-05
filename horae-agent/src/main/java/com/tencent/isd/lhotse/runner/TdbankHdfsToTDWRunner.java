package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.util.CommonUtils;
import com.tencent.isd.lhotse.runner.util.RunnerUtils;
import com.tencent.isd.lhotse.runner.util.RunnerUtils.HadoopResult;
import com.tencent.isd.lhotse.runner.util.RunnerUtils.HdfsIsEmptyFileResult;
import com.tencent.isd.lhotse.runner.util.RunnerUtils.LoadDataResult;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TdbankHdfsToTDWRunner extends TDWDDCTaskRunner {
	
	private static final String SOURCE_PATH_DEEPTH_REGEX = "^(/([a-zA-Z0-9]+[-_.a-zA-Z0-9]*)){3,}+";
	private static Pattern sourcePathDeepRang = null;
	private static final String TDBANK_WORK_PATH = "tdbankWorkPath";
	private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyMMddHHmm");
	
	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(TdbankHdfsToTDWRunner.class, (byte) 107);
	}
	
	static {
		sourcePathDeepRang = Pattern.compile(SOURCE_PATH_DEEPTH_REGEX);
	}

	@Override
	public void execute() 
		throws IOException {
		
		LTask task = getTask();		
		Map<String, String> keyValues = new HashMap<String, String>();
		/* Calculate the real data date, for tasks transferred from USP. */
		runDate = getRealDataDate(task);
		
		/*lyndldeng test here*/
		this.writeLocalLog(Level.INFO, "TdbankHdfsToTDWRunner");
		this.writeLocalLog(Level.INFO, "******************************************************************************");
		
		/* Check the server tags configuration for this task. */
		if (task.getSourceServersCount() < 2) {
        	keyValues.put("exit_code", RunnerUtils.SERVER_CONFIG_ERROR_CODE);
        	keyValues.put("task_desc", "Task server configuration is incorrect, expect at least 2" +
    				                   " source servers configured, but " + task.getSourceServersCount() +
    				                   " server configured");
        	keyValues.put("run_date", HOUR_FORMAT.format(runDate));
        	commitJsonResult(keyValues, false, "");
			return;
		}
		
		if (task.getTargetServersCount() < 3) {
        	keyValues.put("exit_code", RunnerUtils.SERVER_CONFIG_ERROR_CODE);
        	keyValues.put("task_desc", "Task server configuration is incorrect, expect at least 3 target " +
    				                   "servers configured, but " + task.getTargetServersCount() +
    				                   " server configured");
        	keyValues.put("run_date", HOUR_FORMAT.format(runDate));
        	commitJsonResult(keyValues, false, "");
			return;
		}
		
		/* 
		 * Check whether we need to skip the weekly task for those USP 
		 * weekly and monthly tasks. 
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
		
		String partitionName = null;
		String whereClause = null;
		String checkFileName = null;
		String sourceFilePath = null;
		String sourceFileNames = null;
		String subInterfaceList = this.getExtPropValue("subInterfaceList");
		String[] subList = null;
		
		try {
		    partitionName = 
		    	RunnerUtils.formatStrDateParam(this.getExtPropValue("partitionType"), runDate);
		    whereClause = 
			    RunnerUtils.formatStrDateParam(this.getExtPropValue("whereClause"), runDate);
			checkFileName = 
				RunnerUtils.formatStrDateParam(this.getExtPropValue("checkFileName"), runDate);
			sourceFilePath = 
				RunnerUtils.formatStrDateParam(this.getExtPropValue("sourceFilePath"), runDate);
			sourceFileNames = 
				RunnerUtils.formatStrDateParam(this.getExtPropValue("sourceFileNames"), runDate);
			if (sourceFileNames == null) {
				sourceFileNames = "*";
			}
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
		final String sourceHadoopCommand = getHadoopCommandOnVersion(hdfsServer);
		final String targetHadoopCommand = getHadoopCommandOnVersion(hiveNameNodeServer);
		final String tmpPathPrefix = this.requestGlobalParameters().get(NAME_NODE_TMP_PATH);
		final String externalTableDb = this.requestGlobalParameters().get(EXTERNAL_TABLE_CREATE_DB);
		final String workPath = this.requestGlobalParameters().get(TDBANK_WORK_PATH);
		plcParameter = getPLCParameter(task.getId(), runDate);
		//String externalTableName = "ext_" + task.getId() + System.currentTimeMillis();
		String externalTableName = "ext_" + task.getId() + "_" + SECOND_FORMAT.format(runDate) + "_" + System.currentTimeMillis();
		String externalTableDataPath = sourceFilePath;		
		
		boolean success = false;
		boolean committed = false;
		Connection conn = null;
		try {			
			/*For merge multi check file*/
			if(subInterfaceList !=null && subInterfaceList.trim().length() > 0){
				subList = subInterfaceList.trim().split("\\,"); 
	            //hdfsServer,
				String hdfsPathPrefix = checkFileName.substring(0,checkFileName.indexOf("${#1}")).trim();
				if(!checkPathDeepth(hdfsPathPrefix, sourcePathDeepRang)){
					committed = true;
					return;
				}
				String hdfsPath = null;
				if(task.getCycleUnit().equalsIgnoreCase("h")){
					hdfsPath = hdfsPathPrefix + "check/"
							+ simpleDateFormat.format(runDate).substring(0,6) + "/"
							+ simpleDateFormat.format(runDate);
				} else {
					hdfsPath = hdfsPathPrefix + "check/" + simpleDateFormat.format(runDate).substring(0,6);
				}
				
				String localFilePath = workPath + File.separator + task.getId() + File.separator +
						simpleDateFormat.format(runDate) + File.separator;
				String newCheckFileName = "check_" + simpleDateFormat.format(runDate) + ".checks";
				
				this.writeLocalLog(Level.INFO, "begin merge mutil check file.");
				this.writeLocalLog(Level.INFO, "check file partten : " + checkFileName);
				this.writeLocalLog(Level.INFO, "sub path list : " + subInterfaceList);
				
				HadoopResult mergeCheckResult = RunnerUtils.mergeCheckFileList(sourceHadoopCommand,
						                                                       checkFileName,
						                                                       subList,
						                                                       runDate,
						                                                       hdfsServer,
						                                                       localFilePath,
						                                                       hdfsPath,
						                                                       newCheckFileName,
						                                                       this);
				
				if(mergeCheckResult.getExitVal() != 0){
					
					this.writeLocalLog(Level.INFO, "123");
					
					keyValues.put("exit_code", String.valueOf(mergeCheckResult.getExitVal()));
		        	keyValues.put("task_desc", mergeCheckResult.getHadoopReturnCode());
		        	keyValues.put("run_date", HOUR_FORMAT.format(runDate));					
					committed = true;
					return;
				}
				checkFileName = hdfsPath + "/" + newCheckFileName;
				
				this.writeLocalLog(Level.INFO, "after merge check file name : " + checkFileName);
			}
			
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
			/* For TDbank. Close here!*/
			/*if (checkHdfsEmptySource(sourceHadoopCommand, hdfsServer, 
					                 sourceFilePath, sourceFileNames)) {
				committed = true;
				return;
			}*/

			/*Add for TdBank*/
			HdfsIsEmptyFileResult isEmpty = 
					RunnerUtils.isEmptyFileResult( sourceHadoopCommand, hdfsServer, 
							                       checkFileName, this);
			
			if( isEmpty.isEmptyFiles() || isEmpty.getExitVal() != 1 ) {
				if( isEmpty.isEmptyFiles() ){
					committed = true;
		        	keyValues.put("exit_code", String.valueOf(RunnerUtils.SUCCESS_CODE));
		        	keyValues.put("task_desc", "Task succeeded because no files in the source path");
		        	keyValues.put("run_date", HOUR_FORMAT.format(runDate));
		        	commitJsonResult(keyValues, true, "Task succeeded because no files in the source path");
					return;
				} else {
					committed = true;
					keyValues.put("exit_code", isEmpty.getHadoopReturnCode());
		        	keyValues.put("task_desc", isEmpty.getErrorDesc());
		        	keyValues.put("run_date", HOUR_FORMAT.format(runDate));
		        	commitJsonResult(keyValues, false, isEmpty.getErrorDesc());
					return;
				}
			}
			
			
			/* Before loading data, clean the old data first if it exists. */
			if (!clearOldData(loadMode, plcProgram, hiveServer, databaseName, 
					          tableName, partitionName, task)) {
				committed = true;
				return;
			}

			/*
			 * If source HDFS is actually the TDW cluster, create external 
			 * table on the source file path, otherwise copy data from 
			 * source HDFS to a temporary path on TDW name node.
			 */
			if (!hiveNameNodeServer.getHost().equals(hdfsServer.getHost())) {

				/* Create temporary path on TDW name node. */
				//externalTableDataPath = 
				//	tmpPathPrefix + File.separator + task.getId() + 
				//	File.separator + System.currentTimeMillis();
				externalTableDataPath = 
						tmpPathPrefix + File.separator + task.getId() + 
						File.separator + SECOND_FORMAT.format(runDate) + File.separator + System.currentTimeMillis();

				/* First check whether the temporary path for saving data exists. */
				if (!createTmpPath(targetHadoopCommand, hiveNameNodeServer, externalTableDataPath)) {
					committed = true;
					return;
				}

				/* Copy data to the target TDW. */
				/*Need change here for tdbank distcp*/
				if (!mrCopyDataForFilelist(task, hiveServer, hdfsServer, hiveNameNodeServer, 
						checkFileName, externalTableDataPath, true)) {
					committed = true;
					return;
				}
			}
			
			/* Delete the check file if it exists in the external table data path. */
			if (checkFileName != null) {
				RunnerUtils.deleteHdfsFile
				    (targetHadoopCommand, hiveNameNodeServer, externalTableDataPath, checkFileName, this);
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
				loadDataNew(plcProgram, hiveServer, databaseName, 
						 externalTableName, tableName, targetColumnNames, 
						 runDate, whereClause, externalTableDb);
			if (result.getExitVal() != 0) {
				committed = true;
				return;
			}
			
			/* Commit the successful message. */
			keyValues.put("exit_code", "0");
			keyValues.put("success_writed", String.valueOf(result.getSuccessWrite()));
			keyValues.put("failed_writed", String.valueOf(result.getFailWrite()));
			keyValues.put("run_date", SECOND_FORMAT.format(runDate));			
			success = true;
		} catch (SQLException e) {
			keyValues.put("exit_code", RunnerUtils.BI_DB_ERROR_CODE);
			keyValues.put("task_desc",
					      "Connect to BI database failed: " + CommonUtils.stackTraceToString(e));
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
		} catch (Exception e) {
        	keyValues.put("exit_code", RunnerUtils.UNKOWN_ERROR_CODE);
        	keyValues.put("task_desc", "Load data from Hdfs to TDW failed: " + CommonUtils.stackTraceToString(e));
        	keyValues.put("run_date", SECOND_FORMAT.format(runDate));
		} finally {	
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (Exception e) {
				this.writeLocalLog(Level.SEVERE, CommonUtils.stackTraceToString(e));
			}
			
			try {
			    doCleanWork(plcProgram, hiveServer, externalTableDb, 
				    	    externalTableName, targetHadoopCommand, 
				    	    hiveNameNodeServer, externalTableDataPath);
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
	
	private static boolean checkPathDeepth(String targetpath, Pattern pattern) {
		Matcher m = pattern.matcher(targetpath);
		return m.find();
	}
	
	@Override
	public void kill() throws IOException {
		// TODO Auto-generated method stub
	}
}
