/**
 * PbToTDWRunner
 * time:1-15
 * version:v1.0
 * author:jessicajin
 * funcation: ddc
 */
package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.util.CommonUtils;
import com.tencent.isd.lhotse.runner.util.RunnerUtils;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;


public class PbToTDWRunner extends TDWDDCTaskRunner{

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		TaskRunnerLoader.startRunner(PbToTDWRunner.class, (byte) 95);
	}

	@Override
	public void execute() throws IOException {
		System.out.println("begin_pb_to_tdw");
		// TODO Auto-generated method stub
		LTask task = getTask();	  	
		Map<String, String> keyValues = new HashMap<String, String>();
		
		/* Calculate the real data date, for tasks transferred from USP. */
		runDate = getRealDataDate(task);
		
		/* Check the server tags configuration for this task. */
		if (task.getSourceServersCount()<2) {
        	keyValues.put("exit_code", RunnerUtils.SERVER_CONFIG_ERROR_CODE);
        	keyValues.put("task_desc", "Task server configuration is incorrect, expect at least 2 source " +
    				                   "HDFS servers configured, but " + task.getSourceServersCount() +
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
		
		/* Get the servers configurations. */
		LServer hdfsServer = task.getSourceServers(0);
		LServer hiveServer = task.getTargetServers(0);
		LServer hiveNameNodeServer = task.getTargetServers(1);
		
		
		/* Get the task configurations. */		
		String loadMode = this.getExtPropValue("loadMode");
		String tableName = this.getExtPropValue("tableName");
		String databaseName = this.getExtPropValue("databaseName");
		String failedOnZeroWrited = this.getExtPropValue("failedOnZeroWrited");
		String partitionName = null;
		String sourceFilePath = null;
		String sourceFileNames = null;
		
		try {
		    partitionName = 
		    	RunnerUtils.replaceDateExpr(this.getExtPropValue("partitionType"), runDate);
			sourceFilePath = 
				RunnerUtils.replaceDateExpr(this.getExtPropValue("sourceFilePath"), runDate);
			sourceFileNames = 
				RunnerUtils.replaceDateExpr(this.getExtPropValue("sourceFileNames"), runDate);
			//roach_xiang
			tableName=RunnerUtils.replaceDateExpr(tableName, runDate);
			this.writeLocalLog(Level.INFO,"tableName:"+tableName);
			//---------------------------
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
		//final String targetHadoopCommand = getHadoopCommandOnVersion(hiveNameNodeServer);
		final String tmpPathPrefix = this.requestGlobalParameters().get(BASE_PATH);
		plcParameter = getPLCParameter(task.getId(), runDate);
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
			this.writeLocalLog(Level.INFO,"Check to see whether the database exists or not");
			
			/* Check to see whether the table exists or not. */
			if (!checkTableWithPG(conn, databaseName, tableName)) {
				committed = true;
				return;
			}
			this.writeLocalLog(Level.INFO,"Check to see whether the table exists or not");
			/* Create the partition if the partition doesn't exist. */
			System.out.println(partitionName);
			//-----test
			if("NULL".equals(partitionName)){
				partitionName=null;
			}
			//test
			partitionName=partitionName.toLowerCase();
			this.writeLocalLog(Level.INFO,"the partition is: "+partitionName );
			if (!createPartitionWithPG(plcProgram, conn, hiveServer, 
					                   databaseName, tableName, partitionName)) {
				committed = true;
				return;
			}
			this.writeLocalLog(Level.INFO, "Create the partition if the partition doesn't exist:" + 
			                   partitionName);
			/* Return if there is no source files at all. */
			if (checkHdfsEmptySource(sourceHadoopCommand, hdfsServer, 
					                 sourceFilePath, sourceFileNames,
					                 failedOnZeroWrited)) {
				committed = true;
				return;
			}			
			this.writeLocalLog(Level.INFO,"Return if there is no source files at all"+ committed);
			/* Before loading data, clean the old data first if it exists. */
			if (!clearOldData(loadMode, plcProgram, hiveServer, databaseName, 
					          tableName, partitionName, task)) {
				committed = true;
				return;
			}
			this.writeLocalLog(Level.INFO,"clean the old data first if it exists:"+ loadMode);
			
			/*
			 * If source HDFS is actually the TDW cluster, create external 
			 * table on the source file path, otherwise copy data from 
			 * source HDFS to a temporary path on TDW name node.
			 */
			if (!hiveNameNodeServer.getHost().equals(hdfsServer.getHost())) {

				/* Create temporary path on TDW name node. */
				if (partitionName != null && partitionName.length() > 2) {
					externalTableDataPath = tmpPathPrefix + File.separator
							+ databaseName + ".db" + File.separator + tableName
							+ File.separator + partitionName;
				} else {
					externalTableDataPath = tmpPathPrefix + File.separator
							+ databaseName + ".db" + File.separator + tableName;
				}
				/* Copy data to the target TDW. */
				if (!mrCopyData(task, hiveServer, hdfsServer, hiveNameNodeServer, 
						        sourceFilePath, externalTableDataPath, true)) {
					committed = true;
					return;
				}
			}	
			this.writeLocalLog(Level.INFO,"distcp:"+sourceFilePath+" to : "+ externalTableDataPath);
			
			/* Commit the successful message. */
			keyValues.put("exit_code", "0");
			keyValues.put("desc","successful");
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));			
			success = true;
		} catch (SQLException e) {
			keyValues.put("exit_code", RunnerUtils.BI_DB_ERROR_CODE);
			keyValues.put("task_desc",
					      "Connect to BI database failed: " + CommonUtils.stackTraceToString(e));
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
		} catch (Exception e) {
        	keyValues.put("exit_code", RunnerUtils.UNKOWN_ERROR_CODE);
        	keyValues.put("task_desc", "Load data from Hdfs to TDW failed: " + CommonUtils.stackTraceToString(e));
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
