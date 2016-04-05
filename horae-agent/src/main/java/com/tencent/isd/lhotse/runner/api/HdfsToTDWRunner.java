package com.tencent.isd.lhotse.runner.api;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.api.util.RunnerUtils;
import com.tencent.isd.lhotse.runner.api.util.RunnerUtils.SessionAndExitVal;
import org.apache.hadoop.fs.FileSystem;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.logging.Level;

public class HdfsToTDWRunner extends TDWDDCTaskRunner {
	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(HdfsToTDWRunner.class, (byte) 85);
	}

	@Override
	public void execute() 
		throws IOException {
		
		LTask task = getTask();		
		
		/* Check the server tags configuration for this task. */
		if (task.getSourceServersCount() != 1) {
			String failMessage = 
				"Task server configuration is incorrect, expect 1 source " +
				"HDFS servers configured, but " + task.getSourceServersCount() +
				" server configured";						
			writeLocalLog(Level.SEVERE, failMessage);
			commitTask(LState.FAILED, "", failMessage);
			return;
		}
		
		for (LServer server : task.getTargetServersList()) {
			System.err.println(server.getHost());
		}
		
		if (task.getTargetServersCount() != 3) {
			String failMessage = 
				"Task server configuration is incorrect, expect 3 target " +
				"servers configured, but " + task.getTargetServersCount() +
				" server configured";				
			writeLogAndCommitTask(failMessage, Level.SEVERE, LState.FAILED);
			return;
		}
		
		/* Calculate the real data date, for tasks transferred from USP. */
		Date runDate = getRealDataDate(task);

		/* 
		 * Check whether we need to skip the weekly task for those USP 
		 * weekly and monthly tasks. 
		 */
		if (skipWeeklyTaskExecution(task, runDate)) {
			return;
		}
		
		/* Configure the target HDFS cluster parameters. */
		LServer hdfsServer = task.getSourceServers(0);
		String hdfsUser = hdfsServer.getUserName();
		String hdfsGroup = hdfsServer.getUserGroup();
		String hdfsIP = hdfsServer.getHost();
		String hdfsPort = String.valueOf(hdfsServer.getPort());
		writeLocalLog(Level.INFO,
				String.format("[HDFS] user=%s, group=%s, server=%s, port=%s", hdfsUser, hdfsGroup, hdfsIP, hdfsPort));

		/* Configure the hive server parameters. */
		LServer hiveServer = task.getTargetServers(0);
		String hiveUser = hiveServer.getUserName();
		String hivePassword = hiveServer.getPassword();
		String hiveServerIP = hiveServer.getHost();
		String hiveServerPort = String.valueOf(hiveServer.getPort());
		writeLocalLog(Level.INFO, String.format("[PLC] plc=%s, user=%s, pass=%s, server=%s, port=%s", "PLC", hiveUser,
				hivePassword, hiveServerIP, hiveServerPort));

		/* Configure the name node server parameters for TDW. */
		LServer hiveNameNodeServer = task.getTargetServers(1);
		String nameNodeUser = hiveNameNodeServer.getUserName();
		String nameNodeGroup = hiveNameNodeServer.getUserGroup();
		String nameNodeIP = hiveNameNodeServer.getHost();
		String nameNodePort = String.valueOf(hiveNameNodeServer.getPort());
		writeLocalLog(Level.INFO, String.format("[NameNode] user=%s, group=%s, server=%s, port=%s", nameNodeUser,
				nameNodeGroup, nameNodeIP, nameNodePort));
		
		/* Get the task configurations. */
		String sourceFilePath = RunnerUtils.replaceDateExpr(this.getExtPropValue("sourceFilePath"), runDate);
		String loadMode = this.getExtPropValue("loadMode");
		String tableName = this.getExtPropValue("tableName");
		String partitionName = RunnerUtils.replaceDateExpr(this.getExtPropValue("partitionType"), runDate);
		String whereClause = 
			RunnerUtils.replaceDateExpr(this.getExtPropValue("whereClause"), runDate);
		String delimiter = this.getExtPropValue("delimiter");
		String charSet = this.getExtPropValue("charSet");
		String databaseName = this.getExtPropValue("databaseName");
		String sourceColumnNames = this.getExtPropValue("sourceColumnNames");
		String targetColumnNames = this.getExtPropValue("targetColumnNames");
		String checkFileName = RunnerUtils.replaceDateExpr(this.getExtPropValue("checkFileName"), runDate);
		writeLocalLog(Level.INFO, String.format(
				"[TASK PARAMS] loadMode=%s, tableName=%s, partitionName=%s, delimiter=%s, "
						+ "charSet=%s, databaseName=%s, sourceColumnNames, targetColumnNames", loadMode, tableName,
				partitionName, delimiter, charSet, databaseName, sourceColumnNames, targetColumnNames));

		/* Get the global parameter. */
		final String plcProgram = this.requestGlobalParameters().get(PLC_PROGRAM);
		final String hadoopCommand = this.requestGlobalParameters().get(HADOOP_COMMAND);
		final String tmpPathPrefix = this.requestGlobalParameters().get(NAME_NODE_TMP_PATH);
		final String externalTableDb = this.requestGlobalParameters().get(EXTERNAL_TABLE_CREATE_DB);
		final String plcParameter = getPLCParameter(task.getId(), runDate);

		String externalTableName = "ext_" + task.getId() + System.currentTimeMillis();
		String externalTableDataPath = sourceFilePath;
		
		FileSystem nameNodeFS = null;
		try {
			/* Check to see whether the database exists or not. */
			if (!checkDbExistence(plcProgram, hiveUser, hivePassword, 
				                  hiveServerIP, hiveServerPort, databaseName)) {
				return;
			}		
			
			/* Check to see whether the table exists or not. */
			if (!checkTableExistence(plcProgram, hiveUser, hivePassword, 
					                 hiveServerIP, hiveServerPort,
					                 databaseName, tableName)) {
				return;
			}

			/* Create the partition if the partition doesn't exist. */
			if (!createPartition(plcProgram, hiveUser, 
							     hivePassword, hiveServerIP,
						         hiveServerPort, databaseName, 
						         tableName, partitionName)) {
				return;
			}

			/* Return if there is no source files at all. */
			if (checkHdfsEmptySource(hdfsServer, sourceFilePath)) {
				return;
			}			
			
			/* Before loading data, clean the old data first if it exists. */
			if (!clearOldData(loadMode, plcProgram, hiveUser, hivePassword, 
					          hiveServerIP, hiveServerPort, databaseName, 
					          tableName, partitionName, task)) {
				return;
			}

			/*
			 * If source HDFS is actually the TDW cluster, create external 
			 * table on the source file path, otherwise copy data from 
			 * source HDFS to a temporary path on TDW name node.
			 */
			nameNodeFS = RunnerUtils.getHdfsFileSystem(hiveNameNodeServer);
			if (!nameNodeIP.equals(hdfsIP)) {

				/* Create temporary path on TDW name node. */
				externalTableDataPath = 
					tmpPathPrefix + File.separator + task.getId() + 
					File.separator + System.currentTimeMillis();

				/* First check whether the temporary path for saving data exists. */
				if (!createTmpPath(nameNodeFS, externalTableDataPath, nameNodeIP)) {
					return;
				}

				/* Copy data to the target TDW. */
				if (!mrCopyData(task, hadoopCommand, hdfsIP, hdfsPort, nameNodeIP, 
						        nameNodePort, nameNodeUser, nameNodeGroup, sourceFilePath, 
						        externalTableDataPath, nameNodeFS, true)) {
					return;
				}
			}
			
			/* Delete the check file if it exists in the external table data path. */
			if (checkFileName != null) {
				RunnerUtils.deleteHdfsFileWithPattern
				    (nameNodeFS, externalTableDataPath, checkFileName, this);
			}

			/* Create the external table. */
			if (!createExternalTable(plcProgram, plcParameter, hiveUser, hivePassword,
					                 hiveServerIP, hiveServerPort, nameNodeIP, 
					                 nameNodePort, externalTableDataPath, externalTableDb,
					                 externalTableName, sourceColumnNames, delimiter, charSet)) {
				return;
			}
			
			/* Load data from external table into TDW. */
			SessionAndExitVal result = 
				loadData(plcProgram, plcParameter, hiveUser, hivePassword, hiveServerIP, 
						 hiveServerPort, databaseName, externalTableName, 
						 tableName, targetColumnNames, runDate, whereClause, 
						 externalTableDb);
			if (result.getExitVal() != 0) {				
				return;
			}
			
			/* Commit the successful message. */
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("exit_code", "0");
			jsonObject.put("success_write", result.getSuccessWrite());
			jsonObject.put("fail_write", result.getFailWrite());
			jsonObject.put("run_date", runDate);
			commitTask(LState.SUCCESSFUL, result.getSessionId(), jsonObject.toString());
			writeLocalLog(Level.INFO, jsonObject.toString());
		} catch (Exception e) {
			writeLocalLog(Level.SEVERE, e.getMessage());
			commitTask(LState.FAILED, "", "Load data from Hdfs to TDW failed");
			e.printStackTrace();
			throw new IOException(e);
		} finally {
			doCleanWork(plcProgram, hiveUser, hivePassword, hiveServerIP,     
					    hiveServerPort, externalTableDb, externalTableName, 
					    nameNodeFS, externalTableDataPath);
		}
	}
	
	@Override
	public void kill() throws IOException {
		// TODO Auto-generated method stub
	}
}
