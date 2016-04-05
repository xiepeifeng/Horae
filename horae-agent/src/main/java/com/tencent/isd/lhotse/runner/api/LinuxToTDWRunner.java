package com.tencent.isd.lhotse.runner.api;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.api.util.RegFileNameFilter;
import com.tencent.isd.lhotse.runner.api.util.RunnerUtils;
import com.tencent.isd.lhotse.runner.api.util.RunnerUtils.SessionAndExitVal;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.logging.Level;

public class LinuxToTDWRunner extends TDWDDCTaskRunner {	
	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(LinuxToTDWRunner.class, (byte) 82);
	}

	@Override
	public void execute() 
		throws IOException {

		LTask task = getTask();

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
		String sourceFileNames = RunnerUtils.replaceDateExpr(this.getExtPropValue("sourceFileNames"), runDate);
		String loadMode = this.getExtPropValue("loadMode");
		String tableName = this.getExtPropValue("tableName");
		String partitionName = RunnerUtils.replaceDateExpr(this.getExtPropValue("partitionType"), runDate);
		String whereClause = RunnerUtils.replaceDateExpr(this.getExtPropValue("whereClause"), runDate);
		String delimiter = this.getExtPropValue("delimiter");
		String charSet = this.getExtPropValue("charSet");
		String databaseName = this.getExtPropValue("databaseName");
		String sourceColumnNames = this.getExtPropValue("sourceColumnNames");
		String targetColumnNames = this.getExtPropValue("targetColumnNames");
		writeLocalLog(Level.INFO, String.format(
				"[TASK PARAMS] loadMode=%s, tableName=%s, partitionName=%s, delimiter=%s, "
						+ "charSet=%s, databaseName=%s, sourceColumnNames, targetColumnNames", loadMode, tableName,
				partitionName, delimiter, charSet, databaseName, sourceColumnNames, targetColumnNames));

		/* Global parameter. */
		final String plcProgram = this.requestGlobalParameters().get(PLC_PROGRAM);
		final String tmpPathPrefix = this.requestGlobalParameters().get(NAME_NODE_TMP_PATH);	
		final String externalTableDb = this.requestGlobalParameters().get(EXTERNAL_TABLE_CREATE_DB);
		final String plcParameter = getPLCParameter(task.getId(), runDate);
		
		String externalTableName = "ext_" + task.getId() + System.currentTimeMillis();
		String externalTableDataPath = tmpPathPrefix + File.separator + task.getId() + 
				                       File.separator + System.currentTimeMillis();
		
		FileSystem hdfs = null;
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
			if (!createPartition(plcProgram, hiveUser, hivePassword, 
					             hiveServerIP, hiveServerPort, 
					             databaseName, tableName, partitionName)) {
				return;
			}
			
			/* Return if there is no source files at all. */
			File sourcePath = new File(sourceFilePath);
			File[] sourceFiles = 
				sourcePath.listFiles(new RegFileNameFilter(sourceFileNames));
			if (!sourcePath.exists() || (sourceFiles.length == 0)) {
				String successMessage = 
					"Task succeeded because no files in the source path";
				writeLogAndCommitTask(successMessage, Level.INFO, LState.SUCCESSFUL);
				return;
			}

			/* Before loading data, clean the old data first if it exists. */
			if (!clearOldData(loadMode, plcProgram, hiveUser, hivePassword, 
			                  hiveServerIP, hiveServerPort, databaseName, 
			                  tableName, partitionName, task)) {
		        return;
	        }
			
			/* Delete the target directory before copy. */
			hdfs = RunnerUtils.getHdfsFileSystem(hiveNameNodeServer);
			if (!createTmpPath(hdfs, externalTableDataPath, nameNodeIP)) {
				return;
			}

			/* Copy all the files in the local directory to remote HDFS. */
			for (File sourceFile : sourceFiles) {
				Path destFilePath = new Path(externalTableDataPath, sourceFile.getName());
				RunnerUtils.writeLinuxFileToHdfs(hdfs, destFilePath, sourceFile);
				writeLocalLog(Level.INFO, "Write file" + destFilePath.getName() + " to HDFS");
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
						 hiveServerPort, databaseName, externalTableName, tableName, 
						 targetColumnNames,	runDate, whereClause, externalTableDb);
			if (result.getExitVal() != 0) {				
				return;
			}

			/* Log the successful message. */
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("exit_code", "0");
			jsonObject.put("success_write", result.getSuccessWrite());
			jsonObject.put("fail_write", result.getFailWrite());
			jsonObject.put("run_date", runDate);
			
			commitTask(LState.SUCCESSFUL, result.getSessionId(), jsonObject.toString());
			writeLocalLog(Level.INFO, jsonObject.toString());
		} catch (Exception e) {
			writeLocalLog(Level.SEVERE, e.getMessage());
			commitTask(LState.FAILED, "", "Load data from Linux to TDW failed.");
			e.printStackTrace();
			throw new IOException(e);
		} finally {
			doCleanWork(plcProgram, hiveUser, hivePassword, hiveServerIP, 
						hiveServerPort, externalTableDb, externalTableName, 
						hdfs, externalTableDataPath);
		}
	}
	
	@Override
	public void kill() throws IOException {
		// TODO Auto-generated method stub
	}
}
