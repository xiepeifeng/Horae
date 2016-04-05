package com.tencent.isd.lhotse.runner.api;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.api.util.RunnerUtils;
import com.tencent.isd.lhotse.runner.api.util.RunnerUtils.SessionAndExitVal;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Level;

public class TDWToHdfsRunner extends TDWDDCTaskRunner {
	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(TDWToHdfsRunner.class, (byte) 86);
	}

	@Override
	public void execute() 
		throws IOException {

		LTask task = getTask();

		/* Check the server tags configuration for this task. */
		if (task.getSourceServersCount() != 3) {
			String failMessage = 
				"Task server configuration is incorrect, expect 3 source " +
				"HDFS servers configured, but " + task.getSourceServersCount() +
				" server configured";						
			writeLogAndCommitTask(failMessage, Level.SEVERE, LState.FAILED);
			return;
		}
		
		if (task.getTargetServersCount() != 1) {
			String failMessage = 
				"Task server configuration is incorrect, expect 1 target " +
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

		/* Get the hive server configuration. */
		LServer hiveServer = task.getSourceServers(0);
		String hiveUser = hiveServer.getUserName();
		String hivePassword = hiveServer.getPassword();
		String hiveIP = hiveServer.getHost();
		String hivePort = String.valueOf(hiveServer.getPort());
		writeLocalLog(Level.INFO, String.format("[EXT PARAMS] hiveUser=%s, hivePassword=%s, hiveIP=%s, hivePort=%s",
				hiveUser, hivePassword, hiveIP, hivePort));

		/* Get the name node configuration for this TDW. */
		LServer nameNodeServer = task.getSourceServers(1);
		String nameNodeUser = nameNodeServer.getUserName();
		String nameNodeGroup = nameNodeServer.getUserGroup();
		String nameNodeIP = nameNodeServer.getHost();
		String nameNodePort = String.valueOf(nameNodeServer.getPort());
		writeLocalLog(Level.INFO, String.format(
				"[EXT PARAMS] nameNodeUser=%s, nameNodeGroup=%s, nameNodeIP=%s, nameNodePort=%s", nameNodeUser,
				nameNodeGroup, nameNodeIP, nameNodePort));

		/* Get the target HDFS cluster configuration. */
		LServer hdfsServer = task.getTargetServers(0);
		String hdfsUser = hdfsServer.getUserName();
		String hdfsGroup = hdfsServer.getUserGroup();
		String hdfsIP = hdfsServer.getHost();
		String hdfsPort = String.valueOf(hdfsServer.getPort());
		writeLocalLog(Level.INFO, String.format("[EXT PARAMS] hdfsUser=%s, hdfsGroup=%s, hdfsIP=%s, hdfsPort=%s",
				hdfsUser, hdfsGroup, hdfsIP, hdfsPort));

		/* Get the task configuration. */
		String databaseName = this.getExtPropValue("databaseName");
		String filterSQL = RunnerUtils.replaceDateExpr(this.getExtPropValue("filterSQL"), runDate);
		String destFilePath = RunnerUtils.replaceDateExpr(this.getExtPropValue("destFilePath"), runDate);
		String destFileDelimiter = this.getExtPropValue("destFileDelimiter");
		String destCheckFilePath = RunnerUtils.replaceDateExpr(this.getExtPropValue("destCheckFilePath"), runDate);
		String destCheckFileName = RunnerUtils.replaceDateExpr(this.getExtPropValue("destCheckFileName"), runDate);

		/* Get the global parameter. */
		final String plcProgram = this.requestGlobalParameters().get(PLC_PROGRAM);
		final String hadoopCommand = this.requestGlobalParameters().get(HADOOP_COMMAND);
		final String tmpPathPrefix = this.requestGlobalParameters().get(NAME_NODE_TMP_PATH);
		final String externalTableDb = this.requestGlobalParameters().get(EXTERNAL_TABLE_CREATE_DB);
		final String plcParameter = getPLCParameter(task.getId(), runDate);

		String externalTableName = "ext_" + task.getId() + System.currentTimeMillis();
		String externalTableDataPath = destFilePath;

		FileSystem nameNodeFS = null;
		try {
			/* Check to see whether the database exists or not. */
			if (!checkDbExistence(plcProgram, hiveUser, hivePassword, hiveIP, 
					              hivePort, databaseName)) {
				return;
			}

			/* Clean the target file and check file path. */
			FileSystem hadoopFS = RunnerUtils.getHdfsFileSystem(hdfsServer);
			nameNodeFS = RunnerUtils.getHdfsFileSystem(nameNodeServer);
			RunnerUtils.deleteHdfsFile(hadoopFS, new Path(destFilePath));
			hadoopFS.mkdirs(new Path(destFilePath));
			if (destCheckFilePath.equals(destFilePath)) {
				RunnerUtils.deleteHdfsFile(hadoopFS, new Path(destCheckFilePath, destCheckFileName));
			} else {
				RunnerUtils.deleteHdfsFile(hadoopFS, new Path(destCheckFilePath));
				hadoopFS.mkdirs(new Path(destCheckFilePath));
			}
			writeLocalLog(Level.INFO, "Clean the target files and check file successfully");

			/* Need to create temporary directory for external table data. */
			if (!nameNodeIP.equalsIgnoreCase(hdfsIP)) {
				externalTableDataPath = 
					tmpPathPrefix + File.separator + task.getId() + 
					File.separator + System.currentTimeMillis();

				/* First check whether the temporary path for saving data exists. */
				if (!createTmpPath(nameNodeFS, externalTableDataPath, nameNodeIP)) {
					return;
				}
			}

			/* Create external table using the filter SQL. */
			if (!createExternalTableWithSQL(plcProgram, plcParameter, hiveUser, hivePassword, hiveIP, 
					                        hivePort, nameNodeIP, nameNodePort, externalTableDataPath, 
					                        databaseName, externalTableDb, externalTableName, filterSQL, 
					                        destFileDelimiter)) {
				return;
			}
			
			/* Load data to the external table. */
			SessionAndExitVal loadDataResult = 
				loadDataWithSQL(plcProgram, plcParameter, hiveUser, hivePassword, hiveIP, hivePort,
				                databaseName, externalTableDb, externalTableName, filterSQL);
			if (loadDataResult.getExitVal() != 0) {				
				return;
			}

			/* Copy the external table data to the target HDFS cluster. */
			if (!nameNodeIP.equalsIgnoreCase(hdfsIP)) {
				if (!mrCopyData(task,hadoopCommand, nameNodeIP, nameNodePort, hdfsIP, hdfsPort,
						        hdfsUser, hdfsGroup, externalTableDataPath, destFilePath, hadoopFS, false)) {
					return;
				}
			}

			/* Delete the old check file if it exists. */
			Path destCheckFile = new Path(destCheckFilePath, destCheckFileName);
			RunnerUtils.deleteHdfsFile(hadoopFS, destCheckFile);
			ArrayList<String> targetFileNames = new ArrayList<String>();
			for (FileStatus targetFile : hadoopFS.listStatus(new Path(destFilePath))) {
				targetFileNames.add(targetFile.getPath().getName());
			}

			/* Write the check file to the target HDFS cluster. */
			RunnerUtils.writeHdfsCheckFile(hadoopFS, destCheckFilePath, destCheckFileName, targetFileNames);
			writeLocalLog(Level.INFO, "Write check file successfully");

			JSONObject jsonObject = new JSONObject();
			jsonObject.put("exit_code", "0");
			jsonObject.put("success_write", loadDataResult.getSuccessWrite());
			jsonObject.put("fail_write", loadDataResult.getFailWrite());
			jsonObject.put("run_date", runDate);
			commitTask(LState.SUCCESSFUL, "", jsonObject.toString());
			writeLocalLog(Level.INFO, jsonObject.toString());
		} catch (Exception e) {
			e.printStackTrace();
			writeLocalLog(Level.SEVERE, e.getMessage());
			commitTask(LState.FAILED, "", "TDWExport failed.");
			throw new IOException(e);
		} finally {
			doCleanWork(plcProgram, hiveUser, hivePassword, hiveIP, hivePort, 
					    externalTableDb, externalTableName, nameNodeFS, 
					    externalTableDataPath);
		}
	}
	
	@Override
	public void kill() throws IOException {
	}
}