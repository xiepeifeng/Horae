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

		/* Make sure that the task server configuration is correct. */
		if (task.getSourceServersCount() != 3) {
			String failMessage = 
				"Task server configuration is incorrect, expect 3 source " +
				"HDFS servers configured, but " + task.getSourceServersCount() +
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
		
		/* Get TDW server configuration. */
		LServer hiveServer = task.getSourceServers(0);
		String hiveUser = hiveServer.getUserName();
		String hivePassword = hiveServer.getPassword();
		String hiveIP = hiveServer.getHost();
		String hivePort = String.valueOf(hiveServer.getPort());

		/* Get TDW name node configuration. */
		LServer nameNodeServer = task.getSourceServers(1);
		String nameNodeIP = nameNodeServer.getHost();
		String nameNodePort = String.valueOf(nameNodeServer.getPort());

		/* Get out the task configuration. */
		String databaseName = this.getExtPropValue("databaseName");
		String filterSQL = RunnerUtils.replaceDateExpr(this.getExtPropValue("filterSQL"), runDate);
		String destFilePath = RunnerUtils.replaceDateExpr(this.getExtPropValue("destFilePath"), runDate);
		String destFileDelimiter = this.getExtPropValue("destFileDelimiter");
		String destCheckFilePath = RunnerUtils.replaceDateExpr(this.getExtPropValue("destCheckFilePath"), runDate);
		String destCheckFileName = RunnerUtils.replaceDateExpr(this.getExtPropValue("destCheckFileName"), runDate);

		/* Get the global parameter. */
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
			if (!checkDbExistence(plcProgram, hiveUser, hivePassword, hiveIP, 
					              hivePort, databaseName)) {
				return;
			}

			/* Clean the destination path and create it. */
			File destFileDir = new File(destFilePath);
			RunnerUtils.deleteFile(destFileDir);
			destFileDir.mkdirs();

			/* Create the temporary path for creating external table. */
			hdfs = RunnerUtils.getHdfsFileSystem(nameNodeServer);
			if (!createTmpPath(hdfs, externalTableDataPath, nameNodeIP)) {
				return;
			}

			/* Create the external table. */
			if (!createExternalTableWithSQL(plcProgram, plcParameter, hiveUser, hivePassword, 
					                        hiveIP, hivePort, nameNodeIP, nameNodePort, 
     	                                    externalTableDataPath, databaseName, externalTableDb, 
     	                                    externalTableName, filterSQL, destFileDelimiter)) {
				return;
			}
			
			/* Execute the filtering SQL to write the data into external table. */
			SessionAndExitVal loadDataResult = 
				loadDataWithSQL(plcProgram, plcParameter, hiveUser, hivePassword, hiveIP, hivePort,
				                databaseName, externalTableDb, externalTableName, filterSQL);
			if (loadDataResult.getExitVal() != 0) {
				return;
			}

			/* Copy output files from HDFS to target Linux system and write the check file. */
			RunnerUtils.writeLogAndCheckFilesToLinux(hdfs, destFilePath, externalTableDataPath, null,
					                                 destCheckFilePath, destCheckFileName, this);

			/* Composite the JSON message. */
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("exit_code", "0");
			jsonObject.put("success_write", loadDataResult.getSuccessWrite());
			jsonObject.put("fail_write", loadDataResult.getFailWrite());
			jsonObject.put("run_date", runDate);
			commitTask(LState.SUCCESSFUL, "", jsonObject.toString());
			writeLocalLog(Level.INFO, jsonObject.toString());
		} catch (Exception e) {
			writeLocalLog(Level.SEVERE, e.getMessage());
			commitTask(LState.FAILED, "", "Load TDW data to linux failed.");
			e.printStackTrace();
			throw new IOException(e);
		} finally {
			doCleanWork(plcProgram, hiveUser, hivePassword, hiveIP, hivePort, 
					    externalTableDb, externalTableName, hdfs, externalTableDataPath);
		}
	}

	@Override
	public void kill() throws IOException {
	}
}