package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.util.CommonUtils;
import com.tencent.isd.lhotse.runner.util.RegFileNameFilter;
import com.tencent.isd.lhotse.runner.util.RunnerUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

public class LinuxToHdfsRunner extends TDWDDCTaskRunner {
	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(LinuxToHdfsRunner.class, (byte) 81);
	}

	@Override
	public void execute() 
		throws IOException {
		
		/* Initialize the parameters which will be used in this runner. */
		LTask task = getTask();	
		Map<String, String> keyValues = new HashMap<String, String>();

		/* Fail the runner if the task's server configuration is incorrect. */
		if (task.getTargetServersCount() != 1) {				
        	keyValues.put("exit_code", RunnerUtils.SERVER_CONFIG_ERROR_CODE);
        	keyValues.put("task_desc", "Task server configuration is incorrect, expect 1 target " +
					                   "HDFS server configured, but " + task.getTargetServersCount() +
					                   " server configured");
        	commitJsonResult(keyValues, false, "");
			return;
		}
		
		boolean success = false;
		boolean committed = false;
		try {			
			
			
			/* Calculate the real data date. */
			runDate = getRealDataDate(task);
			
			/* 
			 * Check whether we need to skip the weekly task for those USP 
			 * weekly and monthly tasks. 
			 */
			if (skipWeeklyTaskExecution(task, runDate)) {
				committed = true;
				return;
			}
			
			/* Get the source data configuration. */
			String sourceFilePath = null;
			String sourceFileNames = null;
			String destPath = null;
			String destCheckFilePath = null;
			String destCheckFileName = null;
			try {
			    sourceFilePath = 
				    RunnerUtils.replaceDateExpr(this.getExtPropValue("sourceFilePath"), runDate);
			    sourceFileNames = 
				    RunnerUtils.replaceDateExpr(this.getExtPropValue("sourceFileNames"), runDate);
			    destPath = 
				    RunnerUtils.replaceDateExpr(this.getExtPropValue("destPath"), runDate);
			    destCheckFilePath =
				    RunnerUtils.replaceDateExpr(this.getExtPropValue("destCheckFilePath"), runDate);
			    destCheckFileName =
				    RunnerUtils.replaceDateExpr(this.getExtPropValue("destCheckFileName"), runDate);						
			    this.writeLocalLog(Level.INFO, "sourceFilePath: " + sourceFilePath + 
			                                   ", sourceFileNames: " + sourceFileNames +
			                                   ", destPath: " + destPath +
			                                   ", destCheckFilePath: " + destCheckFilePath +
			                                   ", destCheckFileName: " + destCheckFileName);
			} catch (Exception e) {
				keyValues.put("exit_code", RunnerUtils.DATE_FORMAT_ERROR_CODE);
	        	keyValues.put("task_desc", "Task failed becuase date format is not correct: " + 
	        	                           CommonUtils.stackTraceToString(e));
	        	keyValues.put("run_date", HOUR_FORMAT.format(runDate));
	        	commitJsonResult(keyValues, false, "");
	        	committed = true;
	        	return;
			}
			
			/* Return if there is no source files at all. */			
			File sourcePath = new File(sourceFilePath);
			File[] sourceFiles = sourcePath.listFiles(new RegFileNameFilter(sourceFileNames));
			if (!sourcePath.exists() || (sourceFiles.length == 0)) {
				String successMessage =  
					"Source file path doesn't exist or no source files in " +
				    "the path, thought it succeed";
				writeLogAndCommitTask(successMessage, Level.INFO, LState.SUCCESSFUL);
				committed = true;
				return;
			}
			
			/* Copy local files to HDFS and write the check file. */
			LServer targetServer = task.getTargetServers(0);
			final String hadoopCommand = getHadoopCommandOnVersion(targetServer);
			if (!writeLogAndCheckFilesToHdfs(hadoopCommand, targetServer, destPath, 
				                             destCheckFilePath, destCheckFileName, 
				                             sourceFilePath, sourceFileNames)) {
				committed = true;
				return;
			}						
			writeLocalLog(Level.INFO, "Copy all linux files to HDFS succeed");
			
			keyValues.put("exit_code", "0");
			keyValues.put("task_desc", "Load data from HDFS to Linux succeed");
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
	        success = true;
		} catch (Exception e) {
        	keyValues.put("exit_code", RunnerUtils.UNKOWN_ERROR_CODE);
        	keyValues.put("task_desc", "Data transfer from Linux to Hdfs failed: " + 
        	                           CommonUtils.stackTraceToString(e));
        	keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			throw new IOException(e);
		} finally {
			try {
				if (!committed) {
				    commitJsonResult(keyValues, success, "");
				}
			} catch (Exception e) {
				this.writeLocalLog(Level.SEVERE, CommonUtils.stackTraceToString(e));
				throw new IOException(e);
			}
		}
	}	
	
	@Override
	public void kill() throws IOException {
		/* Data transfer doesn't support killing. */
	}
}