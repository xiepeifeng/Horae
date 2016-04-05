package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.util.CommonUtils;
import com.tencent.isd.lhotse.runner.util.RunnerUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

public class HdfsToLinuxRunner extends TDWDDCTaskRunner {
	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(HdfsToLinuxRunner.class, (byte) 80);
	}

	@Override
	public void execute() 
		throws IOException {
		
		/* Initialize the parameters which will be used in this runner. */
		LTask task = getTask();
		Map<String, String> keyValues = new HashMap<String, String>();

		/* Fail the runner if the task server configuration is incorrect. */			
		if (task.getSourceServersCount() != 1) {
        	keyValues.put("exit_code", RunnerUtils.SERVER_CONFIG_ERROR_CODE);
        	keyValues.put("task_desc", "Task server configuration is incorrect, expect 1 source " +
				                       "HDFS server configured, but " + task.getSourceServersCount() +
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
			LServer sourceServer = task.getSourceServers(0);
			final String hadoopCommand = getHadoopCommandOnVersion(sourceServer);
			if (checkHdfsEmptySource(hadoopCommand, sourceServer, sourceFilePath, sourceFileNames)) {
				committed = true;
				return;
			}
			
			if (!writeLogAndCheckFilesToLinux(hadoopCommand, sourceServer, destPath, sourceFilePath, 
				    	                      sourceFileNames, destCheckFilePath, destCheckFileName, this)) {
				committed = true;
				return;
			}			
			writeLocalLog(Level.INFO, "All hadoop files are copied to Linux");
			
			keyValues.put("exit_code", "0");
			keyValues.put("task_desc", "Load data from HDFS to Linux succeed");
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			success = true;	        
		} catch (Exception e) {			
        	keyValues.put("exit_code", RunnerUtils.UNKOWN_ERROR_CODE);
        	keyValues.put("task_desc", "Data transfer from Hdfs to Linux failed: " + 
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