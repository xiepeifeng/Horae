package com.tencent.isd.lhotse.runner.api;

import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.api.util.RegFileNameFilter;
import com.tencent.isd.lhotse.runner.api.util.RunnerUtils;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.logging.Level;

public class LinuxToHdfsRunner extends DDCTaskRunner {
	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(LinuxToHdfsRunner.class, (byte) 81);
	}

	@Override
	public void execute() 
		throws IOException {
		
		boolean success = false;
		try {			
			/* Initialize the parameters which will be used in this runner. */
			LTask task = getTask();	

			/* Fail the runner if the task's server configuration is incorrect. */
			if (task.getTargetServersCount() != 1) {
				String failMessage = 
					"Task server configuration is incorrect, expect 1 target " +
					"HDFS server configured, but " + task.getTargetServersCount() +
					" server configured";
				writeLocalLog(Level.SEVERE, failMessage);
				commitTask(LState.FAILED, "", failMessage);
				writeLogAndCommitTask(failMessage, Level.SEVERE, LState.FAILED);
				return;
			}
			
			/* Calculate the real data date. */
			Date runDate = getRealDataDate(task);
			
			/* 
			 * Check whether we need to skip the weekly task for those USP 
			 * weekly and monthly tasks. 
			 */
			if (skipWeeklyTaskExecution(task, runDate)) {
				return;
			}
			
			/* Get the source data configuration. */
			String sourceFilePath = 
				RunnerUtils.replaceDateExpr(this.getExtPropValue("sourceFilePath"), runDate);
			String sourceFileNames = 
				RunnerUtils.replaceDateExpr(this.getExtPropValue("sourceFileNames"), runDate);
			String destPath = 
				RunnerUtils.replaceDateExpr(this.getExtPropValue("destPath"), runDate);
			String destCheckFilePath =
				RunnerUtils.replaceDateExpr(this.getExtPropValue("destCheckFilePath"), runDate);
			String destCheckFileName =
				RunnerUtils.replaceDateExpr(this.getExtPropValue("destCheckFileName"), runDate);
			
			this.writeLocalLog(Level.INFO, "sourceFilePath: " + sourceFilePath + 
			                               ", sourceFileNames: " + sourceFileNames +
			                               ", destPath: " + destPath +
			                               ", destCheckFilePath: " + destCheckFilePath +
			                               ", destCheckFileName: " + destCheckFileName);			
			
			/* Return if there is no source files at all. */
			File sourcePath = new File(sourceFilePath);
			File[] sourceFiles = sourcePath.listFiles(new RegFileNameFilter(sourceFileNames));
			if (!sourcePath.exists() || (sourceFiles.length == 0)) {
				String successMessage =  
					"Source file path doesn't exist or no source files in " +
				    "the path, thought it succeed";
				writeLogAndCommitTask(successMessage, Level.INFO, LState.SUCCESSFUL);
				return;
			}
			
			/* Copy local files to HDFS and write the check file. */
			FileSystem hdfs = RunnerUtils.getHdfsFileSystem(task.getTargetServers(0));
			RunnerUtils.writeLogAndCheckFilesToHdfs(hdfs, destPath, destCheckFilePath, 
					                                destCheckFileName, sourceFilePath, 
					                                sourceFileNames, this);
			writeLocalLog(Level.INFO, "Copy all linux files to HDFS succeed");
	        success = true;
		} catch (Exception e) {
			writeLocalLog(Level.SEVERE, e.getMessage());
			e.printStackTrace();
			throw new IOException(e);
		} finally {
			if (success) {
				commitTask(LState.SUCCESSFUL, "", "Copy linux files to hdfs succeed");
			} else {
				commitTask(LState.FAILED, "", "Copy linux files to hdfs failed.");
			}
		}
	}	
	
	@Override
	public void kill() throws IOException {
		/* Data transfer doesn't support killing. */
	}
}