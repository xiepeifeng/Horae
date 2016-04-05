package com.tencent.isd.lhotse.runner.api;

import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.api.util.HDFSRegExprFilter;
import com.tencent.isd.lhotse.runner.api.util.RunnerUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Date;
import java.util.logging.Level;

public class HdfsToLinuxRunner extends DDCTaskRunner {
	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(HdfsToLinuxRunner.class, (byte) 80);
	}

	@Override
	public void execute() 
		throws IOException {
		
		boolean success = false;
		try {			
			/* Initialize the parameters which will be used in this runner. */
			LTask task = getTask();

			/* Fail the runner if the task server configuration is incorrect. */			
			if (task.getSourceServersCount() != 1) {
				String failMessage = 
					"Task server configuration is incorrect, expect 1 source " +
				    "HDFS server configured, but " + task.getSourceServersCount() +
					" server configured";
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
			
			/* Move the HDFS files to Linux and write check file. */
			FileSystem hdfs = RunnerUtils.getHdfsFileSystem(task.getSourceServers(0));
			
			/* Return if there is no source files at all. */			
			Path sourcePath = new Path(sourceFilePath);
			FileStatus[] sourceFiles = hdfs.listStatus(sourcePath, new HDFSRegExprFilter(sourceFileNames, this));
			if (!hdfs.exists(sourcePath) || (sourceFiles.length == 0)) {
				String successMessage = 
					"Task succeeded because no files in the source path";
				writeLogAndCommitTask(successMessage, Level.INFO, LState.SUCCESSFUL);
				return;
			}
			
			RunnerUtils.writeLogAndCheckFilesToLinux(hdfs, destPath, sourceFilePath, sourceFileNames, 
					                                 destCheckFilePath, destCheckFileName, this);
			writeLocalLog(Level.INFO, "All hadoop files are copied to Linux");
			success = true;	        
		} catch (Exception e) {			
			this.writeLocalLog(Level.WARNING, e.getMessage());
			e.printStackTrace();
			throw new IOException(e);
		} finally {
			if (success) {
				commitTask(LState.SUCCESSFUL, "", 
						   "Copy hadoop files to linux succeed");
			} else {
				commitTask(LState.FAILED, "", 
						   "Copy hadoop files to linux failed.");
			}
		}
	}

	@Override
	public void kill() throws IOException {
		/* Data transfer doesn't support killing. */
	}
}