/**
 * class: MrPipeRunner
 * vasion:v1.0
 * time: 11-24
 * author:lyndldeng
 */

package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.runner.TaskRunnerLoader;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.logging.Level;

public class MapReducePipeRunner extends MapReduceConfRunner {
	String mrTaskType = "Pipe";

	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(MapReducePipeRunner.class, (byte) 93);
	}

	/**
	 * kill task instance
	 */
	@Override
	public void kill() throws IOException {

	}

	protected boolean constructJobEnv() throws Exception {
		String executableName = null;
		if (this.job.getConfPropMap().containsKey("hadoop.pipes.executable")) {
			executableName = this.job.getConfPropMap().get(
					"hadoop.pipes.executable");
		}
		if (executableName != null) {
			String executableFilePath;
			String pipeJobUgi;
			try {
				executableFilePath = requestGlobalParameters().get(GLOBAL_DEFAULT_PIPE_PATH);
				pipeJobUgi = requestGlobalParameters().get(GLOBAL_PIPE_JOB_UGI);
			} catch (UnsupportedEncodingException e) {
				return false;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				return false;
			}
			String executableFile = executableFilePath + "/" + executableName;
			String hadoopPipeCmd = String.format(
					"%s fs -Dfs.default.name=%s -Dhadoop.job.ugi=%s ",
					this.job.getHadoopClient(), this.job.getHdfsStr(),
					pipeJobUgi);
			Map<String, String> confMap = this.job.getConfPropMap();
			confMap.put("hadoop.pipes.executable", executableFile);
			
			
			try {
				super.constructJobEnv();
				/*
				String lsCommand = hadoopPipeCmd + " -ls " + executableFile;
				if (runCmd(lsCommand, 5)) {
					String rmCommand = hadoopPipeCmd + " -rm " + executableFile;
					if (!runCmd(rmCommand, 5)) {
						return false;
					}
				} 
				String putCommand = hadoopPipeCmd + " -put " + executableName
						+ " " + executableFilePath + "/";
				if (!runCmd(putCommand, 5)) {
					return false;
				}
				*/
				return true;
			} catch (Exception e) {
				setexceptCode(3);
				throw e;
			}
			
		}
		return true;
	}

	public boolean executeTask() throws Exception {
		String hadoopJobCommand = this.job.getHadoopClient() + " pipes -conf "
				+ this.jobConfName;
		this.writeLocalLog(Level.INFO, "[EXECUTING CMD]" + hadoopJobCommand);
		try {
			if (runCmd( hadoopJobCommand, this.job.getMainTimeOut() )) {
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
			setexceptCode(8);
			throw e;
		}
	}
}