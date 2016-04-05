/**
 * class: MrPipeRunner
 * vasion:v1.0
 * time: 11-24
 * author:lyndldeng
 */

package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.runner.TaskRunnerLoader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;

public class MapReduceConfRunner extends AbstractDCMJobRunner {
	String mrTaskType = "Conf";
	String jobConfName = "jobconf.xml";

	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(MapReduceConfRunner.class, (byte) 91);
	}

	/**
	 * kill task instance
	 */
	@Override
	public void kill() throws IOException {

	}

	public boolean toJobXml() {
		// String job_name = this.job.getJobName();
		// String fConf = null;
		BufferedWriter bw = null;
		boolean flag = false;
		StringBuffer sb = new StringBuffer();
		Map<String, String> ConfPropMap = job.getConfPropMap();
		this.writeLocalLog(Level.INFO, "job File name " + this.job.getWorkDir()
				+ "/" + this.jobConfName);
		File confFile = new File(this.job.getWorkDir() + "/" + this.jobConfName);
		if (confFile.exists()) {
			confFile.delete();
		}
		try {
			if (confFile.createNewFile()) {
				this.writeLocalLog(Level.INFO, "[SUCCESS] File was created.");
				bw = new BufferedWriter(new FileWriter(confFile));
			} else {
				this.writeLocalLog(Level.INFO, "[Error] File created Failed!");
				return flag;
			}
		} catch (IOException e) {
			writeExceptionTraceToLog(e);
			return flag;
		}

		this.writeLocalLog(Level.INFO, "create job.conf");
		try {
			sb.append("<?xml version=\"1.0\" encoding=\"gbk\"?>\n<configuration>");
			for (Map.Entry<String, String> keyValue : ConfPropMap.entrySet()) {
				sb.append("\n\t<property>\n\t\t<name>" + keyValue.getKey()
						+ "</name>\n");
				sb.append("\t\t<value>" + keyValue.getValue() + "</value>\n");
				sb.append("\t</property>");
				flag = true;
			}
			sb.append("\n</configuration>");
			bw.write(sb.toString());
			bw.close();
		} catch (Exception e) {
			flag = false;
			writeExceptionTraceToLog(e);
			return flag;
		}
		return flag;
	}

	public boolean executeTask() throws Exception {

		String hadoopJobCommand = job.getHadoopClient() + " job";
		if (job.getConfPropMap().containsKey("libjars")) {
			String jobJars = job.getConfPropMap().get("libjars");
			hadoopJobCommand = hadoopJobCommand + " -libjars " + jobJars;
		}
		if (job.getConfPropMap().containsKey("files")) {
			String jobFiles = job.getConfPropMap().get("files");
			hadoopJobCommand = hadoopJobCommand + " -files " + jobFiles;
		}
		hadoopJobCommand = hadoopJobCommand + " -run " + this.jobConfName;
		// add time limit
		this.writeLocalLog(Level.INFO, "exceute cmd : " + hadoopJobCommand);
		try {
			if( runhadoopCmd( hadoopJobCommand, this.job.getMainTimeOut() )) {
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
			setexceptCode(7);
			throw e;
		}
	}
	
}