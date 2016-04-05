/**
 * class: MrPipeRunner
 * vasion:v1.0
 * time: 11-24
 * author:lyndldeng
 */

package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.runner.TaskRunnerLoader;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;

public class MapReduceCmdRunner extends AbstractDCMJobRunner {
	String mrTaskType = "Cmd";

	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(MapReduceCmdRunner.class, (byte) 92);
	}

	/**
	 * kill task instance
	 */
	@Override
	public void kill() throws IOException {

	}

	/*
	 * def toSlashD(self): property = '' for key in self._jobcontext._confMap:
	 * property = property + '-D%s=%s ' % (key,self._jobcontext._confMap[key])
	 * return property
	 */

	private String toSlashD() {
		String dProperty = "";
		Set<String> keySet = this.job.getConfPropMap().keySet();
		Iterator<String> iter = keySet.iterator();
		while (iter.hasNext()) {
			String propName = iter.next();
			String propValue = this.job.getConfPropMap().get(propName);
			dProperty = dProperty
					+ String.format("-D%s=%s ", propName, propValue);
		}
		return dProperty;
	}

	public boolean executeTask() throws Exception {
		String mainArgs = this.job.getTaskMainArgs();
		String dProperties = toSlashD();
		String[] cmdArgs = mainArgs.split(" ");
		String hadoopJobCmd = this.job.getHadoopClient() + " jar";
		if (cmdArgs.length == 2) {
			hadoopJobCmd = hadoopJobCmd
					+ String.format(" %s %s %s", cmdArgs[0], cmdArgs[1],
							dProperties);
		} else {
			String lastMainArgs = null;
			for (int ind = 2; ind < cmdArgs.length; ind++) {
				if (lastMainArgs == null)
					lastMainArgs = cmdArgs[ind];
				else
					lastMainArgs = lastMainArgs + " " + cmdArgs[ind];
			}
			hadoopJobCmd = hadoopJobCmd
					+ String.format(" %s %s %s %s", cmdArgs[0], cmdArgs[1],
							dProperties, lastMainArgs);
		}
		writeLocalLog(Level.INFO, "exceute cmd : " + hadoopJobCmd);
		try {
			if (runhadoopCmd(hadoopJobCmd, this.job.getMainTimeOut() )) {
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
			setexceptCode(6);
			throw e;
		}
	}
}