package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.util.CommonUtils;
import com.tencent.isd.lhotse.runner.util.RunnerUtils;
import com.tencent.isd.lhotse.runner.util.RunnerUtils.HdfsDirExistResult;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HdfsToHdfsRunner extends TDWDDCTaskRunner {
	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(HdfsToHdfsRunner.class, (byte) 104);
	}

	private static final String TARGET_PATH_DEEPTH_REGEX = "^(/([a-zA-Z0-9]+[-_.a-zA-Z0-9]*)){5,}+";
	private static final String SOURCE_PATH_DEEPTH_REGEX = "^(/([a-zA-Z0-9]+[-_.a-zA-Z0-9]*)){3,}+";
	private static Pattern targetPathDeepRang = null;
	private static Pattern sourcePathDeepRang = null;

	static {
		targetPathDeepRang = Pattern.compile(TARGET_PATH_DEEPTH_REGEX);
		sourcePathDeepRang = Pattern.compile(SOURCE_PATH_DEEPTH_REGEX);
	}

	@Override
	public void execute() throws IOException {
		LTask task = getTask();
		Map<String, String> keyValues = new HashMap<String, String>();
		/* Calculate the real data date, for tasks transferred from USP. */
		runDate = getRealDataDate(task);

		/* Check the server tags configuration for this task. */
		if (task.getSourceServersCount() < 2) {
			keyValues.put("exit_code", RunnerUtils.SERVER_CONFIG_ERROR_CODE);
			keyValues.put("task_desc", "Task server configuration is incorrect, expect at least 2"
					+ " source servers configured, but " + task.getSourceServersCount() + " server configured");
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return;
		}

		if (task.getTargetServersCount() < 2) {
			keyValues.put("exit_code", RunnerUtils.SERVER_CONFIG_ERROR_CODE);
			keyValues.put("task_desc", "Task server configuration is incorrect, expect at least 3 target "
					+ "servers configured, but " + task.getTargetServersCount() + " server configured");
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return;
		}

		/* Get the servers configurations. */
		LServer sourceHdfsServer = task.getSourceServers(0);
		LServer targetHdfsServer = task.getTargetServers(0);

		final String sourceHadoopCommand = getHadoopCommandOnVersion(sourceHdfsServer);
		final String targetHadoopCommand = getHadoopCommandOnVersion(targetHdfsServer);
		String sourceFilePath = null;
		String targetFilePath = null;
		boolean committed = false;
		boolean success = false;
		try {
			sourceFilePath = RunnerUtils.replaceDateExpr(this.getExtPropValue("sourceFilePath"), runDate);
			targetFilePath = RunnerUtils.replaceDateExpr(this.getExtPropValue("targetFilePath"), runDate);
		}
		catch (Exception e) {
			keyValues.put("exit_code", RunnerUtils.DATE_FORMAT_ERROR_CODE);
			keyValues.put("task_desc",
					"Task failed becuase date format is not correct: " + CommonUtils.stackTraceToString(e));
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return;
		}

		/* Return if there is no source files at all. */
		try {
			
			if (!checkPathDeepth(sourceFilePath, sourcePathDeepRang)) {
				keyValues.put("exit_code", RunnerUtils.WRONG_HADOOP_TARGET_PATH);
				keyValues.put("task_desc", "\"" + sourceFilePath + "\"" + " is a wrong source path!");
				keyValues.put("run_date", HOUR_FORMAT.format(runDate));
				commitJsonResult(keyValues, false, "");
				return;
			}
			
			if (!checkPathDeepth(targetFilePath, targetPathDeepRang)) {
				keyValues.put("exit_code", RunnerUtils.WRONG_HADOOP_TARGET_PATH);
				keyValues.put("task_desc", "\"" + targetFilePath + "\"" + " is a wrong target path!");
				keyValues.put("run_date", HOUR_FORMAT.format(runDate));
				commitJsonResult(keyValues, false, "");
				return;
			}
			
			if (checkHdfsEmptySource(sourceHadoopCommand, sourceHdfsServer, sourceFilePath, "*")) {
				committed = true;
				return;
			}

			try {
				HdfsDirExistResult resultTarget = RunnerUtils.isHdfsDirExist4h2h(targetHadoopCommand, targetHdfsServer,
						targetFilePath, this);
				if (resultTarget.isDirExists()) {
					RunnerUtils.deleteHdfsFile4h2h(targetHadoopCommand, targetHdfsServer, targetFilePath, "*", this);
				}
			}
			catch (Exception e) {
				keyValues.put("exit_code", RunnerUtils.UNKOWN_ERROR_CODE);
				keyValues.put("task_desc", "Check target hdfs path exception: " + CommonUtils.stackTraceToString(e));
				keyValues.put("run_date", HOUR_FORMAT.format(runDate));
				commitJsonResult(keyValues, false, "");
				return;
			}

			/* Copy data to the target TDW. */
			if (mrCopyData4h2h(task, sourceHdfsServer, sourceHdfsServer, targetHdfsServer, sourceFilePath, targetFilePath,
					true)) {
				success = true;
				
				/* Commit the successful message. */
				keyValues.put("exit_code", "0");
				keyValues.put("task_desc", "Copy Success!");
				keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			} else {
				success = false;
				
				/* Commit the successful message. */
				keyValues.put("exit_code", RunnerUtils.UNKOWN_ERROR_CODE);
				keyValues.put("task_desc", "Copy Failed in unknown ERROR!");
				keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			}
			
		}
		catch (Exception e) {
			keyValues.put("exit_code", RunnerUtils.CHECK_FILE_NOT_EXIST_CODE);
			keyValues.put("task_desc", "Transfor file exception: " + CommonUtils.stackTraceToString(e));
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return;
		} finally {
			
			try {
				if (!committed) {
				    commitJsonResult(keyValues, success, plcParameter);
				}
			} catch (Exception e) {
				this.writeLocalLog(Level.SEVERE, CommonUtils.stackTraceToString(e));
				throw new IOException(e);
			}
		}

	}

	@Override
	public void kill() throws IOException {
		// TODO Auto-generated method stub

	}

	/* check target path deepth lyndldeng */
	private static boolean checkPathDeepth(String targetpath, Pattern pattern) {
		Matcher m = pattern.matcher(targetpath);
		return m.find();
	}
}