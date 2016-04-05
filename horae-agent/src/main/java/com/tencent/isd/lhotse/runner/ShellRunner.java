package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.AbstractTaskRunner;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.util.CommonUtils;
import com.tencent.isd.lhotse.runner.util.RunTaskProcess;
import org.apache.commons.lang.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.logging.Level;

public class ShellRunner extends AbstractTaskRunner {

	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(ShellRunner.class, (byte) 106);
	}

	@Override
	public void execute() throws IOException {
		RunTaskProcess process = null;
		String output = new String();
		int exitValue = -255;
		try {
			LTask task = getTask();
			this.writeLocalLog(Level.INFO, "=====================");
			String startLogString = "Start running ShellRunner:" + task.getId() + ", date:"
					+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(new Date(task.getCurRunDate()));
			this.writeLocalLog(Level.INFO, startLogString);
			this.writeLocalLog(Level.INFO, "begin to fetch parameters...");

			/**
			 * process task_ext params
			 */
			String shellCmd = this.getExtPropValueWithDefault("shell.cmd", "");
			String shellArgs = this.getExtPropValueWithDefault("shell.args", "");

			shellCmd = StringUtils.trim(shellCmd);
			shellArgs = StringUtils.trim(shellArgs);

			this.writeLocalLog(Level.INFO, String.format("shellCmd=%s, shellArgs=%s", shellCmd, shellArgs));

			if (StringUtils.isBlank(shellCmd)) {
				throw new Exception("Shell cmd is empty.");
			}

			/**
			 * params need to be standardized
			 */
			shellCmd = CommonUtils.standardizeDateString(shellCmd, task.getCurRunDate());
			shellArgs = CommonUtils.standardizeDateString(shellArgs, task.getCurRunDate());

			String[] shellArgsArray = StringUtils.split(shellArgs, " ");
			String[] cmd = new String[shellArgsArray.length + 1];
			cmd[0] = shellCmd;
			for (int i = 1; i < cmd.length; i++) {
				cmd[i] = shellArgsArray[i - 1];
			}
			this.writeLocalLog(Level.INFO, "Command: " + Arrays.toString(cmd));

			process = new RunTaskProcess(cmd, this);
			process.startProcess(false);

			process.waitAndDestroyProcess();
			exitValue = process.getExitVal();
			this.writeLocalLog(Level.INFO, "Command done, exit value=" + exitValue);

			output = process.getNormalResult() + process.getErrResult();
			this.writeLocalLog(Level.INFO, "Command output:" + output);
			commitTaskAndLog(LState.RUNNING, "FORMATTED_LOG", output);

			this.writeLocalLog(Level.INFO, "=====================\n");
		}
		catch (Exception e) {
			String st = CommonUtils.stackTraceToString(e);
			this.writeLocalLog(Level.SEVERE, "Exception stackTrace: " + st);
			commitTaskAndLog(LState.RUNNING, String.valueOf(exitValue), "Exception: " + e.getMessage());
			this.writeLocalLog(Level.INFO, "=====================\n");
			throw new IOException(e);
		}
		finally {
			JSONObject json = new JSONObject();
			putJson(json, "exit_code", String.valueOf(exitValue));
			if (exitValue != 0) {
				commitTaskAndLog(LState.FAILED, String.valueOf(exitValue), json.toString());
			}
			else {
				if (StringUtils.isNotBlank(output))
					commitTaskAndLog(LState.SUCCESSFUL, String.valueOf(exitValue), output);
				else {
					commitTaskAndLog(LState.SUCCESSFUL, String.valueOf(exitValue), json.toString());
				}

			}
		}
	}

	private void putJson(JSONObject json, String key, String value) {
		try {
			json.put(key, value);
		}
		catch (JSONException e) {
			this.writeLocalLog(Level.SEVERE, e.getMessage());
		}
	}

	@Override
	public void kill() throws IOException {
	}

	private void commitTaskAndLog(LState state, String runtimeId, String desc) {
		try {
			if (desc != null && desc.length() > 4000) {
				desc = StringUtils.substring(desc, 0, 4000);
			}
			this.commitTask(state, runtimeId, desc);
			// if (state != null)
			// throw new Exception("nima");
		}
		catch (Exception e) {
			String st = CommonUtils.stackTraceToString(e);
			this.writeLocalLog(Level.INFO, "Log_desc :" + desc);
			this.writeLocalLog(Level.INFO, "Log_desc length:" + desc.length());
			this.writeLocalLog(Level.SEVERE, "Commit task failed, StackTrace: " + st);
		}
	}

	private String getExtPropValueWithDefault(String key, String defaultValue) {
		String value = this.getExtPropValue(key);
		if (StringUtils.isBlank(value))
			value = defaultValue;
		return value;
	}

}