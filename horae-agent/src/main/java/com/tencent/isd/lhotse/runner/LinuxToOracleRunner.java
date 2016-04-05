package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.AbstractTaskRunner;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.util.CommonUtils;
import com.tencent.isd.lhotse.runner.util.DxDESCipher;
import com.tencent.isd.lhotse.runner.util.RunTaskProcess;
import org.apache.commons.lang.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.logging.Level;

import static com.tencent.isd.lhotse.runner.util.CommonUtils.mergekeyValue;

public class LinuxToOracleRunner extends AbstractTaskRunner {

	// TODO:NEED TO BE TEST!!!
	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(LinuxToOracleRunner.class, (byte) 101);
	}

	@Override
	public void execute() throws IOException {
		RunTaskProcess dxProcess = null;
		String output = new String();
		int exitValue = -255;
		try {
			LTask task = getTask();
			this.writeLocalLog(Level.INFO, "=====================");
			String startLogString = "Start running LinuxToOracleRunner:" + task.getId() + ", date:"
					+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(new Date(task.getCurRunDate()));
			this.writeLocalLog(Level.INFO, startLogString);

			this.writeLocalLog(Level.INFO, "begin to fetch parameters...");
			/**
			 * get global config
			 */
			Map<String, String> paramMap = this.requestGlobalParameters();
			final String DX_TEMP_PATH = paramMap.get("dx_temp_path");
			final String DX_HOME = paramMap.get("dx_home");
			final String DX_TEMPLATE_PATH = paramMap.get("dx_template_linux_to_oracle");

			if (CommonUtils.isBlankAny(DX_TEMP_PATH, DX_HOME, DX_TEMPLATE_PATH)) {
				throw new IOException(String.format(
						"Empty value found: dx_temp_path=%s, dx_home=%s, dx_template_linux_to_oracle=%s", DX_TEMP_PATH,
						DX_HOME, DX_TEMPLATE_PATH));
			}
			this.writeLocalLog(Level.INFO, String.format("dx_temp_path=%s, dx_home=%s, dx_template_linux_to_oracle=%s",
					DX_TEMP_PATH, DX_HOME, DX_TEMPLATE_PATH));

			/**
			 * Oracle Server Information
			 */
			LServer oracleServer = task.getTargetServers(0);
			String oracleUser = oracleServer.getUserName();
			String oralcePasswd = oracleServer.getPassword();
			String oracleHost = oracleServer.getHost();
			this.writeLocalLog(Level.INFO, String.format("[Oracle Server] connnectString=%s, user=%s, passwd=%s",
					oracleHost, oracleUser, DxDESCipher.EncryptDES(oralcePasswd)));

			/**
			 * process task_ext params
			 */
			String errorThreshold = this.getExtPropValueWithDefault("errorThreshold", "10000");
			String readConcurrency = this.getExtPropValueWithDefault("readConcurrency", "2");
			String writeConcurrency = this.getExtPropValueWithDefault("writeConcurrency", "8");
			String disableRecursive = this.getExtPropValueWithDefault("disable.recursive", "false");
			String validatorRegex = StringUtils.replace(this.getExtPropValueWithDefault("validator.regex", ""), "*",
					".+\\");
			String charsetEncoding = this.getExtPropValueWithDefault("charset.encoding", "GBK");
			String separator = this.getExtPropValueWithDefault("separator", "\011");
			String columnLimit = this.getExtPropValueWithDefault("column.limit", "");
			String ignoredColumns = this.getExtPropValueWithDefault("ignored.columns", "");
			String ignoreEmptyDatasource = this.getExtPropValueWithDefault("ignore.empty.datasource", "false");

			String connectionString = oracleHost;
			String user = oracleUser;
			String password = DxDESCipher.EncryptDES(oralcePasswd);

			/**
			 * params need to be standardized
			 */
			String truncateSql = CommonUtils.standardizeDateString(this.getExtPropValue("truncate.sql"),
					task.getCurRunDate());
			truncateSql = StringUtils.isBlank(truncateSql) ? "" : truncateSql;
			String targetTablename = CommonUtils.standardizeDateString(this.getExtPropValue("target.tablename"),
					task.getCurRunDate());
			String sourcePath = CommonUtils.standardizeDateString(this.getExtPropValue("source.path"),
					task.getCurRunDate());
			String targetPath = CommonUtils.standardizeDateString(this.getExtPropValue("target.path"),
					task.getCurRunDate());
			String partitionValue = CommonUtils.standardizeDateString(
					this.getExtPropValueWithDefault("partition.value", ""), task.getCurRunDate());

			/**
			 * If DX exit with : lastRuntimeId=4, Hour task, no truncate sql, this task instance will exit with code 4.
			 */
			String lastRuntimeId = StringUtils.trim(task.getRuntimeId());
			String cucleUnit = task.getCycleUnit();
			this.writeLocalLog(Level.INFO, String.format("Last RuntimeId=%s, cycle_unit=%s, truncate.sql=%s",
					lastRuntimeId, cucleUnit, truncateSql));
			if ("4".equals(lastRuntimeId) && "H".equals(cucleUnit) && StringUtils.isBlank(truncateSql)) {
				exitValue = 4;
				throw new Exception("DX swap partition error last time.");
			}

			/**
			 * Generate DX Control file
			 */
			StringBuffer sb = new StringBuffer();
			mergekeyValue(sb, "readConcurrency", readConcurrency);
			mergekeyValue(sb, "writeConcurrency", writeConcurrency);
			mergekeyValue(sb, "errorThreshold", errorThreshold);
			mergekeyValue(sb, "truncate.sql", truncateSql);
			mergekeyValue(sb, "target.tablename", targetTablename);
			mergekeyValue(sb, "source.path", sourcePath);
			mergekeyValue(sb, "disable.recursive", disableRecursive);
			mergekeyValue(sb, "validator.regex", validatorRegex);
			mergekeyValue(sb, "charset.encoding", charsetEncoding);
			mergekeyValue(sb, "separator", separator);
			mergekeyValue(sb, "target.path", targetPath);
			mergekeyValue(sb, "connection.string", connectionString);
			mergekeyValue(sb, "user", user);
			mergekeyValue(sb, "password", password);
			mergekeyValue(sb, "column.limit", columnLimit);
			mergekeyValue(sb, "ignored.columns", ignoredColumns);
			mergekeyValue(sb, "ignore.empty.datasource", ignoreEmptyDatasource);
			mergekeyValue(sb, "partition.value", partitionValue);

			/**
			 * create dir
			 */
			File f = new File(DX_TEMP_PATH + "/dxctl/" + task.getId() + '/');
			if (!f.exists())
				f.mkdirs();

			String dxCtrlFileName = task.getId() + "_"
					+ new SimpleDateFormat("yyyyMMddHHmmss").format(task.getCurRunDate()) + ".dxctl";
			String dxCtrlFileFullPath = f.getAbsolutePath() + "/" + dxCtrlFileName;
			dxCtrlFileFullPath = new File(dxCtrlFileFullPath).getAbsolutePath();

			CommonUtils.string2File(sb.toString(), dxCtrlFileFullPath, "UTF-8");
			this.writeLocalLog(Level.INFO, "DX ctl file generated: " + dxCtrlFileFullPath);

			/**
			 * Exec command
			 */
			String[] cmds = { DX_HOME + "/run.sh", DX_TEMPLATE_PATH, dxCtrlFileFullPath };
			this.writeLocalLog(Level.INFO, "DX Command:" + Arrays.deepToString(cmds));
			dxProcess = new RunTaskProcess(cmds, this);
			dxProcess.startProcess(false);

			dxProcess.waitAndDestroyProcess();
			exitValue = dxProcess.getExitVal();
			this.writeLocalLog(Level.INFO, "All done, exit value=" + exitValue);

			output = dxProcess.getNormalResult() + dxProcess.getErrResult();
			this.writeLocalLog(Level.INFO, "Output:" + output);
			commitTaskAndLog(LState.RUNNING, "FORMATTED_LOG", output);

			this.writeLocalLog(Level.INFO, "=====================\n");
		} catch (Exception e) {
			String st = CommonUtils.stackTraceToString(e);
			this.writeLocalLog(Level.SEVERE, "Exception stackTrace: " + st);
			commitTaskAndLog(LState.RUNNING, String.valueOf(exitValue), "Exception: " + e.getMessage());
			this.writeLocalLog(Level.INFO, "=====================\n");
			throw new IOException(e);
		} finally {
			JSONObject json = new JSONObject();
			putJson(json, "exit_code", String.valueOf(exitValue));
			if (exitValue != 0) {
				commitTaskAndLog(LState.FAILED, String.valueOf(exitValue), json.toString());
			} else {
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
		} catch (JSONException e) {
			this.writeLocalLog(Level.SEVERE, e.getMessage());
		}
	}

	@Override
	public void kill() throws IOException {
	}

	private void commitTaskAndLog(LState state, String runtimeId, String desc) {
		try {
			commitTask(state, runtimeId, desc);
		} catch (Exception e) {
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