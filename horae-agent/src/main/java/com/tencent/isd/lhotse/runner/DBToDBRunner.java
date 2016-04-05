package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.util.CommonUtils;
import com.tencent.isd.lhotse.runner.util.DBUtil;
import com.tencent.isd.lhotse.runner.util.DxDESCipher;
import com.tencent.isd.lhotse.runner.util.RunTaskProcess;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.logging.Level;

public class DBToDBRunner extends WrapperRunner {

	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(DBToDBRunner.class, (byte) 99);
	}

	@Override
	public void execute() throws IOException {
		RunTaskProcess dxProcess = null;
		String output = new String();
		int exitValue = -255;
		try {
			LTask task = getTask();
			this.writeLocalLog(Level.INFO, "=====================");
			String startLogString = "Start running DBToDBRunner:" + task.getId() + ", date:"
					+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(new Date(task.getCurRunDate()));
			this.writeLocalLog(Level.INFO, startLogString);

			this.writeLocalLog(Level.INFO, "begin to fetch parameters...");
			/**
			 * get global config
			 */
			Map<String, String> paramMap = this.requestGlobalParameters();
			final String DX_TEMP_PATH = paramMap.get("dx_temp_path");
			final String DX_HOME = paramMap.get("dx_home");
			final String DX_TEMPLATE_PATH = paramMap.get("dx_template_db_to_db");

			String globalConfString = String.format(
					"Empty value found: dx_temp_path=%s, dx_home=%s, dx_template_db_to_db=%s", DX_TEMP_PATH, DX_HOME,
					DX_TEMPLATE_PATH);
			if (CommonUtils.isBlankAny(DX_TEMP_PATH, DX_HOME, DX_TEMPLATE_PATH)) {
				throw new IOException(globalConfString);
			}
			this.writeLocalLog(Level.INFO, globalConfString);

			/**
			 * HDFS Server Information
			 */
			LServer sourceServer = task.getSourceServers(0);
			String sourceHost = sourceServer.getHost();
			int sourcePort = sourceServer.getPort();
			String sourceServerType = sourceServer.getType();
			String sourceService = sourceServer.getService();
			String sourceUser = sourceServer.getUserName();
			String sourcePassword = DxDESCipher.EncryptDES(sourceServer.getPassword());

			this.writeLocalLog(Level.INFO, String.format(
					"[Source Server] host=%s, port=%s, serverType=%s, service=%s, user=%s, pwd=%s", sourceHost,
					sourcePort, sourceServerType, sourceService, sourceUser, sourcePassword));

			/**
			 * Oracle Server Information
			 */
			LServer targetServer = task.getTargetServers(0);
			String targetHost = targetServer.getHost();
			int targetPort = targetServer.getPort();
			String targetServerType = targetServer.getType();
			String targetService = targetServer.getService();
			String targetUser = targetServer.getUserName();
			String targetPassword = DxDESCipher.EncryptDES(targetServer.getPassword());

			this.writeLocalLog(Level.INFO, String.format(
					"[Target Server] host=%s, port=%s, serverType=%s, service=%s, user=%s, pwd=%s", targetHost,
					targetPort, targetServerType, targetService, targetUser, targetPassword));

			String sourceConnectionString = null;
			String targetConnectionString = null;
			try {
				sourceConnectionString = DBUtil.getConnectionString(sourceServerType, sourceHost, sourcePort,
						sourceService);
				this.writeLocalLog(Level.INFO, "Source Connection String=" + sourceConnectionString);
				targetConnectionString = DBUtil.getConnectionString(targetServerType, sourceHost, sourcePort,
						sourceService);
				this.writeLocalLog(Level.INFO, "Target Connection String=" + targetConnectionString);
			}
			catch (SQLException e) {
				throw new SQLException("Task failed becuase server config is not correct.");
			}

			/**
			 * process task_ext params
			 */
			String errorThreshold = this.getExtPropValueWithDefault("errorThreshold", "10000");
			String readConcurrency = this.getExtPropValueWithDefault("readConcurrency", "2");
			String writeConcurrency = this.getExtPropValueWithDefault("writeConcurrency", "2");
			String ignoreEmptyDatasource = this.getExtPropValueWithDefault("ignore.empty.datasource", "false");
			String ignoredColumnsTrimLinefeed = this.getExtPropValueWithDefault("ignored.columns.trim.linefeed", "");

			/**
			 * params need to be standardized
			 */
			String truncateSql = CommonUtils.standardizeDateString(this.getExtPropValue("truncate.sql"),
					task.getCurRunDate());
			truncateSql = StringUtils.isBlank(truncateSql) ? "" : truncateSql;
			String sourcePath = CommonUtils.standardizeDateString(this.getExtPropValue("source.path"),
					task.getCurRunDate());
			String targetPath = CommonUtils.standardizeDateString(this.getExtPropValue("target.path"),
					task.getCurRunDate());
			String ignoreErrorsSql = CommonUtils.standardizeDateString(
					this.getExtPropValueWithDefault("ignore.errors.sql", ""), task.getCurRunDate());

			/**
			 * Generate DX Control file
			 */
			StringBuffer sb = new StringBuffer();
			mergekeyValue(sb, "readConcurrency", readConcurrency);
			mergekeyValue(sb, "writeConcurrency", writeConcurrency);
			mergekeyValue(sb, "errorThreshold", errorThreshold);
			mergekeyValue(sb, "truncate.sql", truncateSql);
			mergekeyValue(sb, "ignore.errors.sql", ignoreErrorsSql);

			mergekeyValue(sb, "source.path", sourcePath);
			mergekeyValue(sb, "source.driver.string", DBUtil.getDriverString(sourceServerType));
			mergekeyValue(sb, "source.connection.string", sourceConnectionString);
			mergekeyValue(sb, "source.user", sourceUser);
			mergekeyValue(sb, "source.password", sourcePassword);
			mergekeyValue(sb, "ignored.columns.trim.linefeed", ignoredColumnsTrimLinefeed);

			mergekeyValue(sb, "target.path", targetPath);
			mergekeyValue(sb, "target.driver.string", DBUtil.getDriverString(targetServerType));
			mergekeyValue(sb, "target.connection.string", targetConnectionString);
			mergekeyValue(sb, "target.user", targetUser);
			mergekeyValue(sb, "target.password", targetPassword);

			mergekeyValue(sb, "ignore.empty.datasource", ignoreEmptyDatasource);

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
		}
		catch (Exception e) {
			String st = CommonUtils.stackTraceToString(e);
			this.writeLocalLog(Level.SEVERE, "Failed with exception, StackTrace: " + st);
			commitTaskAndLog(LState.FAILED, String.valueOf(exitValue),
					"HdfsToOracle failed with exception, " + e.getMessage());
			this.writeLocalLog(Level.SEVERE, "=====================\n");
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

	@Override
	public void kill() throws IOException {
	}

}