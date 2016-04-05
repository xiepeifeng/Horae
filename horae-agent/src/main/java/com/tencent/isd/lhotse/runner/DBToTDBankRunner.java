package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.util.*;
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

public class DBToTDBankRunner extends WrapperRunner {

	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(DBToTDBankRunner.class, (byte) 125);
	}

	@Override
	public void execute() throws IOException {
		RunTaskProcess dxProcess = null;
		String output = new String();
		int exitValue = -255;

		try {
			/* Get the basic information */
			LTask task = getTask();

			Date dataDate = new Date(task.getCurRunDate());
			String dataDateFullStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dataDate);

			this.writeLocalLog(Level.INFO, "=====================");
			String startLogString = "Start running DBToTDBank Runner:" + task.getId() + ", date:" + dataDateFullStr;
			this.writeLocalLog(Level.INFO, startLogString);

			/* Get the global configuration from base */
			this.writeLocalLog(Level.INFO, "begin to fetch parameters...");
			Map<String, String> paramMap = this.requestGlobalParameters();
			final String DX_TEMP_PATH = FileUtils.replaceHomePath(paramMap.get("dx_temp_path"));
			final String DX_HOME = FileUtils.replaceHomePath(paramMap.get("dx_home"));
			final String DX_TEMPLATE_PATH = FileUtils.replaceHomePath(paramMap.get("dx_template_db_to_tdbank"));

			String paramLogStr = String.format(
					"Global Parameters: dx_temp_path=%s, dx_home=%s, dx_template_db_to_tdbank=%s", DX_TEMP_PATH,
					DX_HOME, DX_TEMPLATE_PATH);

			if (CommonUtils.isBlankAny(DX_TEMP_PATH, DX_HOME, DX_TEMPLATE_PATH)) {
				throw new IOException("Empty value found: " + paramLogStr);
			}
			this.writeLocalLog(Level.INFO, paramLogStr);

			if (!new File(DX_TEMPLATE_PATH).exists()) {
				throw new IOException(DX_TEMPLATE_PATH + " is not exist.");
			}

			/* Get the source server information */
			LServer dbServer = task.getSourceServers(0);
			String dbHost = dbServer.getHost();
			int dbPort = dbServer.getPort();
			String dbServerType = dbServer.getType();
			String dbService = this.getExtPropValue("database.name");
			String dbUser = dbServer.getUserName();
			String dbPassword = DxDESCipher.EncryptDES(dbServer.getPassword());

			this.writeLocalLog(Level.INFO, String.format(
					"[Database Server] host=%s, port=%s, serverType=%s, schema=%s, user=%s, pwd=%s", dbHost, dbPort,
					dbServerType, dbService, dbUser, dbPassword));
			String connectionString = null;
			String driverString = null;
			try {
				connectionString = DBUtil.getConnectionString(dbServerType, dbHost, dbPort, dbService);
				this.writeLocalLog(Level.INFO, "Database Connection String=" + connectionString);
				driverString = DBUtil.getDriverString(dbServerType);
				this.writeLocalLog(Level.INFO, "Database Driver String=" + driverString);
			}
			catch (SQLException e) {
				throw new SQLException("Task failed becuase server config is not correct.");
			}

			/* Get the target server information */
			LServer tdBankServer = task.getTargetServers(0);
			String tdBankConf = FileUtils.replaceHomePath(tdBankServer.getHost());
			String topic = tdBankServer.getService();
			this.writeLocalLog(Level.INFO, "tdBankConf=" + tdBankConf + ", topic=" + topic);

			/* Get the extension parameters */
			String headerMagic = this.getExtPropValueWithDefault("tdbank.header.magic", "0");
			String iname = this.getExtPropValue("tdbank.header.iname");

			String headerDate = null;
			if ("H".equalsIgnoreCase(task.getCycleUnit())) {
				headerDate = new SimpleDateFormat("yyyyMMddHH").format(dataDate);
			}
			else {
				headerDate = new SimpleDateFormat("yyyyMMdd").format(dataDate);
			}

			StringBuffer header = new StringBuffer();
			header.append("m=").append(headerMagic).append("&");
			header.append("iname=").append(iname).append("&");
			header.append("t=").append(headerDate);
			this.writeLocalLog(Level.INFO, "tdbank.header=" + header.toString());

			String errorThreshold = this.getExtPropValueWithDefault("errorThreshold", "10000");
			String readConcurrency = this.getExtPropValueWithDefault("readConcurrency", "1");
			String writeConcurrency = this.getExtPropValueWithDefault("writeConcurrency", "5");
			String ignoreEmptyDatasource = this.getExtPropValueWithDefault("ignore.empty.datasource", "false");
			String ignoredColumnsTrimLinefeed = this.getExtPropValueWithDefault("ignored.columns.trim.linefeed", "");

			String sql = this.getExtPropValue("sql");
			sql = RunnerUtils.formatStrDateParam(sql, dataDate);
			if (CommonUtils.isBlankAny(sql)) {
				throw new Exception("Empty value found for sql/tdbank.topic");
			}

			sql = StringUtils.replace(sql, "\r", "");
			sql = StringUtils.replace(sql, "\n", "");

			/* Generate DX Control file */
			StringBuffer sb = new StringBuffer();
			mergekeyValue(sb, "readConcurrency", readConcurrency);
			mergekeyValue(sb, "writeConcurrency", writeConcurrency);
			mergekeyValue(sb, "errorThreshold", errorThreshold);
			mergekeyValue(sb, "sql", sql);
			mergekeyValue(sb, "driver.string", driverString);
			mergekeyValue(sb, "connection.string", connectionString);
			mergekeyValue(sb, "user", dbUser);
			mergekeyValue(sb, "password", dbPassword);
			mergekeyValue(sb, "ignored.columns.trim.linefeed", ignoredColumnsTrimLinefeed);
			mergekeyValue(sb, "tdbank.conf", tdBankConf);
			mergekeyValue(sb, "tdbank.header", header);
			mergekeyValue(sb, "tdbank.topic", topic);
			mergekeyValue(sb, "ignore.empty.datasource", ignoreEmptyDatasource);

			/* Create DX control file */
			File f = new File(DX_TEMP_PATH + "/dxctl/" + task.getId() + '/');
			if (!f.exists())
				f.mkdirs();

			String dxCtrlFileName = task.getId() + "_"
					+ new SimpleDateFormat("yyyyMMddHHmmss").format(task.getCurRunDate()) + ".dxctl";
			String dxCtrlFileFullPath = f.getAbsolutePath() + "/" + dxCtrlFileName;
			dxCtrlFileFullPath = new File(dxCtrlFileFullPath).getAbsolutePath();

			CommonUtils.string2File(sb.toString(), dxCtrlFileFullPath, "UTF-8");
			this.writeLocalLog(Level.INFO, "DX ctl file generated: " + dxCtrlFileFullPath);

			/* Invoke DX and wait for the result */
			String[] cmds = { DX_HOME + "/run.sh", DX_TEMPLATE_PATH, dxCtrlFileFullPath };
			this.writeLocalLog(Level.INFO, "DX Command:" + Arrays.deepToString(cmds));
			dxProcess = new RunTaskProcess(cmds, this);
			dxProcess.startProcess(false);
			dxProcess.waitAndDestroyProcess();
			exitValue = dxProcess.getExitVal();
			this.writeLocalLog(Level.INFO, "All done, exit value=" + exitValue);

			output = dxProcess.getNormalResult();
			this.writeLocalLog(Level.INFO, "Standard output:" + output);
			this.writeLocalLog(Level.INFO, "Error output:" + dxProcess.getErrResult());
			commitTaskAndLog(LState.RUNNING, "FORMATTED_LOG", output);

			this.writeLocalLog(Level.INFO, "=====================\n");
		}
		catch (Exception e) {
			String st = CommonUtils.stackTraceToString(e);
			this.writeLocalLog(Level.SEVERE, "Failed with exception, StackTrace: " + st);
			commitTaskAndLog(LState.FAILED, String.valueOf(exitValue),
					"DBToTDBank failed with exception, " + e.getMessage());
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
