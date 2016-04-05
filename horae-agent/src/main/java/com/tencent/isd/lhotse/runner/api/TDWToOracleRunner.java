package com.tencent.isd.lhotse.runner.api;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.api.util.CommonUtils;
import com.tencent.isd.lhotse.runner.api.util.DxDESCipher;
import com.tencent.isd.lhotse.runner.api.util.RunTaskProcess;
import com.tencent.isd.lhotse.runner.api.util.RunnerUtils;
import com.tencent.isd.lhotse.runner.api.util.RunnerUtils.SessionAndExitVal;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.logging.Level;

import static com.tencent.isd.lhotse.runner.api.util.CommonUtils.mergekeyValue;

public class TDWToOracleRunner extends TDWDDCTaskRunner {
	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(TDWToOracleRunner.class, (byte) 87);
	}

	@Override
	public void execute() 
		throws IOException {

		LTask task = getTask();

		/* Check the server tags configuration for this task. */
		if (task.getSourceServersCount() != 3) {
			String failMessage = 
				"Task server configuration is incorrect, expect 3 source " +
				"HDFS servers configured, but " + task.getSourceServersCount() +
				" server configured";						
			writeLogAndCommitTask(failMessage, Level.SEVERE, LState.FAILED);
			return;
		}
		
		if (task.getTargetServersCount() != 1) {
			String failMessage = 
				"Task server configuration is incorrect, expect 1 target " +
				"servers configured, but " + task.getTargetServersCount() +
				" server configured";						
			writeLogAndCommitTask(failMessage, Level.SEVERE, LState.FAILED);
			return;
		}
		
		/* Calculate the real data date, for tasks transferred from USP. */
		Date runDate = getRealDataDate(task);
		
		/* 
		 * Check whether we need to skip the weekly task for those USP 
		 * weekly and monthly tasks. 
		 */
		if (skipWeeklyTaskExecution(task, runDate)) {
			return;
		}

		/* Get the hive server configuration. */
		LServer hiveServer = task.getSourceServers(0);
		String hiveUser = hiveServer.getUserName();
		String hivePassword = hiveServer.getPassword();
		String hiveIP = hiveServer.getHost();
		String hivePort = String.valueOf(hiveServer.getPort());
		writeLocalLog(Level.INFO, String.format("[EXT PARAMS] hiveUser=%s, hivePassword=%s, hiveIP=%s, hivePort=%s",
				      hiveUser, hivePassword, hiveIP, hivePort));

		/* Get the name node configuration for this TDW. */
		LServer nameNodeServer = task.getSourceServers(1);
		String nameNodeUser = nameNodeServer.getUserName();
		String nameNodeGroup = nameNodeServer.getUserGroup();
		String nameNodeIP = nameNodeServer.getHost();
		String nameNodePort = String.valueOf(nameNodeServer.getPort());
		writeLocalLog(Level.INFO, String.format("[EXT PARAMS] nameNodeUser=%s, nameNodeGroup=%s, nameNodeIP=%s, nameNodePort=%s", 
				      nameNodeUser,	nameNodeGroup, nameNodeIP, nameNodePort));

		/* Get the target HDFS cluster configuration. */
		LServer oracleServer = task.getTargetServers(0);
		String oracleUser = oracleServer.getUserName(); // denny_test
		String oralcePasswd = oracleServer.getPassword(); // Deny123#
		String oracleHost = oracleServer.getHost();// jdbc:oracle:oci:@DWDBETL
		this.writeLocalLog(Level.INFO, String.format("[Oracle Server] connnectString=%s, user=%s, passwd=%s",
				           oracleHost, oracleUser, oralcePasswd));

		/* Get the task configuration. */
		String tdwDbName = this.getExtPropValue("tdwDbName");
		String oracleTableName = this.getExtPropValue("oracleTableName");
		
		if (!oracleTableName.contains(".")) {
			String failMessage = 
				"Oracle target table name should contain the schema name";
			writeLogAndCommitTask(failMessage, Level.SEVERE, LState.FAILED);
			return;		
		}
		
		String oracleColumnNames = this.getExtPropValue("oracleColumnNames");
		String loadMode = this.getExtPropValue("loadMode");
		String filterSQL = 
			RunnerUtils.replaceDateExpr(this.getExtPropValue("filterSQL"), runDate);
		String partitionValue = (this.getExtPropValue("partitionValue") == null ? "" :
			RunnerUtils.replaceDateExpr(this.getExtPropValue("partitionValue"), runDate));
		String errorPercentage = this.getExtPropValue("errorPercentage");
		int columnLimit = new StringTokenizer(oracleColumnNames, ",").countTokens();
		this.writeLocalLog(Level.INFO, "columnLimit: " + columnLimit);
		String errorThreshold = this.getExtPropValueWithDefault("errorThreshold", "50000");
		
		/* Calculate the date formatter for etl_time. */
		SimpleDateFormat dateFormat = getDateFormat(this.getExtPropValue("offRange"),
				                                    this.getExtPropValue("offRangeType"),
				                                    task.getCycleUnit());
		if (dateFormat == null) {
			String failMessage = "Can't find the appropriate date formatter";
			this.writeLocalLog(Level.SEVERE, failMessage);
			commitTask(LState.FAILED, "", failMessage);
			return;
		}
		
		/* Calculate the etl_time. */
		String etlTime = task.getId() + "_" + dateFormat.format(runDate);
		if (this.getExtPropValue("ddcId") != null) {
			etlTime = this.getExtPropValue("ddcId") + "_" + dateFormat.format(runDate);
		}
		
		/* Composite the truncate SQL. */
		String truncateSQL = null;
		if (APPEND_FLAG.equalsIgnoreCase(loadMode)) {
			truncateSQL = "DELETE FROM " + oracleTableName + 
					      " WHERE ETL_STAMP='" + etlTime + "'";
		} else if (TRUNCATE_FLAG.equalsIgnoreCase(loadMode)) {
			if ((partitionValue == null) || (partitionValue.length() < 2)) {
				truncateSQL = "TRUNCATE TABLE " + oracleTableName;
			} else {
			    truncateSQL = "ALTER TABLE " + oracleTableName + 
				    	      " DROP PARTITION " + partitionValue;
			}
		}
		
		/* Composite the Oracle DX SQL. */		
		String oracleSQL = this.getExtPropValue("oracleSQL");
		if (oracleSQL == null) {
			
			/* 
			 * If the oracleSQL property is null, which means is a TDWToOracle 
			 * task configured on Lhotse, we need to generate the SQL based on 
			 * the column names. 
			 */
			StringBuilder sb1 = new StringBuilder();
			StringBuilder sb2 = new StringBuilder();
			StringTokenizer st = new StringTokenizer(oracleColumnNames, ",");
			while (st.hasMoreTokens()) {
				String tokenValue = st.nextToken().trim();
				
				StringTokenizer newSt = new StringTokenizer(tokenValue, " ");
				int countTokens = newSt.countTokens();
				if (countTokens == 1) {
					sb1.append(tokenValue).append(",");
					sb2.append("?,");
				} else {
					sb1.append(newSt.nextToken()).append(",");
					String oracleDateFormat = "";
					while (newSt.hasMoreTokens()) {
						String newTokenValue = newSt.nextToken();
						if (!"date".equals(newTokenValue)) {
							newTokenValue = newTokenValue.replace("\"", "");
							newTokenValue = newTokenValue.replace("'", "");
						    oracleDateFormat = oracleDateFormat + newTokenValue + " ";
						}
					}
					sb2.append("TO_DATE(?, '" + oracleDateFormat.trim().toUpperCase() + "'),");
				}
			}
			
			String columnNames = sb1.toString().substring(0, sb1.toString().length() - 1);
			String columnValues = sb2.toString().substring(0, sb2.toString().length() - 1);
			
			if (APPEND_FLAG.equalsIgnoreCase(loadMode)) {
				columnNames = "etl_stamp," + columnNames;
				columnValues = "'" + etlTime + ",'" + columnValues;
			}
			
			oracleSQL = "INSERT INTO @{DX_TEMP_TABLENAME} (" + columnNames + ") VALUES (" + columnValues + ")";
		} else {
			if (APPEND_FLAG.equalsIgnoreCase(loadMode)) {
				this.writeLocalLog(Level.INFO, "In replace etl_time codes");
				this.writeLocalLog(Level.INFO, "oracleSQL contains (?: " + oracleSQL.contains("(?"));
				oracleSQL = oracleSQL.replace("(?", "('" + etlTime + "'");
			}
		}
		
		this.writeLocalLog(Level.INFO, "oracleSQL: " + oracleSQL);
		
		/* Set the parameters to a fixed value. */
		String readConcurrency = "2";
		String writeConcurrency = "4";
		String disableRecursive = "true";
		String charsetEncoding = "UTF-8";
		boolean ignoreEmptyDatasource = 
			new Boolean(this.getExtPropValue("ignoreEmptyDataSource"));
		String ignoredColumns = "";
		String validatorRegex = "";
		String validatePartitionKey = "false";

		/* Get the global parameter. */
		final String plcProgram = this.requestGlobalParameters().get(PLC_PROGRAM);
		final String tmpPathPrefix = this.requestGlobalParameters().get(NAME_NODE_TMP_PATH);	
		final String externalTableDb = this.requestGlobalParameters().get(EXTERNAL_TABLE_CREATE_DB);
		final String plcParameter = getPLCParameter(task.getId(), runDate);
		final String DX_TEMP_PATH = this.requestGlobalParameters().get("dx_temp_path");
		final String DX_HOME = this.requestGlobalParameters().get("dx_home");
		final String DX_TEMP_SCHEMA = this.requestGlobalParameters().get("dx_temp_schema");
		final String DX_TEMPLATE_PATH = this.requestGlobalParameters().get("dx_template_hdfs_to_oracle");
		if (CommonUtils.isBlankAny(DX_TEMP_PATH, DX_HOME, DX_TEMP_SCHEMA, DX_TEMPLATE_PATH)) {
			throw new IOException(String.format("Empty value found: dx_temp_path=%s, " +
		                          "dx_home=%s, dx_temp_schema=%s,dx_template_hdfs_to_oracle=%s",
							      DX_TEMP_PATH, DX_HOME, DX_TEMP_SCHEMA, DX_TEMPLATE_PATH));
		}

		/* External table and its delimiter. */
		String externalTableName = "ext_" + task.getId() + System.currentTimeMillis();
		String externalTableDataPath = tmpPathPrefix + File.separator + externalTableName;
		String destFileDelimiter = "\\001";

		FileSystem nameNodeFS = null;
		try {
			/* Check to see whether the database exists or not. */
			if (!checkDbExistence(plcProgram, hiveUser, hivePassword, hiveIP, 
					              hivePort, tdwDbName)) {
				return;
			}

			/* Create the external table data path. */
			nameNodeFS = RunnerUtils.getHdfsFileSystem(nameNodeServer);
			if (!RunnerUtils.createTempHDFSPath(nameNodeFS, externalTableDataPath)) {
				return;
			}
			
			/* Create external table using the filter SQL. */
			if (!createExternalTableWithSQL(plcProgram, plcParameter, hiveUser, hivePassword, hiveIP, 
					                        hivePort, nameNodeIP, nameNodePort, externalTableDataPath, 
					                        tdwDbName, externalTableDb, externalTableName, 
					                        filterSQL, destFileDelimiter)) {
				return;
			}
			
			/* Load data to the external table, whose data directory on the name node. */
			SessionAndExitVal loadDataResult = 
				loadDataWithSQL(plcProgram, plcParameter, hiveUser, hivePassword, hiveIP, hivePort,
				                tdwDbName, externalTableDb, externalTableName, filterSQL);
			if (loadDataResult.getExitVal() != 0) {				
				return;
			}

			/* Now we need to export the data to Oracle. */
			if (!nameNodeIP.toLowerCase().startsWith("hdfs://")) {
				nameNodeIP = "hdfs://" + nameNodeIP;
			}
			
			String fsDefaultName = nameNodeIP + ":" + nameNodePort;
			String hadoopJobUgi = nameNodeUser + "," + nameNodeGroup;
			String connectionString = oracleHost;
			String user = oracleUser;
			String password = DxDESCipher.EncryptDES(oralcePasswd);
			
			StringBuffer sb = new StringBuffer();
			mergekeyValue(sb, "readConcurrency", readConcurrency);
			mergekeyValue(sb, "writeConcurrency", writeConcurrency);
			mergekeyValue(sb, "errorThreshold", errorThreshold);
			mergekeyValue(sb, "truncate.sql", truncateSQL);
			mergekeyValue(sb, "temp.schema", DX_TEMP_SCHEMA);
			mergekeyValue(sb, "target.tablename", oracleTableName);
			mergekeyValue(sb, "source.path", externalTableDataPath);
			mergekeyValue(sb, "disable.recursive", disableRecursive);
			mergekeyValue(sb, "validator.regex", validatorRegex);
			mergekeyValue(sb, "fs.default.name", fsDefaultName);
			mergekeyValue(sb, "hadoop.job.ugi", hadoopJobUgi);
			mergekeyValue(sb, "charset.encoding", charsetEncoding);
			mergekeyValue(sb, "separator", destFileDelimiter);
			mergekeyValue(sb, "target.path", oracleSQL);
			mergekeyValue(sb, "connection.string", connectionString);
			mergekeyValue(sb, "user", user);
			mergekeyValue(sb, "password", password);
			mergekeyValue(sb, "column.limit", columnLimit);
			mergekeyValue(sb, "ignored.columns", ignoredColumns);
			mergekeyValue(sb, "ignore.empty.datasource", ignoreEmptyDatasource);
			mergekeyValue(sb, "validate.partition.key", validatePartitionKey);
			mergekeyValue(sb, "partition.value", partitionValue);

			/* Create the DX control file directory. */
			File f = new File(DX_TEMP_PATH + "/dxctl/" + task.getId() + '/');
			if (!f.exists()) {
				f.mkdirs();
			}

			String dxCtrlFileName = task.getId() + "_"
					+ new SimpleDateFormat("yyyyMMddHHmmssS").format(task.getCurRunDate()) + ".dxctl";
			String dxCtrlFileFullPath = f.getAbsolutePath() + "/" + dxCtrlFileName;
			dxCtrlFileFullPath = new File(dxCtrlFileFullPath).getAbsolutePath();

			CommonUtils.string2File(sb.toString(), dxCtrlFileFullPath, "UTF-8");
			this.writeLocalLog(Level.INFO, "DX ctrl file generated: " + dxCtrlFileFullPath);

			/**
			 * Exec command
			 */
			String[] cmds = { DX_HOME + "/run.sh", DX_TEMPLATE_PATH, dxCtrlFileFullPath };
			this.writeLocalLog(Level.INFO, "DX Command:" + Arrays.deepToString(cmds));
			RunTaskProcess dxProcess = new RunTaskProcess(cmds, this);
			dxProcess.startProcess(false);

			String dxPid = String.valueOf(dxProcess.getProcessId());
			this.writeLocalLog(Level.INFO, "DX process is running, pid=" + dxPid);
			commitTaskAndLog(LState.RUNNING, dxPid, "DX process is running, pid=" + dxPid);

			dxProcess.waitAndDestroyProcess();
			int exitValue = dxProcess.getExitVal();
			this.writeLocalLog(Level.INFO, "All done, exit value=" + exitValue);
			
			if (exitValue != 0) {
				writeLocalLog(Level.SEVERE, "Load data to Oracle using DX failed");
				commitTask(LState.FAILED, "", "Load data to Oracle using DX failed");
				return;
			}

			String output = new String();
			output = dxProcess.getNormalResult() + dxProcess.getErrResult();
			this.writeLocalLog(Level.INFO, "Output:" + output);
			commitTaskAndLog(LState.RUNNING, "FORMATTED_LOG", output);
			this.writeLocalLog(Level.INFO, "=====================\n");
			
			JSONObject jsonObject = new JSONObject(output);
			int failRead = jsonObject.getInt("failed_read");
			int successRead = jsonObject.getInt("success_read");
			int successWrite = jsonObject.getInt("success_writed");
			
			float failWrite = (failRead + successRead - successWrite) / ((float) (failRead + successRead));
			failWrite = failWrite * 100;
			
			/* 
			 * The whole process succeeds if the fail write percentage is below
			 * the error percentage, otherwise we think it fails. 
			 */
			if (failWrite > new Float(errorPercentage)) {
				String failMessage = 
					"The failure percentage is beyond: " + errorPercentage + ", the task fails";
				writeLogAndCommitTask(failMessage, Level.SEVERE, LState.FAILED);
				return;
			}

			writeLogAndCommitTask("Load data from TDW to Oracle succeeds", 
					              Level.INFO, LState.SUCCESSFUL);
		} catch (Exception e) {
			writeLocalLog(Level.SEVERE, e.getMessage());
			commitTask(LState.FAILED, "", "TDWExport failed.");
			e.printStackTrace();
			throw new IOException(e);
		} finally {
			doCleanWork(plcProgram, hiveUser, hivePassword, hiveIP, hivePort, externalTableDb,
						externalTableName, nameNodeFS, externalTableDataPath);
		}
	}
	
	@Override
	public void kill() throws IOException {
	}
	
	private String getExtPropValueWithDefault(String key, String defaultValue) {
		String value = this.getExtPropValue(key);
		if (StringUtils.isBlank(value))
			value = defaultValue;
		return value;
	}
	
	private void commitTaskAndLog(LState state, String runtimeId, String desc) {
		try {
			commitTask(state, runtimeId, desc);
		} catch (Exception e) {
			String st = CommonUtils.stackTraceToString(e);
			this.writeLocalLog(Level.INFO, "log_desc :" + desc);
			this.writeLocalLog(Level.INFO, "log_desc length:" + desc.length());
			this.writeLocalLog(Level.SEVERE, "commit task failed, StackTrace: " + st);
		}
	}
}
