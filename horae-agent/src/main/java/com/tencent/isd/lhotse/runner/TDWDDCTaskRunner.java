package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.AbstractTaskRunner;
import com.tencent.isd.lhotse.runner.util.CheckProcess;
import com.tencent.isd.lhotse.runner.util.RunTaskProcess;
import com.tencent.isd.lhotse.runner.util.RunnerUtils;
import com.tencent.isd.lhotse.runner.util.RunnerUtils.*;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.logging.Level;

public abstract class TDWDDCTaskRunner extends DDCTaskRunner {

	protected final String PLC_PROGRAM = "plcProgram";
	protected final String NAME_NODE_TMP_PATH = "nameNodeTmpPath";
	protected final String EXTERNAL_TABLE_CREATE_DB = "externalTableDb";
	protected final String TRUNCATE_FLAG = "truncate";
	protected final String APPEND_FLAG = "append";
	protected final String HA_VERSION_FLAG = "HAFlag";
	protected final String CGI_HOST = "cgiHost";
	protected final String CGI_URL = "cgiURL";
	protected final String REMOVE_DATA_FILE_RETRY = "removeDataFileRetry";
	protected final String CHECK_FILE_TMP_DIR = "checkFileTmpDir";
	protected final String BI_CONN_STRING = "biConnString";
	protected final String BI_CONN_USER = "biConnUser";
	protected final String BI_CONN_PWD = "biConnPwd";
	protected boolean externalTableCreated = false;
	protected boolean tmpPathCreated = false;
	protected Date runDate;
	protected String plcParameter;
	// --
	protected final String BASE_PATH = "basePath";

	protected Connection getBIConnection() 
	    throws Exception {
		
		final String biConnString = this.requestGlobalParameters().get(BI_CONN_STRING);
		final String biConnUser = this.requestGlobalParameters().get(BI_CONN_USER);
		final String biConnPwd = this.requestGlobalParameters().get(BI_CONN_PWD);
		Class.forName("org.postgresql.Driver").newInstance();
		Connection conn = 
			DriverManager.getConnection(biConnString, biConnUser, biConnPwd);
        conn.setClientInfo("ApplicationName", "lhotse");
        
        return conn;
	}
	
	/* Check the table existence. */
	protected boolean checkDbWithPG(Connection conn, String databaseName)
		throws Exception {

		PreparedStatement ps = null;
		ResultSet res = null;
		boolean dbExists = false;
		try {
		    String dbSQL = "SELECT tdw.is_db_exists('" + databaseName + "')";
		    ps = conn.prepareStatement(dbSQL);
		    res = ps.executeQuery();
		
			while (res.next()) {
			    dbExists = res.getBoolean(1);
		    }
		} finally {
			if (res != null) {
				res.close();
			}
			
			if (ps != null) {
				ps.close();
			}
		}
		
		writeLocalLog(Level.INFO, "The specified database: " + databaseName + " is " +
		                          (dbExists ? "found" : "not found"));
		
		if (!dbExists) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", RunnerUtils.TDW_NO_DB_ERROR_CODE);
			keyValues.put("task_desc", "Can't find database: " + databaseName);
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
		}

		return dbExists;
	}

	/* Check the table existence. */
	protected boolean checkTableWithPG(Connection conn, 
			                           String databaseName, 
			                           String tableName) 
	    throws Exception {

		PreparedStatement ps = null;
		ResultSet res = null;
		boolean tableExists = false;
		try {
			String tableSQL = 
				"SELECT tdw.is_table_exists('" + databaseName + "', '" + tableName + "')";
		    ps = conn.prepareStatement(tableSQL);
		    res = ps.executeQuery();
		
			while (res.next()) {
			    tableExists = res.getBoolean(1);
		    }
		} finally {
			if (res != null) {
				res.close();
			}
			
			if (ps != null) {
				ps.close();
			}
		}
		
		writeLocalLog(Level.INFO, "The specified table: " + tableName + " in specified database: " +
				                  databaseName + " is " + (tableExists? "found" : "not found"));
		
		if (!tableExists) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", RunnerUtils.TDW_NO_TABLE_ERROR_CODE);
			keyValues.put("task_desc", "Can't find table: " + tableName + " in database: " + databaseName);
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
		}

		return tableExists;
	}

	/*
	 * Check the partition existence and create the partition if it doesn't
	 * exist.
	 */
	protected boolean createPartitionWithPG(String plcProgram,
			                                Connection conn,
			                                LServer hiveServer, 
			                                String databaseName,
			                                String tableName, 
			                                String partitionName) 
	    throws Exception {

		if (partitionName != null && partitionName.length() > 2) {
			PreparedStatement ps = null;
			ResultSet res = null;
			boolean partitionExists = false;
			try {
				String partitionSQL = 
					"SELECT tdw.is_partition_exists('" + databaseName + "', '" + tableName + "', '" +
				    partitionName + "')";
			    ps = conn.prepareStatement(partitionSQL);
			    res = ps.executeQuery();
			
				while (res.next()) {
				    partitionExists = res.getBoolean(1);
			    }
			} finally {
				if (res != null) {
					res.close();
				}
				
				if (ps != null) {
					ps.close();
				}
			}
			
			if (!partitionExists) {
				PLCResult result = 
					RunnerUtils.createPartition(plcProgram, hiveServer,
						                        databaseName, tableName, 
						                        partitionName, this, plcParameter);
				
				if (result.getExitVal() != 0) {
					Map<String, String> keyValues = new HashMap<String, String>();
					keyValues.put("exit_code", RunnerUtils.TDW_NO_PARTITION_ERROR_CODE);
					keyValues.put("task_desc", result.getLastLine());
					keyValues.put("run_date", HOUR_FORMAT.format(runDate));
					commitJsonResult(keyValues, false, "");
					return false;
				}

				writeLocalLog(Level.INFO, "Create partition: " + partitionName + " for table: "
						+ tableName + " succeed");
			}
		}

		writeLocalLog(Level.INFO, "Partition: " + partitionName + " exists or just been created");

		return true;
	}

	/* Check the table existence. */
	protected boolean checkDbExistence(String plcProgram, LServer hiveServer, String databaseName)
			throws Exception {

		FindOutputResult checkResult = RunnerUtils.checkDatabases(plcProgram, hiveServer,
				databaseName, this,plcParameter);

		if (checkResult.getExitVal() != 0) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", checkResult.getPLCReturnCode());
			keyValues.put("task_desc", checkResult.getLastLine());
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return false;
		}

		if (!checkResult.isValueFound()) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", RunnerUtils.TDW_NO_DB_ERROR_CODE);
			keyValues.put("task_desc", "Can't find the specified database: " + databaseName);
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return false;
		}

		writeLocalLog(Level.INFO, "The specified database: " + databaseName + " is found");

		return true;
	}

	/* Check the table existence. */
	protected boolean checkTableExistence(String plcProgram, LServer hiveServer,
			String databaseName, String tableName) throws Exception {

		FindOutputResult checkResult = RunnerUtils.checkTables(plcProgram, hiveServer,
				databaseName, tableName, this,plcParameter);

		if (checkResult.getExitVal() != 0) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", checkResult.getPLCReturnCode());
			keyValues.put("task_desc", checkResult.getLastLine());
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return false;
		}

		if (!checkResult.isValueFound()) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", RunnerUtils.TDW_NO_TABLE_ERROR_CODE);
			keyValues.put("task_desc", "Can't find the specified table: " + tableName
					+ " in specified database: " + databaseName);
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return false;
		}

		writeLocalLog(Level.INFO, "The specified table: " + tableName + " in specified database: "
				+ databaseName + " is found");

		return true;
	}

	/*
	 * Check the partition existence and create the partition if it doesn't
	 * exist.
	 */
	protected boolean createPartition(String plcProgram, LServer hiveServer, String databaseName,
			String tableName, String partitionName) throws Exception {

		if (partitionName != null && partitionName.length() > 2) {
			FindOutputResult checkResult = RunnerUtils.checkPartitions(plcProgram, hiveServer,
					databaseName, tableName, partitionName, this,plcParameter);

			if (checkResult.getExitVal() != 0) {
				Map<String, String> keyValues = new HashMap<String, String>();
				keyValues.put("exit_code", checkResult.getPLCReturnCode());
				keyValues.put("task_desc", checkResult.getLastLine());
				keyValues.put("run_date", HOUR_FORMAT.format(runDate));
				commitJsonResult(keyValues, false, "");
				return false;
			}

			if (!checkResult.isValueFound()) {
				PLCResult result = RunnerUtils.createPartition(plcProgram, hiveServer,
						databaseName, tableName, partitionName, this,plcParameter);
				
				if (result.getExitVal() != 0) {
					Map<String, String> keyValues = new HashMap<String, String>();
					keyValues.put("exit_code", RunnerUtils.TDW_NO_PARTITION_ERROR_CODE);
					keyValues.put("task_desc", result.getLastLine());
					keyValues.put("run_date", HOUR_FORMAT.format(runDate));
					commitJsonResult(keyValues, false, "");
					return false;
				}

				writeLocalLog(Level.INFO, "Create partition: " + partitionName + " for table: "
						+ tableName + " succeed");
			}
		}

		writeLocalLog(Level.INFO, "Partition: " + partitionName + " exists or just been created");

		return true;
	}

	/*
	 * Check the partition & subpartition existence and create the partition &
	 * subpartition if it doesn't exist.
	 */
	protected boolean createPartitionAndSubPartition(String plcProgram, LServer hiveServer,
			String databaseName, String tableName, String partitionName, String subPartitionName)
			throws Exception {

		boolean bResult = false;

		if (subPartitionName == null) {
			bResult = createPartition(plcProgram, hiveServer, databaseName, tableName,
					partitionName);
		}
		else {
			FindOutputResultWithSubPrition result = RunnerUtils.checkPartitionWithSubPartition(
					plcProgram, hiveServer, databaseName, tableName, partitionName,
					subPartitionName, this,plcParameter);
			if (result.getExitVal() != 0) {
				Map<String, String> keyValues = new HashMap<String, String>();
				keyValues.put("exit_code", result.getPLCReturnCode());
				keyValues.put("task_desc", result.getLastLine());
				keyValues.put("run_date", HOUR_FORMAT.format(runDate));
				commitJsonResult(keyValues, false, "");
				return false;
			}

			if (!result.isPriPartitionFound()) {
				PLCResult priResult = RunnerUtils.createPartition(plcProgram, hiveServer,
						databaseName, tableName, partitionName, this,plcParameter);
				if (result.getExitVal() != 0) {
					Map<String, String> keyValues = new HashMap<String, String>();
					keyValues.put("exit_code", RunnerUtils.TDW_NO_PARTITION_ERROR_CODE);
					keyValues.put("task_desc", priResult.getLastLine());
					keyValues.put("run_date", HOUR_FORMAT.format(runDate));
					commitJsonResult(keyValues, false, "");
					return false;
				}

				writeLocalLog(Level.INFO, "Create partition: " + partitionName + " for table: "
						+ tableName + " succeed");
			}

			if (!result.isSubPartitionFound()) {
				PLCResult subResult = RunnerUtils.createSubPartition(plcProgram, hiveServer,
						databaseName, tableName, partitionName, this,plcParameter);
				if (result.getExitVal() != 0) {
					Map<String, String> keyValues = new HashMap<String, String>();
					keyValues.put("exit_code", RunnerUtils.TDW_NO_PARTITION_ERROR_CODE);
					keyValues.put("task_desc", subResult.getLastLine());
					keyValues.put("run_date", HOUR_FORMAT.format(runDate));
					commitJsonResult(keyValues, false, "");
					return false;
				}

				writeLocalLog(Level.INFO, "Create sub partition: " + partitionName + " for table: "
						+ tableName + " succeed");
			}

			writeLocalLog(Level.INFO, "Partition: " + partitionName + " ; SubPartition "
					+ subPartitionName + " exists or just been created");

			return true;
		}

		return bResult;

	}

	/* Check to see whether there exists source files in the source path. */
	protected boolean checkHdfsEmptySource(String hadoopCommand, 
			                               LServer hdfsServer,
			                               String sourceFilePath, 
			                               String sourceFileNames) 
		throws Exception {
		
		return 
			checkHdfsEmptySource(hadoopCommand, hdfsServer, sourceFilePath, sourceFileNames, "0");
	}
	
	protected boolean checkHdfsEmptySource(String hadoopCommand,
			                               LServer hdfsServer,
			                               String sourceFilePath,
			                               String sourceFileNames,
			                               String failedOnZeroWrited) 
	    throws Exception {

		this.writeLocalLog(Level.WARNING, "Check multiple hadoop client: " + hadoopCommand);

		HdfsDirExistResult dirExistResult = RunnerUtils.isHdfsDirExist(hadoopCommand, hdfsServer,
				sourceFilePath, this);
		
		if (!dirExistResult.isDirExists()) {
			Map<String, String> keyValues = new HashMap<String, String>();
			
			if("1".equals(failedOnZeroWrited)){
				this.writeLocalLog(Level.INFO, 
						           "This task's source path is empty, but it's configured " +
				                   "to must have data, so it fails");
				keyValues.put("exit_code", "1");
				keyValues.put("task_desc", "That task will be failed because success_writed is 0 and failedOnZeroWrited is set to true");
				keyValues.put("run_date", HOUR_FORMAT.format(runDate));
				commitJsonResult(keyValues, false, "");
			} else {
			    keyValues.put("exit_code", String.valueOf(RunnerUtils.SUCCESS_CODE));
			    keyValues.put("task_desc", "Task succeeded because no files in the source path");
			    keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			    commitJsonResult(keyValues, true, "");
			}
			
			return true;
		}

		/* Return true if there is no source files at all. */
		this.writeLocalLog(Level.INFO, "sourceFilePath: " + sourceFilePath + ", sourceFileNames: "
				+ sourceFileNames);
		/*
		 * HdfsDirFileCounter fileCounter =
		 * RunnerUtils.getHdfsDirFileCount(hadoopCommand, hdfsServer,
		 * sourceFilePath, sourceFileNames, this); if (fileCounter.getExitVal()
		 * != 0) { Map<String, String> keyValues = new HashMap<String,
		 * String>(); keyValues.put("exit_code",
		 * fileCounter.getHadoopReturnCode()); keyValues.put("task_desc",
		 * "Task failed because listing source files status failed exit_value "
		 * + fileCounter.getExitVal()); keyValues.put("run_date",
		 * HOUR_FORMAT.format(runDate)); commitJsonResult(keyValues, false, "");
		 * return true; }
		 * 
		 * if (fileCounter.getFileCount() == 0) { Map<String, String> keyValues
		 * = new HashMap<String, String>(); keyValues.put("exit_code",
		 * String.valueOf(RunnerUtils.SUCCESS_CODE)); keyValues.put("task_desc",
		 * "Task succeeded because no files in the source path");
		 * keyValues.put("run_date", HOUR_FORMAT.format(runDate));
		 * commitJsonResult(keyValues, true, ""); return true; }
		 */
		writeLocalLog(Level.INFO, "There exists files in the source path, " + "need to load data");

		return false;
	}

	protected boolean clearOldData(String loadMode, String plcProgram, LServer hiveServer,
			String databaseName, String tableName, String partitionName, LTask task)
			throws Exception {

		if (TRUNCATE_FLAG.equalsIgnoreCase(loadMode)) {
			/* For truncation, just do truncation. */
			PLCResult result = RunnerUtils.truncateData(plcProgram, hiveServer, databaseName,
					tableName, partitionName, this,plcParameter);
			if (result.getExitVal() != 0) {
				Map<String, String> keyValues = new HashMap<String, String>();
				keyValues.put("exit_code", result.getPLCReturnCode());
				keyValues.put("task_desc", result.getLastLine());
				keyValues.put("run_date", HOUR_FORMAT.format(runDate));
				commitJsonResult(keyValues, false, "");
				return false;
			}
		}
		else if (APPEND_FLAG.equalsIgnoreCase(loadMode)) {
			final String cgiHost = this.requestGlobalParameters().get(CGI_HOST);
			final String cgiURL = this.requestGlobalParameters().get(CGI_URL);
			final String removeDataFileRetry = this.requestGlobalParameters().get(
					REMOVE_DATA_FILE_RETRY);
			String runTimeId = task.getRuntimeId();
			if (runTimeId != null && (runTimeId.length() > 2)) {
				StringTokenizer st = new StringTokenizer(runTimeId, ";");
				String readPLCParameter = st.nextToken();
				int retry = (removeDataFileRetry == null ? 10 : new Integer(removeDataFileRetry));
				boolean success = false;
				String errorMessage = null;
				while (!success && retry-- > 0) {
					DefaultHttpClient httpClient = new DefaultHttpClient();

					String url = cgiHost + cgiURL + "platform=" + hiveServer.getHost() + "&id="
							+ readPLCParameter + "&dbname=" + databaseName + "&tbname=" + tableName;
					if (partitionName != null && partitionName.length() > 2) {
						url = url + "&partname=" + partitionName;
					}

					this.writeLocalLog(Level.INFO, "URL: " + url);
					HttpGet httpGet = new HttpGet(url);
					HttpResponse response = httpClient.execute(httpGet);
					HttpEntity entity = response.getEntity();

					if (entity == null) {
						errorMessage = "No response from CGI.";
						continue;
					}

					BufferedReader reader = null;
					try {
						reader = new BufferedReader(new InputStreamReader(entity.getContent(),
								"UTF-8"));

						String line = null;
						int counter = 0;
						while ((line = reader.readLine()) != null) {
							line = line.trim();
							this.writeLocalLog(Level.INFO, "line: " + line + ", counter: "
									+ counter + ", 0 equals line: " + "0".equals(line));
							if (counter == 0 && "0".equals(line)) {
								success = true;
								break;
							}

							if (counter == 1) {
								errorMessage = line;
							}
							counter++;
						}
					}
					finally {
						if (reader != null) {
							reader.close();
						}
					}
				}

				if (success) {
					return true;
				}

				Map<String, String> keyValues = new HashMap<String, String>();
				keyValues.put("exit_code", RunnerUtils.TDW_CLEAR_OLD_DATA_ERROR_CODE);
				keyValues.put("task_desc", "Clear old data failed for appending table: "
						+ errorMessage);
				keyValues.put("run_date", HOUR_FORMAT.format(runDate));
				commitJsonResult(keyValues, false, runTimeId);
				return false;
			}
		}
		else {
			String failMessage = "Load data from HDFS to TDW failed, because " + loadMode
					+ " is not a supported load mode";
			writeLogAndCommitTask(failMessage, Level.SEVERE, LState.FAILED);
			return false;
		}

		writeLocalLog(Level.INFO, "Old data is cleared successfully");

		return true;
	}

	protected boolean createExternalTable(String plcProgram, LServer hiveServer,
			LServer nameNodeServer, String externalTableDataPath, String externalTableDb,
			String externalTableName, String sourceColumnNames, String delimiter, String charSet)
			throws Exception {

		PLCResult result = RunnerUtils.createExternalTable(plcProgram, plcParameter, hiveServer,
				nameNodeServer, externalTableDataPath, externalTableDb, externalTableName,
				sourceColumnNames, delimiter, charSet, this);
		if (result.getExitVal() != 0) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", result.getPLCReturnCode());
			keyValues.put("task_desc", result.getLastLine());
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return false;
		}
		externalTableCreated = true;
		writeLocalLog(Level.INFO, "Create external table: " + externalTableName + " on TDW: "
				+ hiveServer.getHost() + " successfully");

		return true;
	}

	protected boolean createExternalTableWithSQL(String plcProgram, LServer hiveServer,
			LServer nameNodeServer, String externalTableDataPath, String databaseName,
			String externalTableDb, String externalTableName, String filterSQL,
			String destFileDelimiter) throws Exception {

		ExplainSQLResult explainResult = RunnerUtils.explainSQL(plcProgram, plcParameter,
				hiveServer, databaseName, filterSQL, this);
		if (explainResult.getExitVal() != 0) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", explainResult.getPLCReturnCode());
			keyValues.put("task_desc", explainResult.getLastLine());
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return false;
		}

		PLCResult createExternalTableResult = RunnerUtils.createExternalTableWithSQL(plcProgram,
				plcParameter, hiveServer, nameNodeServer, externalTableDataPath, externalTableDb,
				externalTableName, destFileDelimiter, explainResult.getColumnNames(), this);
		if (createExternalTableResult.getExitVal() != 0) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", createExternalTableResult.getPLCReturnCode());
			keyValues.put("task_desc", createExternalTableResult.getLastLine());
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return false;
		}

		externalTableCreated = true;
		writeLocalLog(Level.INFO, "Create external table: " + externalTableName + " on TDW: "
				+ hiveServer.getHost() + " successfully");

		return true;
	}

	protected LoadDataResult loadData(String plcProgram, LServer hiveServer, String databaseName,
			String externalTableName, String tableName, String targetColumnNames, Date runDate,
			String whereClause, String externalTableDb, String specialParam) throws Exception {

		StringBuffer sb = new StringBuffer();
		sb.append("SET tdw.groupname=nrtgroup;").append("SET usp.param=" + plcParameter + ";");
		if(!StringUtils.isBlank(specialParam)){
			sb.append(specialParam);
			if(!specialParam.endsWith(";")){
				sb.append(";");
			}
		}
		sb.append("USE " + databaseName + ";");

		StringBuffer insertSQL = new StringBuffer();
		insertSQL.append("INSERT TABLE " + tableName + " SELECT ");

		/* Add the column names and the fake column. */
		StringTokenizer st = new StringTokenizer(targetColumnNames, ",");
		int countTokens = st.countTokens();
		int counter = 0;
		while (st.hasMoreTokens()) {
			String column = st.nextToken().trim();
			if (column.contains("[") && column.contains("]")) {
				String prefix = null;
				if (!column.startsWith("[")) {
					prefix = column.substring(column.indexOf("[") - 1, column.indexOf("["));
				}

				this.writeLocalLog(
						Level.INFO,
						"prefix is: " + prefix + ", prefix equals ): "
								+ ")".equalsIgnoreCase(prefix));
				if (prefix == null || !")".equalsIgnoreCase(prefix)) {
					column = column.replace("[", "");
					column = column.replace("]", "");
				}
				column = RunnerUtils.replaceDateExpr(column, runDate);
			}
			insertSQL.append(column);

			counter++;

			if (counter < countTokens) {
				insertSQL.append(",");
			}
		}
		insertSQL.append(" FROM " + externalTableDb + "::" + externalTableName);

		if (whereClause != null) {
			insertSQL.append(" " + RunnerUtils.replaceDateExpr(whereClause, runDate));
		}
		this.writeLocalLog(Level.INFO, "Whole SQL: " + sb.toString() + insertSQL.toString());

		/* Composite the command array. */
		String[] cmdArray = new String[] { plcProgram, hiveServer.getUserName(),
				hiveServer.getPassword(), hiveServer.getHost(),
				String.valueOf(hiveServer.getPort()), sb.toString() + insertSQL.toString() };

		/* Start the process of checking output result to find out session id. */
		CheckProcess process = new CheckProcess(cmdArray, insertSQL.toString(), null, this);
		process.startProcess();
		commitTask(LState.RUNNING,
				plcParameter + ";" + process.getProcessId() + ";" + hiveServer.getHost(),
				"Commit the process id for loading data from "
						+ "inner table to outer table and plc parameter");
		process.waitAndDestroyProcess();

		LoadDataResult result = new LoadDataResult();

		/* Find out the session id from the result. */
		result.setExitVal(process.getExitVal());
		result.setPLCReturnCode(RunnerUtils.getPLCReturnCode(process.getLastLine()));
		RunnerUtils.setLoadDataInformation(process.getStageOutput(), result, this, true);

		if (result.getExitVal() != 0) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", result.getPLCReturnCode());
			keyValues.put("task_desc", process.getLastLine());
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
		}
		writeLocalLog(Level.INFO, "Load data from external table: " + externalTableName
				+ " to regular table: " + tableName + " successfully");

		return result;
	}
	
	protected LoadDataResult loadDataNew(String plcProgram, LServer hiveServer, String databaseName,
			String externalTableName, String tableName, String targetColumnNames, Date runDate,
			String whereClause, String externalTableDb) throws Exception {

		StringBuffer sb = new StringBuffer();
		sb.append("SET tdw.groupname=nrtgroup;").append("SET usp.param=" + plcParameter + ";")
				.append("USE " + databaseName + ";");

		StringBuffer insertSQL = new StringBuffer();
		insertSQL.append("INSERT TABLE " + tableName + " SELECT ");

		/* Add the column names and the fake column. */
		StringTokenizer st = new StringTokenizer(targetColumnNames, ",");
		int countTokens = st.countTokens();
		int counter = 0;
		while (st.hasMoreTokens()) {
			String column = st.nextToken().trim();
			if (column.contains("[") && column.contains("]")) {
				String prefix = null;
				if (!column.startsWith("[")) {
					prefix = column.substring(column.indexOf("[") - 1, column.indexOf("["));
				}

				this.writeLocalLog(
						Level.INFO,
						"prefix is: " + prefix + ", prefix equals ): "
								+ ")".equalsIgnoreCase(prefix));
				if (prefix == null || !")".equalsIgnoreCase(prefix)) {
					column = column.replace("[", "");
					column = column.replace("]", "");
				}
				column = RunnerUtils.formatStrDateParam(column, runDate);
			}
			insertSQL.append(column);

			counter++;

			if (counter < countTokens) {
				insertSQL.append(",");
			}
		}
		insertSQL.append(" FROM " + externalTableDb + "::" + externalTableName);

		if (whereClause != null) {
			insertSQL.append(" " + RunnerUtils.replaceDateExpr(whereClause, runDate));
		}
		this.writeLocalLog(Level.INFO, "Whole SQL: " + sb.toString() + insertSQL.toString());

		/* Composite the command array. */
		String[] cmdArray = new String[] { plcProgram, hiveServer.getUserName(),
				hiveServer.getPassword(), hiveServer.getHost(),
				String.valueOf(hiveServer.getPort()), sb.toString() + insertSQL.toString() };

		/* Start the process of checking output result to find out session id. */
		CheckProcess process = new CheckProcess(cmdArray, insertSQL.toString(), null, this);
		process.startProcess();
		commitTask(LState.RUNNING,
				plcParameter + ";" + process.getProcessId() + ";" + hiveServer.getHost(),
				"Commit the process id for loading data from "
						+ "inner table to outer table and plc parameter");
		process.waitAndDestroyProcess();

		LoadDataResult result = new LoadDataResult();

		/* Find out the session id from the result. */
		result.setExitVal(process.getExitVal());
		result.setPLCReturnCode(RunnerUtils.getPLCReturnCode(process.getLastLine()));
		RunnerUtils.setLoadDataInformation(process.getStageOutput(), result, this, true);

		if (result.getExitVal() != 0) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", result.getPLCReturnCode());
			keyValues.put("task_desc", process.getLastLine());
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
		}
		writeLocalLog(Level.INFO, "Load data from external table: " + externalTableName
				+ " to regular table: " + tableName + " successfully");

		return result;
	}

	protected LoadDataResult loadDataWithSQL(String plcProgram, LServer hiveServer,
			String databaseName, String externalTableDb, String externalTableName, String filterSQL, String specialParam)
			throws Exception {

		StringBuilder sb = new StringBuilder();
		sb.append("SET tdw.groupname=nrtgroup;").append("SET usp.param=" + plcParameter + ";");
		if(!StringUtils.isBlank(specialParam)){
			sb.append(specialParam);
			if(!specialParam.endsWith(";")){
				sb.append(";");
			}
		}
		sb.append("USE " + externalTableDb + ";").append("ALTER TABLE " + externalTableName)
		  .append(" SET serdeproperties ('serialization.null.format'='');")
		  .append("USE " + databaseName + ";");
		final String insertSQLPrefix = "INSERT OVERWRITE TABLE " + externalTableDb + "::"
				+ externalTableName;
		String insertSQL = insertSQLPrefix + " " + filterSQL;
		sb.append(insertSQL);
		
		String[] cmdArray = new String[] { plcProgram, hiveServer.getUserName(),
				hiveServer.getPassword(), hiveServer.getHost(),
				String.valueOf(hiveServer.getPort()), sb.toString() };
		writeLocalLog(Level.INFO, "loadDataWithSql: "+sb.toString());
		
		CheckProcess process = new CheckProcess(cmdArray, insertSQLPrefix.toString(), null, this);
		process.startProcess();
		commitTask(LState.RUNNING,
				plcParameter + ";" + process.getProcessId() + ";" + hiveServer.getHost(),
				"Commit the process id for loading data from "
						+ "inner table to outer table and plc parameter");
		process.waitAndDestroyProcess();

		LoadDataResult result = new LoadDataResult();

		/* Find out the session id from the result. */
		result.setExitVal(process.getExitVal());
		result.setPLCReturnCode(RunnerUtils.getPLCReturnCode(process.getLastLine()));
		RunnerUtils.setLoadDataInformation(process.getStageOutput(), result, this, false);

		if (result.getExitVal() != 0) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", result.getPLCReturnCode());
			keyValues.put("task_desc", process.getLastLine());
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
		}
		writeLocalLog(Level.INFO, "Load data from external table: " + externalTableName
				+ " to regular table successfully");

		return result;
	}

	protected boolean createTmpPath(String hadoopCommand, LServer hdfsServer,
			String externalTableDataPath) throws Exception {

		this.writeLocalLog(Level.WARNING, "Check multiple hadoop client: " + hadoopCommand);

		HadoopResult result = RunnerUtils.createHDFSPath(hadoopCommand, hdfsServer,
				externalTableDataPath, this);
		if (result.getExitVal() != 0) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", result.getHadoopReturnCode());
			keyValues.put("task_desc", "Create temporary path: " + externalTableDataPath
					+ " on name node: " + hdfsServer.getHost() + " failed");
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return false;
		}
		tmpPathCreated = true;
		writeLocalLog(Level.INFO, "Create temporary path: " + externalTableDataPath + " on HDFS: "
				+ hdfsServer.getHost() + " successfully");

		return true;
	}

	private boolean isUsingSourceServer(LServer sourceServer, LServer targetServer) {
		String sourceVersion = sourceServer.getVersion();
		String targetVersion = targetServer.getVersion();

		if (!CDH3_VERSION.equalsIgnoreCase(sourceVersion)
				&& CDH3_VERSION.equalsIgnoreCase(targetVersion)) {

			return true;
		}

		return false;
	}

	private LServer getDistcpJobTracker(LTask task, boolean usingSourceServer, boolean isLoadingData)
			throws Exception {

		if (usingSourceServer) {
			return (isLoadingData ? task.getSourceServers(1) : task.getSourceServers(2));
		}

		return (isLoadingData ? task.getTargetServers(2) : task.getTargetServers(1));
	}

	private LServer getDistcpZookeeper(LTask task, LServer jobTracker, boolean usingSourceServer,
			boolean isLoadingData) throws Exception {

		if (!HA_VERSION.equalsIgnoreCase(jobTracker.getVersion())) {
			return null;
		}

		if (isLoadingData) {
			return (usingSourceServer ? task.getSourceServers(2) : task.getTargetServers(3));
		}
		else {
			// return (usingSourceServer ? task.getTargetServers(3) :
			// task.getSourceServers(2));
			return (usingSourceServer ? task.getSourceServers(3) : task.getTargetServers(2));
		}
	}

	protected boolean mrCopyData(LTask task, LServer hiveServer, LServer sourceServer,
			LServer targetServer, String sourcePath, String targetPath, boolean isLoadingData)
			throws Exception {

		final String haVersionFlag = this.requestGlobalParameters().get(HA_VERSION_FLAG);
		final String distCopyCommand = "distcp";

		/* Figure out the HADOOP command and job trackers used by distcp. */
		final boolean usingSourceServer = isUsingSourceServer(sourceServer, targetServer);
		final String hadoopCommand = (usingSourceServer ? getHadoopCommandOnVersion(sourceServer)
				: getHadoopCommandOnVersion(targetServer));
		final LServer jobTracker = getDistcpJobTracker(task, usingSourceServer, isLoadingData);

		this.writeLocalLog(Level.WARNING, "Hadoop client: " + hadoopCommand);
		this.writeLocalLog(Level.WARNING, "Job tracker: " + jobTracker.getHost());

		/* Get the source and target path. */
		sourcePath = RunnerUtils.getHostWithProtocol(sourceServer.getVersion(),
				sourceServer.getHost())
				+ ":" + sourceServer.getPort() + File.separator + sourcePath + File.separator;
		targetPath = RunnerUtils.getHostWithProtocol(targetServer.getVersion(),
				targetServer.getHost())
				+ ":" + targetServer.getPort() + File.separator + targetPath + File.separator;

		/* lyndldeng change here for CDH3 */
		final LServer defauleNameNode = isLoadingData ? targetServer : sourceServer;
		this.writeLocalLog(Level.INFO, "defauleNameNode: " + defauleNameNode.getHost());

		/* Composite the command array. */
		String[] cmdArray = new String[] {
				hadoopCommand,
				distCopyCommand,
				"-Dtdw.groupname=nrtgroup",
				"-Dfs.default.name=hdfs://" + defauleNameNode.getHost() + ":"
						+ defauleNameNode.getPort(),
				"-Dhadoop.job.ugi=" + targetServer.getUserName() + ","
						+ targetServer.getUserGroup(),
				"-Dmapred.job.tracker=" + jobTracker.getHost() + ":" + jobTracker.getPort(),
				"-update", "-m", "30", sourcePath, targetPath };

		/* If the job tracker is HA version, using the zookeeper configuration. */
		if (haVersionFlag.equals(jobTracker.getVersion())) {
			LServer zookeeper = getDistcpZookeeper(task, jobTracker, usingSourceServer,
					isLoadingData);
			if (zookeeper != null) {
				cmdArray = new String[] {
						hadoopCommand,
						distCopyCommand,
						"-Dtdw.groupname=nrtgroup",
						"-Dfs.default.name=hdfs://" + defauleNameNode.getHost() + ":"
								+ defauleNameNode.getPort(),
						"-Dhadoop.job.ugi=" + targetServer.getUserName() + ","
								+ targetServer.getUserGroup(),
						"-Dzookeeper.address=" + zookeeper.getHost(),
						"-Dmapred.job.tracker=" + jobTracker.getHost() + ":" + jobTracker.getPort(),
						"-update", "-m", "30", sourcePath, targetPath };
			}
		}
		this.writeLocalLog(Level.INFO, "HA version flag: " + haVersionFlag
				+ ", Job tracker version: " + jobTracker.getVersion() + ", jobtracker flag: "
				+ jobTracker.getTag());
		RunnerUtils.logCommand(cmdArray, this);

		RunTaskProcess process = new RunTaskProcess(cmdArray, this);
		process.startProcess(true);
		commitTask(LState.RUNNING,
				plcParameter + ";" + process.getProcessId() + ";" + hiveServer.getHost(),
				"Commit the distcopy process id and plc parameter");
		process.waitAndDestroyProcess();

		if (process.getExitVal() != 0) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", RunnerUtils.DISTCP_ERROR_CODE);
			keyValues.put("task_desc", "Hadoop distcp data to TDW failed");
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return false;
		}
		writeLocalLog(Level.INFO, "Hadoop distcp files successfully");

		/* Remove those useless distcp_logs files. */
		int removeResult = RunnerUtils.removeHadoopCopyLogFiles(hadoopCommand, targetServer,
				targetPath, this);
		if (removeResult != 0) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", RunnerUtils.REMOVE_DISTCP_FILE_ERROR_CODE);
			keyValues.put("task_desc", "Remove distcp log files failed");
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return false;
		}
		writeLocalLog(Level.INFO, "Remove distcp log files successfully");

		return true;
	}

	/* Add for Hdfs2Hdfs */
	protected boolean mrCopyData4h2h(LTask task, LServer hiveServer, LServer sourceServer,
			LServer targetServer, String sourcePath, String targetPath, boolean isLoadingData)
			throws Exception {

		final String haVersionFlag = this.requestGlobalParameters().get(HA_VERSION_FLAG);
		final String distCopyCommand = "distcp";

		/* Figure out the HADOOP command and job trackers used by distcp. */
		final boolean usingSourceServer = isUsingSourceServer(sourceServer, targetServer);
		final String hadoopCommand = (usingSourceServer ? getHadoopCommandOnVersion(sourceServer)
				: getHadoopCommandOnVersion(targetServer));

		/* check job tracker */
		final LServer jobTracker = usingSourceServer ? task.getSourceServers(1) : task
				.getTargetServers(1);

		this.writeLocalLog(Level.WARNING, "Hadoop client: " + hadoopCommand);
		this.writeLocalLog(Level.WARNING, "Job tracker: " + jobTracker.getHost());

		/* Get the source and target path. */
		sourcePath = RunnerUtils.getHostWithProtocol(sourceServer.getVersion(),
				sourceServer.getHost())
				+ ":" + sourceServer.getPort() + File.separator + sourcePath + File.separator;
		targetPath = RunnerUtils.getHostWithProtocol(targetServer.getVersion(),
				targetServer.getHost())
				+ ":" + targetServer.getPort() + File.separator + targetPath + File.separator;

		/* lyndldeng change here for CDH3 */
		final LServer defauleNameNode = isLoadingData ? targetServer : sourceServer;
		this.writeLocalLog(Level.INFO, "defauleNameNode: " + defauleNameNode.getHost());

		/* Composite the command array. */
		String[] cmdArray = new String[] {
				hadoopCommand,
				distCopyCommand,
				"-Dtdw.groupname=nrtgroup",
				"-Dhadoop.job.ugi=" + targetServer.getUserName() + ","
						+ targetServer.getUserGroup(),
				"-Dmapred.job.tracker=" + jobTracker.getHost() + ":" + jobTracker.getPort(),
				"-update", "-m", "30", sourcePath, targetPath };

		/* If the job tracker is HA version, using the zookeeper configuration. */
		if (haVersionFlag.equals(jobTracker.getVersion())) {
			LServer zookeeper = usingSourceServer ? task.getSourceServers(2) : task
					.getTargetServers(2);
			if (zookeeper != null) {
				cmdArray = new String[] {
						hadoopCommand,
						distCopyCommand,
						"-Dtdw.groupname=nrtgroup",
						"-Dhadoop.job.ugi=" + targetServer.getUserName() + ","
								+ targetServer.getUserGroup(),
						"-Dzookeeper.address=" + zookeeper.getHost(),
						"-Dmapred.job.tracker=" + jobTracker.getHost() + ":" + jobTracker.getPort(),
						"-update", "-m", "30", sourcePath, targetPath };
			}
		}
		this.writeLocalLog(Level.INFO, "HA version flag: " + haVersionFlag
				+ ", Job tracker version: " + jobTracker.getVersion() + ", jobtracker flag: "
				+ jobTracker.getTag());
		RunnerUtils.logCommand(cmdArray, this);

		RunTaskProcess process = new RunTaskProcess(cmdArray, this);
		process.startProcess(true);
		commitTask(LState.RUNNING,
				plcParameter + ";" + process.getProcessId() + ";" + hiveServer.getHost(),
				"Commit the distcopy process id and plc parameter");
		process.waitAndDestroyProcess();

		if (process.getExitVal() != 0) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", RunnerUtils.DISTCP_ERROR_CODE);
			keyValues.put("task_desc", "Hadoop distcp data to TDW failed");
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return false;
		}
		writeLocalLog(Level.INFO, "Hadoop distcp files successfully");

		/* Remove those useless distcp_logs files. */
		int removeResult = RunnerUtils.removeHadoopCopyLogFiles(hadoopCommand, targetServer,
				targetPath, this);
		if (removeResult != 0) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", RunnerUtils.REMOVE_DISTCP_FILE_ERROR_CODE);
			keyValues.put("task_desc", "Remove distcp log files failed");
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return false;
		}
		writeLocalLog(Level.INFO, "Remove distcp log files successfully");

		return true;
	}

	/* Add for TdBank */
	protected boolean mrCopyDataForFilelist(LTask task, LServer hiveServer, LServer sourceServer,
			LServer targetServer, String checkFileName, String targetPath, boolean isLoadingData)
			throws Exception {

		final String haVersionFlag = this.requestGlobalParameters().get(HA_VERSION_FLAG);
		final String distCopyCommand = "distcp";

		final boolean usingSourceServer = isUsingSourceServer(sourceServer, targetServer);
		final String hadoopCommand = (usingSourceServer ? getHadoopCommandOnVersion(sourceServer)
				: getHadoopCommandOnVersion(targetServer));
		final LServer jobTracker = getDistcpJobTracker(task, usingSourceServer, isLoadingData);

		this.writeLocalLog(Level.WARNING, "Hadoop client: " + hadoopCommand);
		this.writeLocalLog(Level.WARNING, "Job tracker: " + jobTracker.getHost());

		/* Get the source and target path. */
		checkFileName = RunnerUtils.getHostWithProtocol(sourceServer.getVersion(),
				sourceServer.getHost())
				+ ":" + sourceServer.getPort() + File.separator + checkFileName;
		targetPath = RunnerUtils.getHostWithProtocol(targetServer.getVersion(),
				targetServer.getHost())
				+ ":" + targetServer.getPort() + File.separator + targetPath + File.separator;

		/* lyndldeng change here for CDH3 */
		final LServer defauleNameNode = isLoadingData ? targetServer : sourceServer;
		this.writeLocalLog(Level.INFO, "defauleNameNode: " + defauleNameNode.getHost());

		/* Composite the command array. */
		String[] cmdArray = new String[] {
				hadoopCommand,
				distCopyCommand,
				"-Dtdw.groupname=nrtgroup",
				"-Dfs.default.name=hdfs://" + defauleNameNode.getHost() + ":"
						+ defauleNameNode.getPort(),
				"-Dhadoop.job.ugi=" + targetServer.getUserName() + ","
						+ targetServer.getUserGroup(),
				"-Dmapred.job.tracker=" + jobTracker.getHost() + ":" + jobTracker.getPort(),
				"-update", "-m", "30", "-f", checkFileName, targetPath };

		/* If the job tracker is HA version, using the zookeeper configuration. */
		if (haVersionFlag.equals(jobTracker.getVersion())) {
			LServer zookeeper = getDistcpZookeeper(task, jobTracker, usingSourceServer,
					isLoadingData);
			if (zookeeper != null) {
				cmdArray = new String[] {
						hadoopCommand,
						distCopyCommand,
						"-Dtdw.groupname=nrtgroup",
						"-Dfs.default.name=hdfs://" + defauleNameNode.getHost() + ":"
								+ defauleNameNode.getPort(),
						"-Dhadoop.job.ugi=" + targetServer.getUserName() + ","
								+ targetServer.getUserGroup(),
						"-Dzookeeper.address=" + zookeeper.getHost(),
						"-Dmapred.job.tracker=" + jobTracker.getHost() + ":" + jobTracker.getPort(),
						"-update", "-m", "30", "-f", checkFileName, targetPath };
			}
		}
		this.writeLocalLog(Level.INFO, "HA version flag: " + haVersionFlag
				+ ", Job tracker version: " + jobTracker.getVersion() + ", jobtracker flag: "
				+ jobTracker.getTag());
		RunnerUtils.logCommand(cmdArray, this);

		RunTaskProcess process = new RunTaskProcess(cmdArray, this);
		process.startProcess(true);
		commitTask(LState.RUNNING,
				plcParameter + ";" + process.getProcessId() + ";" + hiveServer.getHost(),
				"Commit the distcopy process id and plc parameter");
		process.waitAndDestroyProcess();

		if (process.getExitVal() != 0) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", RunnerUtils.DISTCP_ERROR_CODE);
			keyValues.put("task_desc", "Hadoop distcp data to TDW failed");
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return false;
		}
		writeLocalLog(Level.INFO, "Hadoop distcp files successfully");

		/* Remove those useless distcp_logs files. */
		int removeResult = RunnerUtils.removeHadoopCopyLogFiles(hadoopCommand, targetServer,
				targetPath, this);
		if (removeResult != 0) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", RunnerUtils.REMOVE_DISTCP_FILE_ERROR_CODE);
			keyValues.put("task_desc", "Remove distcp log files failed");
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return false;
		}
		writeLocalLog(Level.INFO, "Remove distcp log files successfully");

		return true;

	}

	/* Drop the external table and delete the temporary path for external table. */
	protected void doCleanWork(String plcProgram, LServer hiveServer, String externalTableDb,
			String externalTableName, String hadoopCommand, LServer nameNodeServer,
			String externalTableDataPath) throws Exception {

		this.writeLocalLog(Level.WARNING, "Check multiple hadoop client: " + hadoopCommand);

		/* Drop the external table. */
		if (externalTableCreated) {
			try {
				RunnerUtils.dropTable(plcProgram, hiveServer, externalTableDb, externalTableName,
						this,plcParameter);
				writeLocalLog(Level.INFO, "Drop external table: " + externalTableName + " on TDW: "
						+ hiveServer.getHost() + " successfully");
			}
			catch (Exception e) {
				writeLocalLog(
						Level.WARNING,
						"Drop external table: " + externalTableName + " on TDW: "
								+ hiveServer.getHost() + " failed with message: " + e.getMessage());
			}
		}

		/* Need to delete those files in the temporary path for external table. */
		if (tmpPathCreated) {
			HdfsDirExistResult result = RunnerUtils.isHdfsDirExist(hadoopCommand, nameNodeServer,
					externalTableDataPath, this);
			if (result.isDirExists()) {
				RunnerUtils.deleteHdfsFileRegressive(hadoopCommand, nameNodeServer,
						externalTableDataPath, this);
			}
			writeLocalLog(Level.INFO, "Delete the temporary path: " + externalTableDataPath
					+ " successfully");
		}
	}

	public boolean writeLogAndCheckFilesToHdfs(String hadoopCommand, LServer hdfsServer,
			String destFilePath, String destCheckFilePath, String destCheckFileName,
			String sourceFilePath, String sourceFileNames) throws Exception {

		this.writeLocalLog(Level.WARNING, "Check multiple hadoop client: " + hadoopCommand);

		/* Clean the dirty data and create a new directory. */
		HadoopResult result = RunnerUtils.createHDFSPath(hadoopCommand, hdfsServer, destFilePath,
				this);
		if (result.getExitVal() != 0) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", result.getHadoopReturnCode());
			keyValues.put("task_desc",
					"Create directory for saving files on target HDFS server failed");
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return false;
		}

		/*
		 * Clean the destination check file path if it's different from
		 * destination data path.
		 */
		if (!destCheckFilePath.equals(destFilePath)) {
			result = RunnerUtils.createHDFSPath(hadoopCommand, hdfsServer, destCheckFilePath, this);
			if (result.getExitVal() != 0) {
				Map<String, String> keyValues = new HashMap<String, String>();
				keyValues.put("exit_code", result.getHadoopReturnCode());
				keyValues.put("task_desc",
						"Create directory to save check file on target HDFS server failed");
				keyValues.put("run_date", HOUR_FORMAT.format(runDate));
				commitJsonResult(keyValues, false, "");
				return false;
			}
		}
		this.writeLocalLog(Level.INFO, "Cleaned the old check file and data files");

		/* Copy all the files in the local directory to remote HDFS. */
		result = RunnerUtils.writeLinuxFileToHdfs(hadoopCommand, hdfsServer, destFilePath,
				sourceFilePath, sourceFileNames, this);
		if (result.getExitVal() != 0) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", result.getHadoopReturnCode());
			keyValues.put("task_desc", "Copy source files on Linux to HDFS failed");
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return false;
		}

		/* Write the check file. */
		result = RunnerUtils.createHdfsCheckFile(this.getTask().getId(), HOUR_FORMAT
				.format(runDate), hadoopCommand, hdfsServer, destFilePath, this
				.requestGlobalParameters().get(CHECK_FILE_TMP_DIR), destCheckFilePath,
				destCheckFileName, this);
		if (result.getExitVal() != 0) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", result.getHadoopReturnCode());
			keyValues.put("task_desc", "Create the check file for this task failed");
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return false;
		}
		this.writeLocalLog(Level.INFO, "Write the check file for the task");

		return true;
	}

	/* Move the results from HDFS to Linux, also write the check file. */
	public boolean writeLogAndCheckFilesToLinux(String hadoopCommand, LServer hdfsServer,
			String destPath, String hdfsDirPath, String hdfsFilePattern, String destCheckFilePath,
			String destCheckFileName, AbstractTaskRunner runner) throws Exception {

		this.writeLocalLog(Level.WARNING, "Check multiple hadoop client: " + hadoopCommand);

		/* Clean and create a new target Linux directory. */
		File destDir = new File(destPath);
		RunnerUtils.deleteFile(destDir);
		boolean created = destDir.mkdirs();
		runner.writeLocalLog(Level.INFO, "Create path: " + destDir.getAbsolutePath()
				+ ", succeed: " + created);

		/* Clean the old check file. */
		File checkFilePathDir = new File(destCheckFilePath);
		if (!destCheckFilePath.equals(destPath)) {
			RunnerUtils.deleteFile(checkFilePathDir);
			created = checkFilePathDir.mkdirs();
			runner.writeLocalLog(Level.INFO, "Create path: " + checkFilePathDir.getAbsolutePath()
					+ ", succeed: " + created);
		}

		runner.writeLocalLog(Level.INFO, "Check file path: " + checkFilePathDir.getAbsolutePath()
				+ ", destCheckFileName: " + destCheckFileName);
		File checkFile = new File(checkFilePathDir, destCheckFileName);
		if (!checkFilePathDir.getName().equals(destDir.getName())) {
			/*
			 * Delete the whole check file directory if it's different from data
			 * directory.
			 */
			RunnerUtils.deleteFile(checkFilePathDir);
			checkFilePathDir.mkdirs();
		}
		else {
			/*
			 * Just clean the check file if it's the same ase the data
			 * directory.
			 */
			if (checkFile.exists()) {
				RunnerUtils.deleteFile(checkFile);
			}
		}

		/*
		 * Filter the source files and start writing to the destination
		 * directory.
		 */
		HadoopResult copyResult = RunnerUtils.copyHdfsFileToLinux(hadoopCommand, hdfsServer,
				hdfsDirPath, hdfsFilePattern, destDir, runner);
		if (copyResult.getExitVal() != 0) {
			Map<String, String> keyValues = new HashMap<String, String>();
			keyValues.put("exit_code", copyResult.getHadoopReturnCode());
			keyValues.put("task_desc", "Copy files from Hdfs to broker machine failed");
			keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			commitJsonResult(keyValues, false, "");
			return false;
		}
		runner.writeLocalLog(Level.INFO, "Copy Hdfs files from: " + hdfsDirPath + " to destDir: "
				+ destDir + " succeeded");

		/* Write the check contents to the check file. */
		File[] linuxFiles = destDir.listFiles();
		BufferedWriter checkFileWriter = null;
		try {
			checkFileWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
					checkFile)));
			checkFileWriter.write(linuxFiles.length + "");
			checkFileWriter.newLine();
			for (File linuxFile : linuxFiles) {
				checkFileWriter.write(linuxFile.getName());
				checkFileWriter.newLine();
			}
			checkFileWriter.flush();
		}
		finally {
			if (checkFileWriter != null) {
				checkFileWriter.close();
			}
		}

		return true;
	}
}