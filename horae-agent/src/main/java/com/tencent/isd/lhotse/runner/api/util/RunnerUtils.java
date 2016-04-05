package com.tencent.isd.lhotse.runner.api.util;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.runner.AbstractTaskRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.logging.Level;

public class RunnerUtils {
	private final static int BUFFER_SIZE = 1024000;
	/* Flush buffer size is 64M. */
	private final static int FLUSH_BUFFER_SIZE = 64 * 1024000;
	private final static int MAX_RETRY = 10;
	
	private static final String DAY_EXPR_PATH = "${YYYYMMDD}";
	private static final String HOUR_EXPR_PATH = "${YYYYMMDDHH}";
	private static final String MONTH_EXPR_PATH = "${YYYYMM}";
	public static final String DAY_EXPR = "YYYYMMDD";
	public static final String HOUR_EXPR = "YYYYMMDDHH";
	public static final String MONTH_EXPR = "YYYYMM";
	
	/* Replace the date regression string in the path. */
	public static String replaceDateExpr(String originalExpr, Date runDate) {
		if (originalExpr == null) {
			return null;
		}
		
		String year = new SimpleDateFormat("yyyy").format(runDate);
		String month = new SimpleDateFormat("MM").format(runDate);
		String day = new SimpleDateFormat("dd").format(runDate);
		String hour = new SimpleDateFormat("HH").format(runDate);
		
		/* Replace those date expression in the path. */
		originalExpr = 
			originalExpr.replace(HOUR_EXPR_PATH, year + month + day + hour);
		originalExpr = originalExpr.replace(DAY_EXPR_PATH, year + month + day);
		originalExpr = originalExpr.replace(MONTH_EXPR_PATH, year + month);
		
		/* Replace those data expression in the partition expression. */
		originalExpr = 
			originalExpr.replace(HOUR_EXPR, year + month + day + hour);
		originalExpr = originalExpr.replace(DAY_EXPR, year + month + day);
		originalExpr = originalExpr.replace(MONTH_EXPR, year + month);
		
		if (originalExpr.contains("{") || originalExpr.contains("}")) {
			throw new IllegalArgumentException("Illegal date format for: " + originalExpr);
		}
				
		return originalExpr;
	}
	
	public static FileSystem getHdfsFileSystem(LServer server) 
	    throws IOException {
			
		String nameNode = null;
		if (server.getHost().startsWith("hdfs://")) {
			nameNode = server.getHost() + ":" + server.getPort();
		} else {
			nameNode = "hdfs://" + server.getHost() + ":" + server.getPort();
		}
		String ugi = 
			server.getUserName() + "," + server.getUserGroup();
		
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name", nameNode);
		configuration.set("hadoop.job.ugi", ugi);
		configuration.setBoolean("fs.hdfs.impl.disable.cache", true);
		configuration.setBoolean("fs.file.impl.disable.cache", true);
		
		return FileSystem.get(configuration);
	}
	
	/* Delete a local Linux file. */
	public static void deleteFile(File deleteFile) {
		if (!deleteFile.exists()) {
			return;
		}
		
		if (deleteFile.isFile()) {
			deleteFile.delete();
		} else {
			/* Delete all the files in the destination directory first. */
			File[] allDestFiles = deleteFile.listFiles();
			for (File destFile : allDestFiles) {
				destFile.delete();
			}
			
			deleteFile.delete();
		}
	}
	
	/* Copy a HDFS file to a local Linux platform. */
	public static void copyHdfsFileToLinux(FileSystem hdfs, 
			                               Path srcPath, 
			                               File localFile) 
		throws Exception {
		
		/* Check whether the local file exists. */
		if (localFile.exists()) {
			localFile.delete();
		}
		
		File dir = localFile.getParentFile();
		if (!dir.exists()) {
			dir.mkdirs();
		}
		
		/* Do the actual copy. */
		int counter = 1;
		while (true) {
			boolean success = false;
			
		    InputStream in = null;
		    OutputStream out = null;
		    try {
			    in = hdfs.open(srcPath);
			    out = new FileOutputStream(localFile);
			    int bytesReadCycle = 0;
			    int bytesRead = -1;
		        byte buf[] = new byte[BUFFER_SIZE];
		        while ((bytesRead = in.read(buf)) >= 0) {
			        out.write(buf, 0, bytesRead);
			        bytesReadCycle += bytesRead;
			        /* Flush the buffer if we've read 64M data. */
			        if (bytesReadCycle >= FLUSH_BUFFER_SIZE) {
			    	    out.flush();
			    	    bytesReadCycle = 0;
			        }
		        }
		        out.flush();
		        success = true;
		    } catch (Exception e) {
		    	if (counter > MAX_RETRY) {
		    		throw e;
		    	}
		    } finally {
			    if (out != null) {
			        out.close();
		        }

		        if (in != null) {
			        in.close();
		        }
		    }
		    
		    counter++;
		    if (success) {		    
		        break;
		    }
		}
	}
	
	/* Write the Linux file to HDFS. */
	public static void writeLinuxFileToHdfs(FileSystem hdfs, 
			                                Path path, 
			                                File sourceFile) 
		throws Exception {
		
		int counter = 1;
		while (true) {
			boolean success = false;
			
		    FSDataOutputStream out = null;
		    FileInputStream fis = null;
		    try {
			    out = hdfs.create(path, true);//overwrite
			    fis = new FileInputStream(sourceFile);
			    byte buf[] = new byte[BUFFER_SIZE];
			    int bytesReadCycle = 0;
		        int bytesRead = -1;
		        while ((bytesRead = fis.read(buf)) >= 0) {
			        out.write(buf, 0, bytesRead);
			        bytesReadCycle += bytesRead;
			        /* Flush the buffer if we've read 64M data. */
			        if (bytesReadCycle >= FLUSH_BUFFER_SIZE) {
			    	    out.flush();
			    	    bytesReadCycle = 0;
			        }
		        }
		        out.flush();
		        success = true;
		    } catch (Exception e) {
		    	if (counter > MAX_RETRY) {
		    		throw e;
		    	}
		    } finally {
		        if (out != null) {
			        out.close();
		        }
		    
		        if (fis != null) {
		    	    fis.close();
		        }
		    }
		    
		    counter++;
		    if (success) {
		        break;
		    }
		}
	}
	
	/* Delete a HDFS file. */
	public static void deleteHdfsFile(FileSystem hdfs, Path deleteFile) 
	    throws IOException {
		
		if (!hdfs.exists(deleteFile)) {
			return;
		}
		
		FileStatus destPathStatus = hdfs.getFileStatus(deleteFile);
		hdfs.delete(deleteFile, destPathStatus.isDir());
	}
	
	/* Delete HDFS files with the pattern. */
	public static void deleteHdfsFileWithPattern(FileSystem hdfs, 
			                                     String hdfsDir, 
			                                     String pattern,
			                                     AbstractTaskRunner runner)
	    throws IOException {
		
		Path hdfsPath = new Path(hdfsDir);
		if (!hdfs.exists(hdfsPath)) {
			return;
		}
		
		FileStatus[] deleteFiles = 
			hdfs.listStatus(hdfsPath, new HDFSRegExprFilter(pattern, runner));
		for (FileStatus deleteFile : deleteFiles) {
			hdfs.delete(deleteFile.getPath(), deleteFile.isDir());
		}
	}
	
	/* Check whether a database exists in the TDW. */
	public static FindOutputResult checkDatabases(String plcProgram, 
			                                      String hiveUser,
			                                      String hivePassword,
			                                      String hiveIP,
			                                      String hivePort,
			                                      String dbName,
			                                      AbstractTaskRunner runner) 
	    throws IOException, InterruptedException {
		
		final String showDBCommands = "show databases";
		
		return returnProcessCheckResult(plcProgram, hiveUser, hivePassword, 
				                        hiveIP, hivePort, showDBCommands, 
				                        showDBCommands, dbName, runner);
    }
	
	/* 
	 * Check whether a specified value that you want to find in the process 
	 * standard output. 
	 */ 
	private static FindOutputResult returnProcessCheckResult(String plcProgram,
			                                                 String hiveUser,
			                                                 String hivePassword,
			                                                 String hiveIP,
			                                                 String hivePort,
			                                                 String executeCommand,
			                                                 String commandStart,
			                                                 String expectFindValue,
			                                                 AbstractTaskRunner runner) 
	    throws IOException, InterruptedException {
		
		String[] cmdArray = new String[] {
			plcProgram,
			hiveUser,
			hivePassword,
			hiveIP,
			hivePort,
			executeCommand
		};
		
		CheckProcess process = new CheckProcess(cmdArray, commandStart, 
				                                expectFindValue, runner);
		process.runProcess();
		
		return new FindOutputResult(process.getExitVal(), process.findCheckValue());
	}

	/* Check whether a table exists in a specified database. */
	public static FindOutputResult checkTables(String plcProgram,
			                                   String hiveUser,
			                                   String hivePassword,
			                                   String hiveIP,
			                                   String hivePort,
			                                   String dbName,
			                                   String tableName,
			                                   AbstractTaskRunner runner) 
	    throws IOException, InterruptedException {
		
		final String useDBCommands = "use " + dbName;
		final String showTableCommands = "show tables";
		
		return returnProcessCheckResult(plcProgram, hiveUser, hivePassword, 
				                        hiveIP, hivePort, 
				                        useDBCommands + ";" + showTableCommands,
				                        showTableCommands, tableName, runner);
	}
	
	/* Check whether a partition exists in a specified table. */
	public static FindOutputResult checkPartitions(String plcProgram,
                                                   String hiveUser,
                                                   String hivePassword,
                                                   String hiveIP,
                                                   String hivePort,
                                                   String dbName,
                                                   String tableName,
                                                   String partitionName,
                                                   AbstractTaskRunner runner) 
	    throws IOException, InterruptedException {
		
		final String useDBCommands = "use " + dbName;
		final String showPartitionCommands = "show partitions " + tableName;
		
		return returnProcessCheckResult(plcProgram, hiveUser, hivePassword,
				                        hiveIP, hivePort,
				                        useDBCommands + ";" + showPartitionCommands,
				                        showPartitionCommands,
				                        partitionName, runner);
	}
	
	/* Create the partition. */
	public static int createPartition(String plcProgram,
			                          String hiveUser,
			                          String hivePassword,
			                          String hiveIP,
			                          String hivePort,
			                          String dbName,
			                          String tableName,
			                          String partitionName,
			                          AbstractTaskRunner runner) 
	    throws Exception {
		
		final String addPartitionCommand =
			"USE " + dbName + ";" + "ALTER TABLE " + tableName + 
			" ADD PARTITION " + partitionName + " VALUES IN (" +
			partitionName.substring(2, partitionName.length()) + ")";
		
		runner.writeLocalLog(Level.INFO, addPartitionCommand);
		
		String[] cmdArray = new String[] {
			plcProgram,
			hiveUser,
			hivePassword,
			hiveIP,
			hivePort,
			addPartitionCommand
		};
		
		RunTaskProcess addPartitionProcess = 
			new RunTaskProcess(cmdArray, runner);
		addPartitionProcess.startProcess(true);
		addPartitionProcess.waitAndDestroyProcess();
		
		return addPartitionProcess.getExitVal();
	}
	
	/* Truncate the partition. */
	public static int truncateData(String plcProgram,
			                       String hiveUser,
			                       String hivePassword,
			                       String hiveIP,
			                       String hivePort,
			                       String dbName,
			                       String tableName,
			                       String partitionName, 
			                       AbstractTaskRunner runner) 
	    throws Exception {
		
		final String truncateDataCommand =
			"USE " + dbName + ";" + ((partitionName == null || partitionName.length() < 2)?
			"TRUNCATE TABLE " + tableName
			: "ALTER TABLE " + tableName + " TRUNCATE PARTITION (" + partitionName + ")");
		
		String[] cmdArray = new String[] {
			plcProgram,
			hiveUser,
			hivePassword,
			hiveIP,
			hivePort,
			truncateDataCommand
		};
		
		RunTaskProcess truncateDataProcess = 
			new RunTaskProcess(cmdArray, runner);
		truncateDataProcess.startProcess(true);
		truncateDataProcess.waitAndDestroyProcess();
	
		return truncateDataProcess.getExitVal();
	}
	
	/* Remove the TDW data with the session id, for those redo task. */
	public static boolean removeHDFSFilesWithPattern(String nameNodeUser,
			                                         String nameNodeGroup,
			                                         String nameNodeIP,
			                                         String nameNodePort,
			                                         String sessionId) 
	    throws IOException {
		
		/* Get the query id based on the session id. */
		
		/* Get the job id by the query id. */
				
		/* Initialize the HDFS file system to clean those log files. */
				
		/* If the table doesn't have partition, delete the log files in the default partition. */ 
				
		return false;
	}
	
	/* Delete those hadoop distcp copy log files. */
	public static void removeHadoopCopyLogFiles(FileSystem hdfs,
			                                    String filePath) 
	    throws IOException {
		
		final String copyLogFilePattern = "_distcp_logs";
		
		FileStatus[] copyLogFiles = 
			hdfs.listStatus(new Path(filePath), 
					        new CopyLogFileFilter(copyLogFilePattern));
		
		for (FileStatus copyLogFile : copyLogFiles) {
			deleteHdfsFile(hdfs, new Path(filePath, copyLogFile.getPath().getName()));
		}
	}
	
	/* Create the HDFS temporary path for HDFS, LINUX load in task. */
	public static boolean createTempHDFSPath(FileSystem hdfs, String tmpPath) 
	    throws IOException {
		
		Path hdfsTempPath = new Path(tmpPath);
		
		/* Delete the old temporary path first to clean the old data. */
		if (hdfs.exists(hdfsTempPath)) {
			deleteHdfsFile(hdfs, hdfsTempPath);
		}
		
		/* Create the new temporary path. */
		return hdfs.mkdirs(hdfsTempPath);
	}
	
	public static int hadoopMRCopy(String hadoopCommand,
			                       String hdfsIP,
			                       String hdfsPort,
			                       String nameNodeIP,
			                       String nameNodePort,
			                       String nameNodeUser,
			                       String nameNodeGroup,
			                       String mrCopyJobTracker,
			                       String sourceFilePath,
			                       String tmpPath,
			                       AbstractTaskRunner runner) 
        throws Exception {
		
		final String distCopyCommand = "distcp";
	    final String hdfsPath = 
	    	"hdfs://" + hdfsIP + ":" + hdfsPort + Path.SEPARATOR + 
	    	sourceFilePath + Path.SEPARATOR;
	    final String nameNodePath =
	    	"hdfs://" + nameNodeIP + ":" + nameNodePort + Path.SEPARATOR + 
	    	tmpPath + Path.SEPARATOR;
	    
		String[] cmdArray = new String[] {
			hadoopCommand,
			distCopyCommand,
			"-Dtdw.groupname=nrtgroup",
			"-Dhadoop.job.ugi=" + nameNodeUser + "," + nameNodeGroup,
			"-Dmapred.job.tracker=" + mrCopyJobTracker,
			"-update",
			"-m",
			"30",
			hdfsPath,
			nameNodePath
		};
		
		for (String cmdArrayStr : cmdArray) {
			runner.writeLocalLog(Level.INFO, cmdArrayStr);
		}
		
		RunTaskProcess distcopyProcess = new RunTaskProcess(cmdArray, runner);
		distcopyProcess.startProcess(true);
		distcopyProcess.waitAndDestroyProcess();
		
		return distcopyProcess.getExitVal();
	}
	
	public static int createExternalTable(String plcProgram, 
			                              String plcParameter,
			                              String hiveUser, 
			                              String hivePassword, 
                                          String hiveServerIP, 
                                          String hiveServerPort,
                                          String nameNodeIP,
                                          String nameNodePort,
                                          String tempPath,
                                          String externalTableDb,
                                          String externalTableName, 
                                          String sourceColumnNames,
                                          String delimiter,
                                          String charSet,
                                          AbstractTaskRunner runner) 
        throws Exception {
		
		StringBuffer sb = new StringBuffer();
		sb.append("SET tdw.groupname=nrtgroup;")
		  .append("SET usp.param=" + plcParameter + ";")
		  .append("USE " + externalTableDb + ";")
		  .append("CREATE EXTERNAL TABLE " + externalTableName + " (");
		
		StringTokenizer st = new StringTokenizer(sourceColumnNames, ",");
		int countTokens = st.countTokens();
		int counter = 0;
		while (st.hasMoreTokens()) {
			sb.append(st.nextToken().trim() + " string");
			counter++;
			if (counter < countTokens) {
				sb.append(",");
			} else {
				sb.append(")");
			}
		}
		
		/* TODO: replace the delimiter using the ASCII code. */
		sb.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + delimiter + "' LOCATION ")
		  .append("'hdfs://" + nameNodeIP + ":" + nameNodePort + Path.SEPARATOR + tempPath + "';")
		  .append("ALTER TABLE " + externalTableName + " set serdeproperties ('charset'='" + charSet + "');")
		  .append("ALTER TABLE " + externalTableName + " set TBLPROPERTIES ('escape.delim'=' ')");
		
		String[] cmdArray = new String[] {
			plcProgram,
			hiveUser,
			hivePassword,
			hiveServerIP,
			hiveServerPort,
			sb.toString()			
		};
		
		RunTaskProcess createExternalTableProcess = 
			new RunTaskProcess(cmdArray, runner);
		createExternalTableProcess.startProcess(true);
		createExternalTableProcess.waitAndDestroyProcess();
		
		return createExternalTableProcess.getExitVal();
	}
	
	/* Load data from the external table into the TDW table. */
	public static SessionAndExitVal loadData(String plcProgram,
			                                 String plcParameter,
			                                 String hiveUser,
			                                 String hivePassword,
			                                 String hiveIP,
			                                 String hivePort,
			                                 String databaseName,
			                                 String externalTableName,
			                                 String tableName,
			                                 String targetColumnNames,
			                                 Date runDate,
			                                 String whereClause,
			                                 String externalTableDb,
			                                 AbstractTaskRunner runner) 
	    throws IOException, InterruptedException {
		
		StringBuffer sb = new StringBuffer();
		sb.append("SET tdw.groupname=nrtgroup;")
		  .append("SET usp.param=" + plcParameter + ";")
		  .append("USE " + databaseName + ";");
		
		StringBuffer insertSQL = new StringBuffer();
		insertSQL.append("INSERT TABLE " + tableName + " SELECT ");
		
		/* Add the column names and the fake column. */
		StringTokenizer st = new StringTokenizer(targetColumnNames, ",");
		int countTokens = st.countTokens();
		int counter = 0;
		while (st.hasMoreTokens()) {
			String column = st.nextToken().trim();
			if (column.startsWith("[") && column.endsWith("]")) {
				column = column.substring(1, column.length() - 1);
				column = replaceDateExpr(column, runDate);
			} 
			insertSQL.append(column);
					
			counter++;
			
			if (counter < countTokens) {
				insertSQL.append(",");
			}
		}		
		insertSQL.append(" FROM " + externalTableDb + "::" + externalTableName);
		
		if (whereClause != null) {
			insertSQL.append(" " + whereClause);
		}
		
		runner.writeLocalLog(Level.INFO, "Whole SQL: " + sb.toString() + insertSQL.toString());
		
		/* Composite the command array. */
		String[] cmdArray = new String[] {
			plcProgram,
			hiveUser,
			hivePassword,
			hiveIP,
			hivePort,
			sb.toString() + insertSQL.toString()
		};
		
		/* Start the process of checking output result to find out session id. */
		CheckProcess loadDataProcess = 
			new CheckProcess(cmdArray, insertSQL.toString(), null, runner);
		loadDataProcess.runProcess();
		
		final String sessionToken = "session:";		
		boolean find = false;
		SessionAndExitVal result = new SessionAndExitVal();
		
		/* Find out the session id from the result. */
		String lastLineWithSession = loadDataProcess.getLastLine();
		st = new StringTokenizer(lastLineWithSession, " ");
		while (st.hasMoreTokens()) {
			String tokenValue = st.nextToken();
			if (!find && sessionToken.equalsIgnoreCase(tokenValue)) {
				find = true;
				continue;
			}
			
			if (find) {
				result.setSessionId(tokenValue);
				break;
			}
		}
		result.setExitVal(loadDataProcess.getExitVal());
		
		ArrayList<String> stageOutput = loadDataProcess.getStageOutput();
		runner.writeLocalLog(Level.INFO, "stage output size: " + stageOutput.size());
		final String SUCCESS_PREFIX = "READ_SUCCESS_COUNT:";
		final String FAIL_PREFIX = "READ_ERROR_COUNT:";
		for (String output : stageOutput) {
			runner.writeLocalLog(Level.INFO, "output: " + output);
			if (output.startsWith(SUCCESS_PREFIX)) {
				result.setSuccessWrite(new Long(output.substring(SUCCESS_PREFIX.length(), 
						                        output.length())));
			}
			
			if (output.startsWith(FAIL_PREFIX)) {
				result.setFailWrite(new Long(output.substring(FAIL_PREFIX.length(), 
						                     output.length())));
			}
		}
		
		return result;
	}
	
	/* Drop the external table. */
	public static int dropTable(String plcProgram, 
			                    String hiveUser, 
			                    String hivePassword,
			                    String hiveServerIP, 
			                    String hiveServerPort, 
			                    String databaseName,
                                String tableName,
                                AbstractTaskRunner runner) 
        throws Exception {
		
		final String dropTableSQL = "USE " + databaseName + ";" + "DROP TABLE " + tableName;
		
		String[] cmdArray = new String[] {
			plcProgram,
			hiveUser,
			hivePassword,
			hiveServerIP,
			hiveServerPort,
			dropTableSQL
		};
		
		RunTaskProcess dropTableProcess = new RunTaskProcess(cmdArray, runner);
		dropTableProcess.startProcess(true);
		dropTableProcess.waitAndDestroyProcess();
		
		return dropTableProcess.getExitVal();		
	}
	
	public static int createExternalTableWithSQL(String plcProgram, 
			                                     String plcParameter,
			                                     String hiveUser, 
			                                     String hivePassword, 
			                                     String hiveIP, 
			                                     String hivePort, 
	                                             String nameNodeIP, 
	                                             String nameNodePort, 
	                                             String externalTableDataPath,
	                                             String databaseName,
	                                             String externalTableDb, 
	                                             String externalTableName, 
	                                             String filterSQL, 
	                                             String destFileDelimiter,
	                                             AbstractTaskRunner runner) 
	    throws Exception {
		
		/* First explain the filterSQL to get out the columns we need to export. */
		final String explainSQL = 
			"SET tdw.groupname=nrtgroup; SET usp.param=" + plcParameter + "; " +
		    "USE " + databaseName + "; EXPLAIN SELECT * FROM (" + filterSQL + ") GETFILEDTABLE";
		
		String[] cmdArray = new String[] {
			plcProgram,
			hiveUser,
			hivePassword,
			hiveIP,
			hivePort,
			explainSQL.toString()
		};
		
		GetFieldNamesProcess explainProcess = 
			new GetFieldNamesProcess(cmdArray, runner);
		explainProcess.runProcess();
		
		if (explainProcess.getExitVal() != 0) {
			return explainProcess.getExitVal();
		}
		
		ArrayList<String> fieldNames = explainProcess.getFieldNames();
		if (fieldNames.size() == 0) {
			return -1;
		}
		
		StringBuilder sb = new StringBuilder();
		sb.append("SET tdw.groupname=nrtgroup;")
		  .append("SET usp.param=" + plcParameter + ";")
		  .append("USE " + externalTableDb + ";")
		  .append("CREATE EXTERNAL TABLE " + externalTableName + "(");
		
		int counter = 0;
		for (String fieldName : fieldNames) {
			sb.append(fieldName + " string");
			counter++;
			if (counter < fieldNames.size()) {
				sb.append(", ");
			}
		}
		sb.append(") ROW FORMAT DELIMITED FIELDS TERMINATED BY '")
		  .append(destFileDelimiter + "' LOCATION 'hdfs://" + nameNodeIP)
		  .append(":" + nameNodePort + Path.SEPARATOR + externalTableDataPath + "'");
		
		cmdArray = new String[] {
			plcProgram,
			hiveUser,
			hivePassword,
			hiveIP,
			hivePort,
			sb.toString()
		};
		
		RunTaskProcess createExternalTableProcess = 
			new RunTaskProcess(cmdArray, runner);
		createExternalTableProcess.startProcess(true);
		createExternalTableProcess.waitAndDestroyProcess();
		
		return createExternalTableProcess.getExitVal();
	}
	
	public static SessionAndExitVal loadDataWithSQL(String plcProgram,
			                                        String plcParameter,
			                                        String hiveUser, 
			                                        String hivePassword, 
			                                        String hiveIP, 
			                                        String hivePort, 
		                                            String databaseName,
		                                            String externalTableDb,
		                                            String externalTableName, 
		                                            String filterSQL,
		                                            AbstractTaskRunner runner) 
		throws Exception {
		
		StringBuilder sb = new StringBuilder();
		sb.append("SET tdw.groupname=nrtgroup;")
		  .append("SET usp.param=" + plcParameter + ";")
		  .append("USE " + externalTableDb + ";")
		  .append("ALTER TABLE " + externalTableName)
		  .append(" SET serdeproperties ('serialization.null.format'='');")
		  .append("USE " + databaseName + ";");
		final String insertSQLPrefix = "INSERT OVERWRITE TABLE " + 
		                               externalTableDb + "::" + externalTableName;
		String insertSQL = insertSQLPrefix + " " + filterSQL;
		sb.append(insertSQL);
		
		String[] cmdArray = new String[] {
			plcProgram,
			hiveUser,
			hivePassword,
			hiveIP,
			hivePort,
			sb.toString()
		};
		
		CheckProcess loadDataProcess = 
			new CheckProcess(cmdArray, insertSQLPrefix.toString(), null, runner);
		loadDataProcess.runProcess();
			
		final String sessionToken = "session:";		
		boolean find = false;
		SessionAndExitVal result = new SessionAndExitVal();
			
		/* Find out the session id from the result. */
		String lastLineWithSession = loadDataProcess.getLastLine();
		StringTokenizer	st = new StringTokenizer(lastLineWithSession, " ");
		while (st.hasMoreTokens()) {
			String tokenValue = st.nextToken();
			if (!find && sessionToken.equalsIgnoreCase(tokenValue)) {
				find = true;
				continue;
			}
				
			if (find) {
				result.setSessionId(tokenValue);
				break;
			}
		}
		result.setExitVal(loadDataProcess.getExitVal());
			
		ArrayList<String> stageOutput = loadDataProcess.getStageOutput();
		runner.writeLocalLog(Level.INFO, "stage output size: " + stageOutput.size());
		final String SUCCESS_PREFIX = "READ_SUCCESS_COUNT:";
		final String FAIL_PREFIX = "READ_ERROR_COUNT:";
		for (String output : stageOutput) {
			runner.writeLocalLog(Level.INFO, "output: " + output);
			if (output.startsWith(SUCCESS_PREFIX)) {
				result.setSuccessWrite(new Long(output.substring(SUCCESS_PREFIX.length(), 
						                        output.length())));
			}
				
			if (output.startsWith(FAIL_PREFIX)) {
				result.setFailWrite(new Long(output.substring(FAIL_PREFIX.length(), 
						                     output.length())));
			}
		}
			
		return result;		
	}
	
	/* Move the results from HDFS to Linux, also write the check file. */
	public static void writeLogAndCheckFilesToLinux(FileSystem hdfs, 
			                                        String destPath, 
			                                        String hdfsDirPath, 
			                                        String hdfsFilePattern,
			                                        String destCheckFilePath,
			                                        String destCheckFileName,
			                                        AbstractTaskRunner runner) 
	    throws Exception {
		
		/* Clean and create a new target Linux directory. */
		File destDir = new File(destPath);
		RunnerUtils.deleteFile(destDir);
		destDir.mkdirs();
		
		/* Clean the old check file. */
        File checkFilePathDir = new File(destCheckFilePath);
        File checkFile = new File(checkFilePathDir, destCheckFileName);
        if (!checkFilePathDir.getName().equals(destDir.getName())) {
        	/* Delete the whole check file directory if it's different from data directory. */
            RunnerUtils.deleteFile(checkFilePathDir);
            checkFilePathDir.mkdirs();
        } else {
            /* Just clean the check file if it's the same ase the data directory. */
            if (checkFile.exists()) {
                RunnerUtils.deleteFile(checkFile);
            }
        }
		
		/* Filter the source files and start writing to the destination directory. */
        runner.writeLocalLog(Level.INFO, "hdfs dir path: " + hdfsDirPath);
        FileStatus[] sourceFiles = null;
        if (hdfsFilePattern == null) {
            sourceFiles = hdfs.listStatus(new Path(hdfsDirPath));
        } else {
            sourceFiles = hdfs.listStatus(new Path(hdfsDirPath), 
            	                          new HDFSRegExprFilter(hdfsFilePattern, runner));
        }
        
        runner.writeLocalLog(Level.INFO, "source files size: " + sourceFiles.length);
        /* Move the files from HDFS to Linux. */
        for (FileStatus sourceFile : sourceFiles) {
            copyHdfsFileToLinux(hdfs, sourceFile.getPath(), 
        	    	            new File(destPath, sourceFile.getPath().getName()));
            runner.writeLocalLog(Level.INFO, "Copy file: " + sourceFile.getPath().getName());
        }        
        
        /* Write the check contents to the check file. */
        BufferedWriter checkFileWriter = null;
        try {
            checkFileWriter =	
        	    new BufferedWriter(new OutputStreamWriter(new FileOutputStream(checkFile)));
            checkFileWriter.write(sourceFiles.length + "");
            checkFileWriter.newLine();
            for (FileStatus sourceFile : sourceFiles) {
                checkFileWriter.write(sourceFile.getPath().getName());
                checkFileWriter.newLine();
            }
            checkFileWriter.flush();
        } finally {
            if (checkFileWriter != null) {
        	    checkFileWriter.close();
            }
        }        
	}	
	
	public static void writeLogAndCheckFilesToHdfs(FileSystem hdfs,
                                                   String destFilePath,
                                                   String destCheckFilePath,
                                                   String destCheckFileName,
                                                   String sourceFilePath,
                                                   String sourceFileNames,
                                                   AbstractTaskRunner runner) 
        throws Exception {

        /* Clean the target directory before the action in case it has dirty data. */
        Path destDirPath = new Path(destFilePath); 
        RunnerUtils.deleteHdfsFile(hdfs, destDirPath);

        /* Create a new directory. */
        hdfs.mkdirs(destDirPath);       

        /* Clean the destination check file path if it's different from destination data path. */
        Path destCheckFileDir = new Path(destCheckFilePath);
        Path destCheckFile = new Path(destCheckFileDir, destCheckFileName);
        if (destCheckFileDir.getName().equals(destDirPath.getName())) {
            RunnerUtils.deleteHdfsFile(hdfs, destCheckFile);
        } else {
            RunnerUtils.deleteHdfsFile(hdfs, destCheckFileDir);
            hdfs.mkdirs(destCheckFileDir);
        }
        
        runner.writeLocalLog(Level.INFO, "Cleaned the old check file and data files");

        /* Copy all the files in the local directory to remote HDFS. */
        ArrayList<String> fileNames = new ArrayList<String>();
        File[] sourceFiles = 
            new File(sourceFilePath).listFiles(new RegFileNameFilter(sourceFileNames));
        for (File sourceFile : sourceFiles) {
            Path destFile = new Path(destDirPath, sourceFile.getName());
            writeLinuxFileToHdfs(hdfs, destFile, sourceFile);
            runner.writeLocalLog(Level.INFO, "Write file: " + destFile.getName() + " to HDFS");
            fileNames.add(sourceFile.getName());
        }        

        writeHdfsCheckFile(hdfs, destCheckFilePath, destCheckFileName, fileNames);
        runner.writeLocalLog(Level.INFO, "Write the check file for the task");
    }
	
	/* Write the check file on HDFS. */
	public static void writeHdfsCheckFile(FileSystem hdfs, 
			                              String destCheckFileDir, 
			                              String destCheckFileName,
			                              ArrayList<String> fileNames) 
	    throws IOException {
		
		BufferedWriter checkFileWriter = null;
        FSDataOutputStream fos = null;
        try {
            fos = hdfs.create(new Path(destCheckFileDir, destCheckFileName));
            checkFileWriter = 
                new BufferedWriter(new OutputStreamWriter(fos));
            checkFileWriter.write(fileNames.size() + "");
            checkFileWriter.newLine();
            for (String fileName : fileNames) {
                checkFileWriter.write(fileName);
                checkFileWriter.newLine();
            }
            fos.sync();
            checkFileWriter.flush();
        } finally {
            if (fos != null) {
                fos.close();
            }

            if (checkFileWriter != null) {
                checkFileWriter.close();
            }
        }
	}
	
	public static class SessionAndExitVal {
		private String sessionId;
		private int exitVal;
		private long successWrite;
		private long failWrite;
		
		public void setSessionId(String sessionId) {
			this.sessionId = sessionId;
		}
		
		public void setExitVal(int exitVal) {
			this.exitVal = exitVal;
		}
		
		public void setSuccessWrite(long successWrite) {
			this.successWrite = successWrite;
		}
		
		public void setFailWrite(long failWrite) {
			this.failWrite = failWrite;
		}
		
		public String getSessionId() {
			return sessionId;
		}
		
		public int getExitVal() {
			return exitVal;	
		}
		
		public long getSuccessWrite() {
			return successWrite;
		}
		
		public long getFailWrite() {
			return failWrite;
		}
	}
	
	public static class FindOutputResult {
		private final int exitVal;
		private final boolean findValue;
		
		public FindOutputResult(int exitVal,
				                boolean findValue) {
			this.exitVal = exitVal;
			this.findValue = findValue;
		}
		
		public int getExitVal() {
			return exitVal;
		}
		
		public boolean isValueFound() {
			return findValue;
		}
	}	
}
