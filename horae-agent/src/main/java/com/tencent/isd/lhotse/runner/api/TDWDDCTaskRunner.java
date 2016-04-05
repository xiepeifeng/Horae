package com.tencent.isd.lhotse.runner.api;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.api.util.RunnerUtils;
import com.tencent.isd.lhotse.runner.api.util.RunnerUtils.FindOutputResult;
import com.tencent.isd.lhotse.runner.api.util.RunnerUtils.SessionAndExitVal;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Date;
import java.util.logging.Level;

public abstract class TDWDDCTaskRunner extends DDCTaskRunner {
	protected final String PLC_PROGRAM = "plcProgram";
	protected final String HADOOP_COMMAND = "hadoopCommand";
	protected final String NAME_NODE_TMP_PATH = "nameNodeTmpPath";
	protected final String EXTERNAL_TABLE_CREATE_DB = "externalTableDb";
	protected final String TRUNCATE_FLAG = "truncate";
	protected final String APPEND_FLAG = "append";
	protected boolean externalTableCreated = false;
	protected boolean tmpPathCreated = false;
	
	/* Check the table existence. */
	protected boolean checkDbExistence(String plcProgram, 
			                           String hiveUser,
			                           String hivePassword,
			                           String hiveServerIP,
			                           String hiveServerPort,
			                           String databaseName)
	    throws Exception {
		
		FindOutputResult checkResult = 
			RunnerUtils.checkDatabases(plcProgram, hiveUser, hivePassword, 
			                           hiveServerIP, hiveServerPort,
				                       databaseName, this);
		
		if (checkResult.getExitVal() != 0) {		                       
			String failMessage = 
				"Find database existence process exited abnormally with exit " +
			    "value: " + checkResult.getExitVal();
			writeLogAndCommitTask(failMessage, Level.SEVERE, LState.FAILED);			
			return false;
		}
		
		if (!checkResult.isValueFound()) {
		   	String failMessage = 
		   		"Can't find the specified database: " + databaseName;
		   	writeLogAndCommitTask(failMessage, Level.SEVERE, LState.FAILED);
			return false;
		}
		
		writeLocalLog(Level.INFO, "The specified database: " + databaseName + " is found");
		
		return true;
	}
	
	/* Check the table existence. */
	protected boolean checkTableExistence(String plcProgram, 
			                              String hiveUser, 
			                              String hivePassword, 
                                          String hiveServerIP, 
                                          String hiveServerPort,
                                          String databaseName, 
                                          String tableName)
	    throws Exception {
		
		FindOutputResult checkResult = 
		    RunnerUtils.checkTables(plcProgram, hiveUser, hivePassword, 
                                    hiveServerIP, hiveServerPort,
                                    databaseName, tableName, this);
		
		if (checkResult.getExitVal() != 0) {
			String failMessage = 
				"Find table existence process exited abnormally with exit " +
			    "value: " + checkResult.getExitVal();
			writeLogAndCommitTask(failMessage, Level.SEVERE, LState.FAILED);
            return false;
		}
		
		if (!checkResult.isValueFound()) {
            String failMessage = "Can't find the specified table: " + 
                                 tableName + " in specified database: " +
                                 databaseName;
            writeLogAndCommitTask(failMessage, Level.SEVERE, LState.FAILED);
            return false;
        }
		
		writeLocalLog(Level.INFO, "The specified table: " + tableName + 
				                  " in specified database: " + databaseName + 
				                  " is found");
		
		return true;
	}
	
	/* Check the partition existence and create the partition if it doesn't exist. */
	protected boolean createPartition(String plcProgram, 
			                          String hiveUser,
			                          String hivePassword,
			                          String hiveServerIP,
			                          String hiveServerPort,
			                          String databaseName,
			                          String tableName,
			                          String partitionName)
	    throws Exception {
	
		if (partitionName != null && partitionName.length() > 2) {
			FindOutputResult checkResult = 
				RunnerUtils.checkPartitions(plcProgram, hiveUser, 
						                    hivePassword, hiveServerIP,
					                        hiveServerPort, databaseName, 
					                        tableName, partitionName, this);
			
			if (checkResult.getExitVal() != 0) {
				String failMessage = 
					"Find parition existence process exited abnormally with " +
				    "exit value: " + checkResult.getExitVal();
				writeLogAndCommitTask(failMessage, Level.SEVERE, LState.FAILED);
		        return false;
			}
			
			if (!checkResult.isValueFound()) {
				/* create the partition. */
				int createResult = 
					RunnerUtils.createPartition(plcProgram, hiveUser, 
							                    hivePassword, hiveServerIP,
						                        hiveServerPort, databaseName, 
						                        tableName, partitionName, this);
				if (createResult != 0) {
					String failMessage = "Create partition: " + partitionName +
							             " for table: " + tableName + " failed";
					writeLogAndCommitTask(failMessage, Level.SEVERE, LState.FAILED);
					return false;
				}
				
				writeLocalLog(Level.INFO, "Create partition: " + partitionName +
						                  " for table: " + tableName + " succeed");
			}
		}
		
		writeLocalLog(Level.INFO, "Partition: " + partitionName + 
				                  " exists or just been created");
		
		return true;
	}
	
	/* Check to see whether there exists source files in the source path. */
	protected boolean checkHdfsEmptySource(LServer hdfsServer, 
			                               String sourceFilePath) 
	    throws Exception {
		
		FileSystem sourceFS = RunnerUtils.getHdfsFileSystem(hdfsServer);
		Path sourcePath = new Path(sourceFilePath);
		FileStatus[] sourceFiles = sourceFS.listStatus(sourcePath);
		if (!sourceFS.exists(sourcePath) || (sourceFiles.length == 0)) {
			String successMessage = 
				"Task succeeded because no files in the source path";
			writeLogAndCommitTask(successMessage, Level.INFO, LState.SUCCESSFUL);
			return true;
		}
		
		writeLocalLog(Level.INFO, "There exists files in the source path, " +
		                          "need to load data");
		
		return false;
	}
	
	protected boolean clearOldData(String loadMode,
			                       String plcProgram,
			                       String hdfsUser,
			                       String hdfsPassword,
			                       String hdfsServerIP,
			                       String hdfsServerPort,
			                       String databaseName,
			                       String tableName,
			                       String partitionName,
			                       LTask task) 
	    throws Exception {
		
		if (TRUNCATE_FLAG.equalsIgnoreCase(loadMode)) {
			/* For truncation, just do truncation. */
			int truncateResult = 
				RunnerUtils.truncateData(plcProgram, hdfsUser, hdfsPassword, 
						                 hdfsServerIP, hdfsServerPort, 
						                 databaseName, tableName, partitionName, this);
			if (truncateResult != 0) {
				String failMessage = 
					"Truncate partition: " + partitionName + " for table: " + 
				    tableName + " failed";
				writeLogAndCommitTask(failMessage, Level.SEVERE, LState.FAILED);				
				return false;
			}
		} else if (APPEND_FLAG.equalsIgnoreCase(loadMode)) {

			/*
			 * TODO: this method should be provided by TDW.
			 * If the runtime id is not null, which means this is a retry or 
			 * redo task, remove the files that we've written last time.
			 */
			if (task.getRuntimeId() != null) {
				/*RunnerUtils.removeHDFSFilesWithPattern(nameNodeUser, nameNodeGroup, nameNodeIP, nameNodePort,
						task.getRuntimeId());*/
			}
		} else {
			String failMessage = 
				"Load data from HDFS to TDW failed, because " + loadMode +
				" is not a supported load mode";
			writeLogAndCommitTask(failMessage, Level.SEVERE, LState.FAILED);			
			return false;
		}
		
		writeLocalLog(Level.INFO, "Old data is cleared successfully");
		
		return true;
	}
	
	protected boolean createExternalTable(String plcProgram, 
			                              String plcParameter,
			                              String hiveUser,
			                              String hivePassword,
			                              String hiveServerIP,
			                              String hiveServerPort,
			                              String nameNodeIP,
			                              String nameNodePort,
			                              String externalTableDataPath,
			                              String externalTableDb,
			                              String externalTableName,
			                              String sourceColumnNames,
			                              String delimiter,
			                              String charSet) 
	    throws Exception {
		
		int createExternalTableResult = 
			RunnerUtils.createExternalTable(plcProgram, plcParameter, hiveUser, hivePassword,
				                            hiveServerIP, hiveServerPort, nameNodeIP, 
				                            nameNodePort, externalTableDataPath, externalTableDb,
				                            externalTableName, sourceColumnNames, delimiter, 
				                            charSet, this);
		if (createExternalTableResult != 0) {
			String failMessage = 
				"Can't create external table: " + externalTableName +
				" and the task fails";
			writeLogAndCommitTask(failMessage, Level.SEVERE, LState.FAILED);	
			return false;
		}
		externalTableCreated = true;
		writeLocalLog(Level.INFO, "Create external table: " + externalTableName + 
				                  " on TDW: " + hiveServerIP + " successfully");
		
		return true;
	}
	
	protected boolean createExternalTableWithSQL(String plcProgram,
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
			                                     String destFileDelimiter)
	    throws Exception {
		
		int createExternalTableResult = 
			RunnerUtils.createExternalTableWithSQL(plcProgram, plcParameter, hiveUser, hivePassword,
				                                   hiveIP, hivePort, nameNodeIP, nameNodePort, 
				                                   externalTableDataPath, databaseName, 
				                                   externalTableDb, externalTableName, filterSQL, 
				                                   destFileDelimiter, this);
		if (createExternalTableResult != 0) {
			String failMessage = 
				"Can't create external table: " + externalTableName +
				" and the task fails";
			writeLogAndCommitTask(failMessage, Level.SEVERE, LState.FAILED);	
			return false;
		}
		
		externalTableCreated = true;
		writeLocalLog(Level.INFO, "Create external table: " + externalTableName + 
	                              " on TDW: " + hiveIP + " successfully");
		
		
		return true;
	}
	
	protected SessionAndExitVal loadData(String plcProgram,
			                             String plcParameter,
			                             String hiveUser,
			                             String hivePassword,
			                             String hiveServerIP,
			                             String hiveServerPort,
			                             String databaseName,
			                             String externalTableName,
			                             String tableName,
			                             String targetColumnNames,
			                             Date runDate,
			                             String whereClause, 
			                             String externalTableDb)
	    throws Exception {
		
		SessionAndExitVal result = 
			RunnerUtils.loadData(plcProgram, plcParameter, hiveUser, hivePassword, 
					             hiveServerIP, hiveServerPort, databaseName, 
					             externalTableName, tableName, targetColumnNames, 
					             runDate, whereClause, externalTableDb, this);
		if (result.getExitVal() != 0) {
			String failMessage = 
				"Load data from external table: " + externalTableName + 
				" to regular table: " + tableName + " failed";
			writeLocalLog(Level.SEVERE, failMessage);
			commitTask(LState.FAILED, result.getSessionId(), failMessage);
		
		    return result;
		}
		writeLocalLog(Level.INFO, "Load data from external table: " + externalTableName +
				                  " to regular table: " + tableName + " successfully");
		
		return result;
	}
	
	protected SessionAndExitVal loadDataWithSQL(String plcProgram,
			                                    String plcParameter,
			                                    String hiveUser,
			                                    String hivePassword,
			                                    String hiveIP,
			                                    String hivePort,
			                                    String databaseName,
			                                    String externalTableDb,
			                                    String externalTableName,
			                                    String filterSQL)
	    throws Exception {
		
		SessionAndExitVal loadDataResult = 
			RunnerUtils.loadDataWithSQL(plcProgram, plcParameter, hiveUser, hivePassword, 
					                    hiveIP, hivePort, databaseName, externalTableDb, 
					                    externalTableName, filterSQL, this);
		if (loadDataResult.getExitVal() != 0) {
			String failMessage = 
				"Load data from external table: " + externalTableName + 
				" to regular table failed";
			writeLocalLog(Level.SEVERE, failMessage);
			commitTask(LState.FAILED, loadDataResult.getSessionId(), failMessage);
		
			return loadDataResult;
		}		
		writeLocalLog(Level.INFO, "Load data from external table: " + externalTableName +
	                              " to regular table successfully");
		
		return loadDataResult;
	}
	
	protected boolean createTmpPath(FileSystem nameNodeFS,
			                        String externalTableDataPath,
			                        String nameNodeIP)
	    throws Exception {
	
		if (!RunnerUtils.createTempHDFSPath(nameNodeFS, externalTableDataPath)) {
			String failMessage = 
				"Create temporary path: " + externalTableDataPath + 
				" on name node: " + nameNodeIP + " failed";
			writeLogAndCommitTask(failMessage, Level.SEVERE, LState.FAILED);			
			return false;			
		}
		tmpPathCreated = true;
		writeLocalLog(Level.INFO, "Create temporary path: " + externalTableDataPath + 
				                  " on HDFS: " + nameNodeIP + " successfully");
		
		return true;
	}
	
	protected boolean mrCopyData(LTask task,
			                     String hadoopCommand,
			                     String sourceIP,
			                     String sourcePort,
			                     String targetIP,
			                     String targetPort,
			                     String targetUser,
			                     String targetGroup,
			                     String sourceFilePath,
			                     String externalTableDataPath,
			                     FileSystem nameNodeFS,
			                     boolean isLoadingData)
	    throws Exception {
		
		LServer jobTracker = (isLoadingData ? task.getTargetServers(2) : task.getSourceServers(2));
		String mrCopyJobTracker = 
			jobTracker.getHost() + ":" + jobTracker.getPort();
		int copyResult = 
			RunnerUtils.hadoopMRCopy(hadoopCommand, sourceIP, sourcePort, 
					                 targetIP, targetPort, targetUser, 
					                 targetGroup, mrCopyJobTracker, 
					                 sourceFilePath, externalTableDataPath, this);
		if (copyResult != 0) {
			String failMessage = "Hadoop distcp data to TDW failed";
			writeLogAndCommitTask(failMessage, Level.SEVERE, LState.FAILED);			
			return false;
		}
		writeLocalLog(Level.INFO, "Hadoop distcp files successfully");

		/* Remove those useless distcp_logs files. */
		RunnerUtils.removeHadoopCopyLogFiles(nameNodeFS, externalTableDataPath);
		writeLocalLog(Level.INFO, "Remove distcp log files successfully");
		
		return true;
	}
	
	/* Drop the external table and delete the temporary path for external table. */
	protected void doCleanWork(String plcProgram, 
			                   String hiveUser,
			                   String hivePassword,
			                   String hiveServerIP,
			                   String hiveServerPort,
			                   String externalTableDb,
			                   String externalTableName,
			                   FileSystem hdfs,
			                   String externalTableDataPath) 
	    throws IOException {
		
		/* Drop the external table. */
		if (externalTableCreated) {
			try {
				RunnerUtils.dropTable(plcProgram, hiveUser, hivePassword, hiveServerIP, 
						              hiveServerPort, externalTableDb, externalTableName, this);
				writeLocalLog(Level.INFO, "Drop external table: " + externalTableName + 
						                  " on TDW: " + hiveServerIP + " successfully");
			} catch (Exception e) {
				writeLocalLog(Level.WARNING, "Drop external table: " + externalTableName +
						                     " on TDW: " + hiveServerIP + 
						                     " failed with message: " + e.getMessage());
			}
		}

		/* Need to delete those files in the temporary path for external table. */
		if (tmpPathCreated && (hdfs != null)) {
			RunnerUtils.deleteHdfsFile(hdfs, new Path(externalTableDataPath));
			writeLocalLog(Level.INFO, "Delete the temporary path: " + 
			                          externalTableDataPath + " successfully");
		}            
	}
}
