package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.util.CommonUtils;
import com.tencent.isd.lhotse.runner.util.RegFileNameFilter;
import com.tencent.isd.lhotse.runner.util.RunnerUtils;
import com.tencent.isd.lhotse.runner.util.RunnerUtils.*;

import java.io.*;
import java.util.*;
import java.util.logging.Level;

public class TdbankReadCheckFileRunner extends DDCTaskRunner  {	
	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(ReadCheckFileRunner.class, (byte) 108);
	}

	@Override
	public void execute() throws 
	    IOException {
		
		boolean success = false;
		Map<String, String> keyValues = new HashMap<String, String>();
		
		/* Calculate the real data date. */
		LTask task = getTask();
		Date runDate = getRealDataDate(task);
		
		try {
			/* Check the source file system type. */
			LServer sourceFileServer = null;
			if (task.getSourceServersCount() != 0) {
				sourceFileServer = task.getSourceServers(0);
			}			
			writeLocalLog(Level.INFO, "server size: " + task.getSourceServersCount());
						
			String sourceFileSystemType = this.getExtPropValue("sourceFileSystemType");
			String sourceFilePath = null;
			String sourceFileName = null;
			String checkFilePath = null;
			String checkFileName = null;
			String checkLevel = null;
			String subInterfaceList = this.getExtPropValue("subInterfaceList");
			String[] subList = null;
			ArrayList<HashMap<String,String>> checkPathList = new ArrayList<HashMap<String,String>>();
			try {
			    sourceFilePath = 
				    RunnerUtils.formatStrDateParam(this.getExtPropValue("sourceFilePath"), runDate);
			    sourceFileName = 
				    RunnerUtils.formatStrDateParam(this.getExtPropValue("sourceFileNames"), runDate);
			    checkFilePath = 
				    RunnerUtils.formatStrDateParam(this.getExtPropValue("checkFilePath"), runDate);
			    checkFileName = 
				    RunnerUtils.formatStrDateParam(this.getExtPropValue("checkFileName"), runDate);
			    checkLevel =
				    RunnerUtils.formatStrDateParam(this.getExtPropValue("checkLevel"), runDate);
			    if (checkLevel == null) {
			    	checkLevel = "1";
			    }
			} catch (Exception e) {
				keyValues.put("exit_code", RunnerUtils.DATE_FORMAT_ERROR_CODE);
	        	keyValues.put("task_desc", "Task failed becuase date format is not correct: " + 
	        	                           CommonUtils.stackTraceToString(e));
	        	commitJsonResult(keyValues, false, "");
	        	return;
			}
			
			if(subInterfaceList !=null && subInterfaceList.trim().length() > 0){
				subList = subInterfaceList.trim().split(",");
			}
			
			if(subList !=null && subList.length > 0){
				for(String subStr : subList){
					HashMap<String,String> tmpMap = new HashMap<String,String>();
					tmpMap.put("sourceFilePath", sourceFilePath.replaceAll("\\$\\{#1\\}", subStr));
					tmpMap.put("checkFilePath", checkFilePath.replaceAll("\\$\\{#1\\}", subStr));
					checkPathList.add(tmpMap);
				}
			} else {
				HashMap<String,String> tmpMapOne = new HashMap<String,String>();
				tmpMapOne.put("sourceFilePath", sourceFilePath);
				tmpMapOne.put("checkFilePath", checkFilePath);
				checkPathList.add(tmpMapOne);
			}
			
			CheckResult checkResult = null;
			
			for (HashMap<String, String> psthMap : checkPathList) {
				if (HDFS_SYSTEM.equalsIgnoreCase(sourceFileSystemType)) {
					checkResult = checkHdfsSource(sourceFileServer, 
							                      /*sourceFilePath*/psthMap.get("sourceFilePath"), 
							                      sourceFileName, 
							                      /*checkFilePath*/psthMap.get("checkFilePath"),
							                      checkFileName, 
							                      checkLevel);
				}
				else if (LINUX_SYSTEM.equalsIgnoreCase(sourceFileSystemType)) {
					checkResult = checkLinuxSource(sourceFileServer, 
							                       /*sourceFilePath*/psthMap.get("sourceFilePath"), 
							                       sourceFileName, 
							                       /*checkFilePath*/psthMap.get("checkFilePath"),
							                       checkFileName, 
							                       checkLevel);
				}
				else {
					throw new UnsupportedOperationException("Not supported source file type:" + sourceFileSystemType);
				}

				if (checkResult.getCheckResult()) {
					keyValues.put("exit_code", "0");
					keyValues.put("task_desc", "Check file statuses succeed");
					keyValues.put("run_date", HOUR_FORMAT.format(runDate));
					success = true;
				}
				else {
					keyValues.put("exit_code", checkResult.getErrorCode());
					keyValues.put("task_desc", checkResult.getFailMessage());
					keyValues.put("run_date", HOUR_FORMAT.format(runDate));
					break;
				}
			}
		} catch (Exception e) {
        	keyValues.put("exit_code", RunnerUtils.UNKOWN_ERROR_CODE);
        	keyValues.put("task_desc", "Check fail tasks failed: " + 
                                       CommonUtils.stackTraceToString(e));
        	keyValues.put("run_date", HOUR_FORMAT.format(runDate));
			throw new IOException(e);
		} finally {
			try {
				commitJsonResult(keyValues, success, "");
			} catch (Exception e) {
				this.writeLocalLog(Level.SEVERE, CommonUtils.stackTraceToString(e));
				throw new IOException(e);
			}
		}
	}
	
	private CheckResult checkHdfsSource(LServer sourceServer, 
			                            String sourceFilePath,
			                            String sourceFileNamePattern,
			                            String checkFilePath,
			                            String checkFileName,
			                            String checkLevel) 
	    throws Exception {
		
		final String hadoopCommand = getHadoopCommandOnVersion(sourceServer);
		
		CheckResult checkResult = new CheckResult();
		
		if (sourceServer == null) {
			String failMessage = 
				"No hdfs server specified when check file on HDFS";
			this.writeLocalLog(Level.INFO, failMessage);
			checkResult.setFailMessage(failMessage);
			checkResult.setErrorCode(RunnerUtils.SERVER_CONFIG_ERROR_CODE);
			return checkResult;
		}
		
		/* Check the existence of the check file path. */
		HdfsDirExistResult result = 
			RunnerUtils.isHdfsDirExist(hadoopCommand, sourceServer, checkFilePath, this);		
		/* HADOOP command failed. */
		if (result.getExitVal() != 0 && (result.getHadoopReturnCode() != RunnerUtils.UNKOWN_ERROR_CODE)) {
			String failMessage =
		    	"The Hadoop command fails because of the error code: " + result.getHadoopReturnCode();
			this.writeLocalLog(Level.SEVERE, failMessage);
		    checkResult.setFailMessage(failMessage);
		    checkResult.setErrorCode(result.getHadoopReturnCode());
	        return checkResult;
		}
		
		/* Directory doesn't exist. */
		if (!result.isDirExists()) {
			String failMessage =
			    "Check file path: " + checkFilePath + " on HDFS cluster: " + 
			    sourceServer.getHost() + " doesn't exist";
			this.writeLocalLog(Level.SEVERE, failMessage);
			checkResult.setFailMessage(failMessage);
			checkResult.setErrorCode(RunnerUtils.CHECK_FILE_NOT_EXIST_CODE);
		    return checkResult;
		}
			
		HdfsDirFileCounter fileCounter = 
			RunnerUtils.getHdfsDirFileCount(hadoopCommand, sourceServer, 
					                        checkFilePath, checkFileName, this);
		/* Return false if the process for getting file information fails. */
		if (fileCounter.getExitVal() != 0) {
		   	String failMessage = 
		   		"Check file: " + checkFileName + " doesn't exist on " +
		   	    "HDFS cluster: " + sourceServer.getHost();
		   	this.writeLocalLog(Level.SEVERE, failMessage);
			checkResult.setFailMessage(failMessage);
			checkResult.setErrorCode(RunnerUtils.CHECK_FILE_NOT_EXIST_CODE);
		    return checkResult;
		}
		    
        /* Return false if there is no check file in the path. */
        if (fileCounter.getFileCount() == 0) {
           	String failMessage = 
           		"Check file path: " + checkFilePath + " exists on HDFS cluster: " +
           	    sourceServer.getHost() + ", but check file: " + checkFileName +
           	    " doesn't exist";
           	this.writeLocalLog(Level.SEVERE, failMessage);
            checkResult.setFailMessage(failMessage);
            checkResult.setErrorCode(RunnerUtils.CHECK_FILE_NOT_EXIST_CODE);
            return checkResult;
        } 

        /* Get the source file and check file statuses. */                       
        CheckFileLevel fileLevel = 
           	CheckFileLevel.getCheckFileLevel(checkLevel);
        
        /* For the case that we only need the check file exists. */
        if (fileLevel == CheckFileLevel.FIRST_LEVEL) {
           	checkResult.setCheckResult(true);
           	return checkResult;
        }
 
        ListHdfsDirResult checkDirResult =
    	    RunnerUtils.getHdfsFileDescriptors(hadoopCommand, sourceServer, 
						                       checkFilePath, checkFileName, this);
        CatFileResult catResult = 
           	RunnerUtils.catHdfsFile(hadoopCommand, sourceServer, 
           			                checkFilePath, checkDirResult.getDescriptors().get(0).getFileName(), this);
            
        if (catResult.getExitVal() != 0) {
        	String failMessage = 
        	    "Check file: " + checkFileName + " doesn't exist on " +
        		"HDFS cluster: " + sourceServer.getHost();
    		this.writeLocalLog(Level.SEVERE, failMessage);
    		checkResult.setFailMessage(failMessage);
    		checkResult.setErrorCode(RunnerUtils.CHECK_FILE_NOT_EXIST_CODE);
    		return checkResult;
        }             
            
        CheckContent checkContent = 
        	readCheckFileContent(fileLevel, catResult.getContents(), checkResult);
            
        /* Return the error message if can't read the check file content. */
        if (checkContent == null) {
        	checkResult.setErrorCode(RunnerUtils.CHECK_FILE_NOT_EXIST_CODE);
           	return checkResult;
        }
            
        writeLocalLog(Level.INFO, "expected file number: " + checkContent.getExpectTotalFileNumber() +
                                   ", expected file size: " + checkContent.getExpectTotalFileSize());
            
        /* Do nothing if the expected file number is 0, return true. */
        if (checkContent.getExpectTotalFileNumber() == 0) {
          	checkResult.setCheckResult(true);
           	return checkResult;
        }            
            
        /* Return false if the source file path does not exist, but it requires source files exist. */
        result = 
        	RunnerUtils.isHdfsDirExist(hadoopCommand, sourceServer, sourceFilePath, this);
        if (result.getExitVal() != 0 && (result.getHadoopReturnCode() != RunnerUtils.UNKOWN_ERROR_CODE)) {
        	String failMessage =
    		    "The Hadoop command fails because of the error code: " + result.getHadoopReturnCode();
    		this.writeLocalLog(Level.SEVERE, failMessage);
    		checkResult.setFailMessage(failMessage);
    		checkResult.setErrorCode(result.getHadoopReturnCode());
    	    return checkResult;
        }
        
		if (!result.isDirExists()) {
			String failMessage =
		    	"Source file path: " + sourceFilePath + " on HDFS cluster: " + 
		    	sourceServer.getHost() + " doesn't exist";
		    this.writeLocalLog(Level.SEVERE, failMessage);
		    checkResult.setFailMessage(failMessage);
		    checkResult.setErrorCode(RunnerUtils.CHECK_FILE_NOT_EXIST_CODE);
	        return checkResult;
		}
		
		/* Get the source files' status. */
		ListHdfsDirResult dirResult =
	        RunnerUtils.getHdfsFileDescriptors(hadoopCommand, sourceServer, 
	              		                       sourceFilePath, sourceFileNamePattern, this);
			    
		/* Return false if the process for getting file information fails. */
		if (dirResult.getExitVal() != 0) {
			String failMessage = 
				"The process of listing check file status failed with error code: " +
			    dirResult.getHadoopReturnCode();
			this.writeLocalLog(Level.SEVERE, failMessage);
			checkResult.setFailMessage(failMessage);
			checkResult.setErrorCode(dirResult.getHadoopReturnCode());
			return checkResult;
		}            
            
        ArrayList<NameAndSize> sourceContents = new ArrayList<NameAndSize>();
        for (HdfsFileDescriptor descriptor : dirResult.getDescriptors()) {
            sourceContents.add
                (new NameAndSize(descriptor.getFileName(),
            		             (fileLevel == CheckFileLevel.FOURTH_LEVEL ? 
            		              descriptor.getFileSize() : 0)));
        }
            
        doRealCheck(sourceContents, checkContent, checkResult, fileLevel);
        
        /* Clear the objects in the ArrayList. */
        checkDirResult.getDescriptors().clear();
        checkDirResult = null;
        dirResult.getDescriptors().clear();
        dirResult = null;
        sourceContents.clear();
        sourceContents = null;
		
		return checkResult;
	}
	
	private CheckResult checkLinuxSource(LServer sourceServer,
                                         String sourceFilePath,
                                         String sourceFileNamePattern,
                                         String checkFilePath,
                                         String checkFileNamePattern,
                                         String checkLevel) 
        throws Exception {

        CheckResult checkResult = new CheckResult();

        BufferedReader reader = null;
        try {
            File checkPath = new File(checkFilePath);
            if (!checkPath.exists()) {
            	String failMessage =
    		    	"Check file path: " + checkFilePath + " on Linux machine: " + 
    		    	sourceServer.getHost() + " doesn't exist";
            	this.writeLocalLog(Level.INFO, failMessage);
            	checkResult.setFailMessage(failMessage);
            	checkResult.setErrorCode(RunnerUtils.CHECK_FILE_NOT_EXIST_CODE);
            	return checkResult;
            }
            
            File[] checkFiles = 
            	checkPath.listFiles(new RegFileNameFilter(checkFileNamePattern));

            /* Check the existence of the check file. */
            if (checkFiles.length == 0) {
            	String failMessage = 
                	"Check file path: " + checkFilePath + " exists on Linux machine: " +
                	sourceServer.getHost() + ", but check file: " + checkFileNamePattern +
                	" doesn't exist";
            	this.writeLocalLog(Level.INFO, failMessage);
                checkResult.setFailMessage(failMessage);
                checkResult.setErrorCode(RunnerUtils.CHECK_FILE_NOT_EXIST_CODE);
                return checkResult;
            }
            
            if (checkFiles.length > 1) {
            	String failMessage =
                	"Find " + checkFiles.length + " check file exists " +
                	"in path: " + checkFilePath + " on Linux machine: " + 
                	sourceServer.getHost();
            	this.writeLocalLog(Level.INFO, failMessage);
            	checkResult.setFailMessage(failMessage);
            	checkResult.setErrorCode(RunnerUtils.CHECK_FILE_NOT_EXIST_CODE);
            	return checkResult;
            }

            /* Get the source and check file status. */
            reader = 
                new BufferedReader(new InputStreamReader(new FileInputStream(checkFiles[0])));
            CheckFileLevel fileLevel = 
                CheckFileLevel.getCheckFileLevel(checkLevel);
            
            /* For the case that we only need the check file exists. */
            if (fileLevel == CheckFileLevel.FIRST_LEVEL) {
            	checkResult.setCheckResult(true);
            	return checkResult;
            }
            
            ArrayList<String> fileContents = new ArrayList<String>();
            String fileContent = null;
            while ((fileContent = reader.readLine()) != null) {
            	fileContents.add(fileContent);
            }
                        
            CheckContent checkContent = 
            	readCheckFileContent(fileLevel, fileContents, checkResult);                  
            
            /* Return the error message if can't read the check file content. */
            if (checkContent == null) {
            	checkResult.setErrorCode(RunnerUtils.CHECK_FILE_NOT_EXIST_CODE);
            	return checkResult;
            }            

            writeLocalLog(Level.INFO, "expected file number: " + checkContent.getExpectTotalFileNumber() +
	                                  ", expected file size: " + checkContent.getExpectTotalFileSize());
            
            /* Do nothing if the expected file number is 0, return true. */
            if (checkContent.getExpectTotalFileNumber() == 0) {
            	checkResult.setCheckResult(true);
            	return checkResult;
            }            
            
            /* Return false if the source file path does not exist, but it requires source files exist. */
            File sourcePath = new File(sourceFilePath);
            if (!sourcePath.exists()) {
            	checkResult.setCheckResult(false);
            	checkResult.setErrorCode(RunnerUtils.CHECK_FILE_NOT_EXIST_CODE);
            	checkResult.setFailMessage("Expect: " + checkContent.getExpectTotalFileNumber() +
            			                   " source files, but the data directory doesn't exist.");
            	return checkResult;
            }            
            
            File[] sourceFiles = 
            	new File(sourceFilePath).listFiles(new RegFileNameFilter(sourceFileNamePattern));
            ArrayList<NameAndSize> sourceContents = new ArrayList<NameAndSize>();
            for (File sourceFile : sourceFiles) {
                sourceContents.add
                    (new NameAndSize(sourceFile.getName(),
                                      (fileLevel == CheckFileLevel.FOURTH_LEVEL ? 
                                       sourceFile.length() : 0)));
            }            
            
            doRealCheck(sourceContents, checkContent, checkResult, fileLevel);
        } finally {
            if (reader != null) {
                reader.close();
            }
        }        

        return checkResult;
    }
	
	private CheckContent readCheckFileContent(CheckFileLevel fileLevel,
                                              ArrayList<String> fileContents,
                                              CheckResult checkResult)
        throws IOException {

        CheckContent checkContent = null;

        switch (fileLevel) {
            case SECOND_LEVEL:
                checkContent = checkSecondLevel(fileContents, checkResult);
                break;
            case THIRD_LEVEL:
                checkContent = checkThirdLevel(fileContents, checkResult);
                break;
            case FOURTH_LEVEL:
                checkContent = checkFourthLevel(fileContents, checkResult);
                break;
            default:
                String failMessage = 
                    "The check level: " + fileLevel + " of this task is illegal";
                checkResult.setCheckResult(false);
                checkResult.setFailMessage(failMessage);
                checkResult.setErrorCode(RunnerUtils.CHECK_FILE_NOT_EXIST_CODE);
                this.writeLocalLog(Level.INFO, failMessage);
                break;
        }

        return checkContent;
    }
	
	/* Check the second level check file. */
	private CheckContent checkSecondLevel(ArrayList<String> fileContents, 
			                              CheckResult checkResult) 
	    throws IOException {
		
		String firstLine = fileContents.get(0);
		int expectTotalFileNumber = 0;
		writeLocalLog(Level.INFO, "firstLine == null ? " + (firstLine == null));
    	if (firstLine != null) {
    		StringTokenizer st = new StringTokenizer(firstLine, "\t");
    		String number = st.nextToken();
    		writeLocalLog(Level.INFO, "st counts: " + st.countTokens() + ", number string: " + number);
    		try {
    		    expectTotalFileNumber = new Integer(number);
    		    writeLocalLog(Level.INFO, "expectedTotalFileNumber: " + expectTotalFileNumber);
    		} catch (NumberFormatException e) {
    			checkResult.setFailMessage
    			    ("The first line of check file is not a number");
    			writeLocalLog(Level.INFO, e.getMessage());
    		}
    	}
    	
    	return new CheckContent(new ArrayList<NameAndSize>(), expectTotalFileNumber, 0);
	}
	
	/* Check the third level check file. */
	private CheckContent checkThirdLevel(ArrayList<String> fileContents,
			                             CheckResult checkResult)
	    throws IOException {
		
		int counter = 0;
		int expectTotalFileNumber = 0;
		ArrayList<NameAndSize> nameAndSizes = new ArrayList<NameAndSize>();
		for (String readLine : fileContents) {
			StringTokenizer st = new StringTokenizer(readLine, "\t");
			
			if (st.countTokens() < 1) {
				String failMessage = "Check file format is not valid";
				checkResult.setFailMessage(failMessage);
				this.writeLocalLog(Level.INFO, failMessage);
				return null;
			}
			
			if (counter == 0) {
				expectTotalFileNumber = new Integer(st.nextToken());
				continue;
			}
			
			nameAndSizes.add(new NameAndSize(st.nextToken(), 0));
			counter++;
		}
		
		return new CheckContent(nameAndSizes, expectTotalFileNumber, 0);
	}
	
	/* Check the fourth level check file. */
	private CheckContent checkFourthLevel(ArrayList<String> fileContents,
			                              CheckResult checkResult) 
	    throws IOException {
		
		int counter = 0;
		int expectTotalFileNumber = 0;
		long expectTotalFileSize = 0;
		ArrayList<NameAndSize> nameAndSizes = new ArrayList<NameAndSize>();
		
		for (String readLine : fileContents) {
			StringTokenizer st = new StringTokenizer(readLine, "\t");
			
			if (st.countTokens() != 2) {
				String failMessage = "Check file format is not valid";
				checkResult.setCheckResult(false);
				checkResult.setFailMessage(failMessage);
				this.writeLocalLog(Level.INFO, failMessage);
				return null;
			}
			
			if (counter == 0) {
				expectTotalFileNumber = new Integer(st.nextToken());
				expectTotalFileSize = new Long(st.nextToken());
				continue;
			}
			
			nameAndSizes.add(new NameAndSize(st.nextToken(), new Long(st.nextToken())));
			counter++;
		}
		
		return new CheckContent(nameAndSizes, expectTotalFileNumber, expectTotalFileSize);
	}
	
	/* Check the total file size, total file number and the contents. */
	private void doRealCheck(ArrayList<NameAndSize> sourceContents,
			                 CheckContent checkContent,
			                 CheckResult checkResult,
			                 CheckFileLevel fileLevel) {
        /* Check the total file number equivalence. */
        if (checkContent.getExpectTotalFileNumber() > sourceContents.size()) {
        	String failMessage =
        		"Expect total file number: " + checkContent.getExpectTotalFileNumber() +
	            ", but actual total file number: " + sourceContents.size();
        	checkResult.setCheckResult(false);
        	checkResult.setFailMessage(failMessage);
        	checkResult.setErrorCode(RunnerUtils.CHECK_FILE_NOT_EXIST_CODE);
        	this.writeLocalLog(Level.INFO, failMessage);
        	return;
        }
        
        if (fileLevel == CheckFileLevel.SECOND_LEVEL) {
        	checkResult.setCheckResult(true);
        	return;
        }
        
        /* Check both the file name and size for each source file. */        
        for (NameAndSize sourceContent : sourceContents) {
            if (!checkContent.getCheckFileContents().contains(sourceContent)) {
            	String failMessage =
            		"source file: " + sourceContent.getFileName() +
		            " information is not correctly included in the check file";
                checkResult.setCheckResult(false);                
                checkResult.setFailMessage(failMessage);
                checkResult.setErrorCode(RunnerUtils.CHECK_FILE_NOT_EXIST_CODE);
                this.writeLocalLog(Level.INFO, failMessage);
                return;
            }        	
        }
        
        if (fileLevel == CheckFileLevel.THIRD_LEVEL) {
        	checkResult.setCheckResult(true);
        	return;
        }
        
        /* Calculate the total source file size. */
        long actualTotalFileSize = 0;
        for (NameAndSize sourceContent : sourceContents) {
        	actualTotalFileSize += sourceContent.getFileSize();
        }
        
        /* Check the total file size equivalence. */
        if (actualTotalFileSize != checkContent.getExpectTotalFileSize()) {
        	String failMessage =
        		"Expect total file size: " + checkContent.getExpectTotalFileSize() +
	            ", but actual total file size: " + actualTotalFileSize;
        	checkResult.setCheckResult(false);
        	checkResult.setFailMessage(failMessage);
        	checkResult.setErrorCode(RunnerUtils.CHECK_FILE_NOT_EXIST_CODE);
        	this.writeLocalLog(Level.INFO, failMessage);
        	return;
        }        
        
        checkResult.setCheckResult(true);
    }

	@Override
	public void kill() throws IOException {		
	}
	
	/* The below classes are POJO, just for recording the returned value. */
	private class CheckResult {
		private boolean checkResult = false;
		private String errorCode;
		private String failMessage;
		
		public void setCheckResult(boolean checkResult) {
			this.checkResult = checkResult;
		}
		
		public void setErrorCode(String errorCode) {
			this.errorCode = errorCode;
		}
		
		public String getErrorCode() {
			return errorCode;
		}
		
		public void setFailMessage(String failMessage) {
			this.failMessage = failMessage;
		}
		
		public boolean getCheckResult() {
			return checkResult;
		}
		
		public String getFailMessage() {
			return failMessage;
		}
	}	
	
	private class NameAndSize {
		private final String fileName;
		private final long fileSize;
		
		public NameAndSize(String fileName,long fileSize) {
			this.fileName = fileName;
			this.fileSize = fileSize;
		}
		
		public String getFileName() {
			return fileName;
		}
		
		public long getFileSize() {
			return fileSize;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			}
			
			if (!(obj instanceof NameAndSize)) {
				return false;
			}
			
			NameAndSize NameAndSize = (NameAndSize) obj;
			
			/* Return true only when both file name and size are the same. */
			return fileName.equals(NameAndSize.getFileName()) &&
				   (fileSize == NameAndSize.getFileSize());
		}
		
		@Override
		public int hashCode() {
			return new Long(fileSize).intValue();
		}
	}
	
	private class CheckContent {
		private final ArrayList<NameAndSize> checkFileContents;
		private final int expectTotalFileNumber;
		private final long expectTotalFileSize;
		
		public CheckContent(ArrayList<NameAndSize> checkFileContents,
				            int expectTotalFileNumber,
				            long expectTotalFileSize) {
			this.checkFileContents = checkFileContents;
			this.expectTotalFileNumber = expectTotalFileNumber;
			this.expectTotalFileSize = expectTotalFileSize;
		}
		
		public ArrayList<NameAndSize> getCheckFileContents() {
			return checkFileContents;
		}
		
		public int getExpectTotalFileNumber() {
			return expectTotalFileNumber;
		}
		
		public long getExpectTotalFileSize() {
			return expectTotalFileSize;
		}
	}
	
	public enum CheckFileLevel {
		FIRST_LEVEL,
		SECOND_LEVEL,
		THIRD_LEVEL,
		FOURTH_LEVEL;
		
		public static CheckFileLevel getCheckFileLevel(String checkLevel) {
			if ("1".equals(checkLevel)) {
				return FIRST_LEVEL;
			} else if ("2".equals(checkLevel)) {
				return SECOND_LEVEL;
			} else if ("3".equals(checkLevel)) {
				return THIRD_LEVEL;
			} else if ("4".equals(checkLevel)) {
				return FOURTH_LEVEL;
			}
			
			throw new IllegalArgumentException("checkLevel: " + checkLevel + " is not supported");
		}
	}
}
