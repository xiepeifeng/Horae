package com.tencent.isd.lhotse.runner.api;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.api.util.HDFSRegExprFilter;
import com.tencent.isd.lhotse.runner.api.util.RegFileNameFilter;
import com.tencent.isd.lhotse.runner.api.util.RunnerUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.logging.Level;

public class ReadCheckFileRunner extends DDCTaskRunner {	
	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(ReadCheckFileRunner.class, (byte) 84);
	}

	@Override
	public void execute() throws 
	    IOException {
		
		try {
			LTask task = getTask();
			
			/* Check the source file system type. */
			LServer sourceFileServer = null;
			if (task.getSourceServersCount() != 0) {
				sourceFileServer = task.getSourceServers(0);
			}			
			writeLocalLog(Level.INFO, "server size: " + task.getSourceServersCount());
			
			/* Calculate the real data date. */			
			Date runDate = getRealDataDate(task);
			
			String sourceFileSystemType = this.getExtPropValue("sourceFileSystemType");
			String sourceFilePath = 
				RunnerUtils.replaceDateExpr(this.getExtPropValue("sourceFilePath"), runDate);
			String sourceFileName = 
				RunnerUtils.replaceDateExpr(this.getExtPropValue("sourceFileNames"), runDate);
			String checkFilePath = 
				RunnerUtils.replaceDateExpr(this.getExtPropValue("checkFilePath"), runDate);
			String checkFileName = 
				RunnerUtils.replaceDateExpr(this.getExtPropValue("checkFileName"), runDate);
			String checkLevel =
				RunnerUtils.replaceDateExpr(this.getExtPropValue("checkLevel"), runDate);
						
			CheckResult checkResult = null;
			if (HDFS_SYSTEM.equalsIgnoreCase(sourceFileSystemType)) {
				checkResult = checkHdfsSource(sourceFileServer, sourceFilePath, sourceFileName, 
				                              checkFilePath, checkFileName, checkLevel);
			} else if (LINUX_SYSTEM.equalsIgnoreCase(sourceFileSystemType)) {
				checkResult = checkLinuxSource(sourceFileServer, sourceFilePath, sourceFileName,
				                               checkFilePath, checkFileName, checkLevel);
			} else {
				throw new UnsupportedOperationException("Not supported source file type:" + sourceFileSystemType);
			}
			
			if (checkResult.getCheckResult()) {
				writeLogAndCommitTask("Check the file statuses succeed", Level.INFO, LState.SUCCESSFUL);
			} else {
				writeLogAndCommitTask(checkResult.getFailMessage(), Level.SEVERE, LState.FAILED);
			}
		} catch (Exception e) {
			e.printStackTrace();
			writeLocalLog(Level.SEVERE, e.getMessage());
			commitTask(LState.FAILED, "", "Check the file statuses failed.");
			throw new IOException(e);
		}
	}
	
	private CheckResult checkHdfsSource(LServer sourceServer, 
			                            String sourceFilePath,
			                            String sourceFileNamePattern,
			                            String checkFilePath,
			                            String checkFileNamePattern,
			                            String checkLevel) 
	    throws Exception {
		
		CheckResult checkResult = new CheckResult();
		
		if (sourceServer == null) {
			String failMessage = 
				"No hdfs server specified when check file on HDFS";
			this.writeLocalLog(Level.INFO, failMessage);
			checkResult.setFailMessage(failMessage);			
			return checkResult;
		}
		
		FileSystem hdfs = null;
		BufferedReader reader = null;
		try {
		    hdfs = RunnerUtils.getHdfsFileSystem(sourceServer);
		
		    /* Check the existence of the check file path. */
		    Path checkFileDir = new Path(checkFilePath);
		    if (!hdfs.exists(checkFileDir)) {
		    	String failMessage =
		    		"Check file path: " + checkFilePath + " on HDFS cluster: " + 
		    	     sourceServer.getHost() + " doesn't exist";
		    	this.writeLocalLog(Level.INFO, failMessage);
		    	checkResult.setFailMessage(failMessage);
            	return checkResult;
		    }
		    
		    /* Get all the files' status in the check file path. */
            FileStatus[] checkFileStatuses = 
        	    hdfs.listStatus(new Path(checkFilePath), 
        	    		        new HDFSRegExprFilter(checkFileNamePattern, this));
        
            /* Return false if there is no check file in the path. */
            if (checkFileStatuses.length == 0) {
            	String failMessage = 
            		"Check file path: " + checkFilePath + " exists on HDFS cluster: " +
            	    sourceServer.getHost() + ", but check file: " + checkFileNamePattern +
            	    " doesn't exist";
            	this.writeLocalLog(Level.INFO, failMessage);
                checkResult.setFailMessage(failMessage);
                return checkResult;
            }
            
            if (checkFileStatuses.length > 1) {
            	String failMessage =
            		"Find " + checkFileStatuses.length + " check file exists " +
            	    "in path: " + checkFilePath + " on HDFS cluster: " + 
            		sourceServer.getHost();
                writeLocalLog(Level.INFO, failMessage);
            	checkResult.setFailMessage(failMessage);
            	return checkResult;
            }    

            /* Get the source file and check file statuses. */                       
            CheckFileLevel fileLevel = 
            	CheckFileLevel.getCheckFileLevel(checkLevel);
            FSDataInputStream fis = hdfs.open(checkFileStatuses[0].getPath());
            reader = new BufferedReader(new InputStreamReader(fis));
            
            /* For the case that we only need the check file exists. */
            if (fileLevel == CheckFileLevel.FIRST_LEVEL) {
            	checkResult.setCheckResult(true);
            	return checkResult;
            }
            
            CheckContent checkContent = 
            	readCheckFileContent(fileLevel, reader, checkResult);
            
            /* Return the error message if can't read the check file content. */
            if (checkContent == null) {
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
            Path sourcePath = new Path(sourceFilePath);
            if (!hdfs.exists(sourcePath)) {
            	checkResult.setCheckResult(false);
            	checkResult.setFailMessage("Expect: " + checkContent.getExpectTotalFileNumber() +
            			                   " source files, but the data directory doesn't exist.");
            	return checkResult;
            }            
            
            FileStatus[] sourceFiles =
                	hdfs.listStatus(sourcePath, new HDFSRegExprFilter(sourceFileNamePattern, this)); 
            ArrayList<NameAndSize> sourceContents = new ArrayList<NameAndSize>();
            for (FileStatus sourceFile : sourceFiles) {
            	sourceContents.add
            	    (new NameAndSize(sourceFile.getPath().getName(),
            		                  (fileLevel == CheckFileLevel.FOURTH_LEVEL ? 
            		                   sourceFile.getLen() : 0)));
            }
            
            doRealCheck(sourceContents, checkContent, checkResult, fileLevel);   
        } finally {
        	if (reader != null) {
        		reader.close();
        	}
        }
		
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
                return checkResult;
            }
            
            if (checkFiles.length > 1) {
            	String failMessage =
                	"Find " + checkFiles.length + " check file exists " +
                	"in path: " + checkFilePath + " on Linux machine: " + 
                	sourceServer.getHost();
            	this.writeLocalLog(Level.INFO, failMessage);
            	checkResult.setFailMessage(failMessage);
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
            
            CheckContent checkContent = 
            	readCheckFileContent(fileLevel, reader, checkResult);                  
            
            /* Return the error message if can't read the check file content. */
            if (checkContent == null) {
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
			                                  BufferedReader reader,
			                                  CheckResult checkResult)
	    throws IOException {
	
		CheckContent checkContent = null;
		
		switch (fileLevel) {
		    case SECOND_LEVEL:
		    	checkContent = checkSecondLevel(reader, checkResult);
		    	break;
		    case THIRD_LEVEL:
		    	checkContent = checkThirdLevel(reader, checkResult);
		    	break;
		    case FOURTH_LEVEL:
		    	checkContent = checkFourthLevel(reader, checkResult);
		    	break;
		    default:
		    	String failMessage = 
		    	    "The check level: " + fileLevel + " of this task is illegal";
		    	checkResult.setCheckResult(false);
		    	checkResult.setFailMessage(failMessage);
		    	this.writeLocalLog(Level.INFO, failMessage);
		    	break;
		}
		
		return checkContent;
	}
	
	/* Check the second level check file. */
	private CheckContent checkSecondLevel(BufferedReader reader, 
			                              CheckResult checkResult) 
	    throws IOException {
		
		String firstLine = reader.readLine();
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
	private CheckContent checkThirdLevel(BufferedReader reader,
			                             CheckResult checkResult)
	    throws IOException {
		
		String readLine = null;
		int counter = 0;
		int expectTotalFileNumber = 0;
		ArrayList<NameAndSize> nameAndSizes = new ArrayList<NameAndSize>();
		while ((readLine = reader.readLine()) != null) {
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
	private CheckContent checkFourthLevel(BufferedReader reader,
			                               CheckResult checkResult) 
	    throws IOException {
		
		int counter = 0;
		int expectTotalFileNumber = 0;
		long expectTotalFileSize = 0;
		ArrayList<NameAndSize> nameAndSizes = new ArrayList<NameAndSize>();
		
		String readLine = null;
		while ((readLine = reader.readLine()) != null) {
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
		private String failMessage;
		
		public void setCheckResult(boolean checkResult) {
			this.checkResult = checkResult;
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