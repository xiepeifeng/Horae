package com.tencent.isd.lhotse.runner.util;

import com.tencent.isd.lhotse.runner.AbstractTaskRunner;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

public class CheckProcess {
	private final CountDownLatch outputExitLatch = new CountDownLatch(1);
	private static final String PROC_NAME = "java.lang.UNIXProcess";
	private static final String PID_KEY = "pid";
	private final String[] cmdArray;
	private final String commandStart;
	private final String checkValue;
	private final AbstractTaskRunner runner;
	
	private Process process;
	private CheckOutputThread outputThread;	
	private int processId;
	private InputStream os;
	
	public CheckProcess(String[] cmdArray, 
			            String commandStart, 
			            String checkValue,
			            AbstractTaskRunner runner) {
		this.cmdArray = cmdArray;
		this.commandStart = commandStart;
		this.checkValue = checkValue;
		this.runner = runner;
	}
	
	/* Run the process of the commands. */
	public void startProcess() 
	    throws Exception {
		
		startProcess(true);
	}
	
	/* Run the process of the commands. */
	public void startProcess(boolean printLog) 
	    throws Exception {
		
		/* Start a new process and redirect error stream to the normal stream. */
        ProcessBuilder builder = new ProcessBuilder(cmdArray);
        builder.redirectErrorStream(true);       	
        process = builder.start();
        os = process.getInputStream();

        /* Print out the error and standard output for the process. */
        outputThread = new CheckOutputThread(os, commandStart, checkValue, 
          		                             outputExitLatch, printLog, runner);
        outputThread.start();
            
        /* Get the process id. */
    	if (PROC_NAME.equals(process.getClass().getName())) {
   			Field pidField = process.getClass().getDeclaredField(PID_KEY);
   			pidField.setAccessible(true);
   			processId = pidField.getInt(process);
   		}
            
        process.waitFor();        
	}
	
	/* Wait the process finish and finally destory the process. */
	public void waitAndDestroyProcess() 
		throws Exception {

		try {
			process.waitFor();
		} finally {
			if (os!= null) {
        		outputExitLatch.await();
        		os.close();
        	}
        	
        	/* 
        	 * Must call destroy method, otherwise the file handle will 
        	 * exceed the system limit. 
        	 */
        	if (process != null) {
        		process.destroy();
        	}
		}
	}
	
	public int getProcessId() {
		return processId;
	}
			
	/* Return the return value of the process. */
	public int getExitVal() {
		return process.exitValue();
	}
	
	public boolean findCheckValue() {
		return outputThread.findValue();
	}
	
	public String getLastLine() {
		return outputThread.getLastLine();		
	}
	
	public ArrayList<String> getStageOutput() {
		return outputThread.getStageOutput();
	}
}
