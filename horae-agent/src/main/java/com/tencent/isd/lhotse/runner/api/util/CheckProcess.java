package com.tencent.isd.lhotse.runner.api.util;

import com.tencent.isd.lhotse.runner.AbstractTaskRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

public class CheckProcess {
	private final CountDownLatch errExitLatch  = new CountDownLatch(1);
	private final CountDownLatch outputExitLatch = new CountDownLatch(1);
	
	private final String[] cmdArray;
	private final String commandStart;
	private final String checkValue;
	private final AbstractTaskRunner runner;
	
	private Process process;
	private CheckOutputThread outputThread;
	private RunnerOutputThread errThread;	
	
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
	public void runProcess() 
	    throws IOException, InterruptedException {
		
		Runtime runTime = Runtime.getRuntime();
		InputStream es = null;
		InputStream os = null;
        try {
            process = runTime.exec(cmdArray);

            es = process.getErrorStream();
            os = process.getInputStream();

            /* Print out the error and standard output for the process. */
            errThread = new RunnerOutputThread(es, true, errExitLatch, runner);
            errThread.start();

            outputThread = new CheckOutputThread(os, commandStart, checkValue, 
            		                             outputExitLatch, runner);
            outputThread.start();
            
            process.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        	if (es != null) {
        		errExitLatch.await();
        		es.close();
        	}
        	
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
