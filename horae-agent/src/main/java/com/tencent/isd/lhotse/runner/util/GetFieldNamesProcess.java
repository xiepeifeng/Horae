package com.tencent.isd.lhotse.runner.util;

import com.tencent.isd.lhotse.runner.AbstractTaskRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.concurrent.CountDownLatch;

public class GetFieldNamesProcess {
	private final String startPrefix = "outputColumnNames:";
	private final CountDownLatch outputLatch = new CountDownLatch(1);
	
	private final String[] cmdArray;
	private final AbstractTaskRunner runner;
	
	private Process process;
	private InputStream os;
	private GetFieldNamesThread outputThread;
	
	
	public GetFieldNamesProcess(String[] cmdArray, AbstractTaskRunner runner) {
		this.cmdArray = cmdArray;
		this.runner = runner;
	}
	
	/* Run the process of the commands. */
	public void runProcess() 
	    throws InterruptedException, IOException {
		
        try {
        	ProcessBuilder builder = new ProcessBuilder(cmdArray);
        	builder.redirectErrorStream(true);
            process = builder.start();

            os = process.getInputStream();

            /* Print out the error and standard output for the process. */
            outputThread = 
            	new GetFieldNamesThread(os, startPrefix, outputLatch, runner);
            outputThread.start();
            
            process.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {        	
        	if (os != null) {
        		outputLatch.await();
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
	
	public String getLastLine() {
		return outputThread.getLastLine();
	}
	
	public ArrayList<String> getFieldNames() {
		ArrayList<String> fieldNames = new ArrayList<String>();		
		
		if (outputThread.getFieldNamesLine() != null) {		
			StringTokenizer st = 
		   	    new StringTokenizer(outputThread.getFieldNamesLine().trim(), ":");
		    st.nextToken();
		
		    StringTokenizer fieldNameTokens = 
		    	new StringTokenizer(st.nextToken(), ",");
		    while (fieldNameTokens.hasMoreTokens()) {
			    fieldNames.add(fieldNameTokens.nextToken().trim());
		    }
		}
		
		return fieldNames;
	}
}

