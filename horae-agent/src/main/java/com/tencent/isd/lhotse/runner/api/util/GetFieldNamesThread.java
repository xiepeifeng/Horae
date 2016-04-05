package com.tencent.isd.lhotse.runner.api.util;

import com.tencent.isd.lhotse.runner.AbstractTaskRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;

public class GetFieldNamesThread extends Thread {
	private final InputStream is;
	private final String fieldNamesPrefix;
	private final CountDownLatch exitLatch;
	private final AbstractTaskRunner runner;
	
	private String fieldNamesLine = null;	

	
	
	
    public GetFieldNamesThread(InputStream is, 
    		                   String fieldNamesPrefix,
    		                   CountDownLatch exitLatch,
    		                   AbstractTaskRunner runner) {    		                
        this.is = is;
        this.fieldNamesPrefix = fieldNamesPrefix;
        this.exitLatch = exitLatch;
        this.runner = runner;
    }

    @Override
    public void run() {
    	BufferedReader in = null;
        try {
            in = new BufferedReader(new InputStreamReader(is));
            String temp = null;
            while ((temp = in.readLine()) != null) {
            	runner.writeLocalLog(Level.INFO, temp);
            	if (temp.trim().startsWith(fieldNamesPrefix)) {
            		fieldNamesLine = temp;
            		System.err.println("The field names line: " + fieldNamesLine);
            	}
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        	try {
        	    if (in != null && is != null) {
        	    	in.close();
        	    	in = null;
        	    }
        	} catch (IOException e) {
        		e.printStackTrace();
        	}
        	
        	exitLatch.countDown();
        }
    }
   
    public String getFieldNamesLine() {
    	return fieldNamesLine;
    }
}
