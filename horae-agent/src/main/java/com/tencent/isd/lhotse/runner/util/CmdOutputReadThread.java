package com.tencent.isd.lhotse.runner.util;

import com.tencent.isd.lhotse.runner.AbstractTaskRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;

public class CmdOutputReadThread extends Thread {
	
	private final StringBuffer sb = new StringBuffer();
	private final CountDownLatch exitLatch;
	
	private InputStream is;	
	private final ArrayList<String> lines = new ArrayList<String>();
	private final Object getResultLock = new Object();
	private boolean getOutOver = false;
	private final AbstractTaskRunner runner;
	
	CmdOutputReadThread( InputStream is, CountDownLatch exitLatch, AbstractTaskRunner runner ){
		this.is = is;
		this.runner = runner;
		this.exitLatch = exitLatch;		
	}
	
	@Override          
	public void run() {
		BufferedReader in = null;
		try{
			in = new BufferedReader( new InputStreamReader(is));
			String temp = null;
			while((temp = in.readLine()) != null){
				runner.writeLocalLog( Level.INFO, temp);
				synchronized( getResultLock) {
					lines.add( temp);
					sb.append(temp + "\n");
				}
			}
		}catch( Exception e ){
			e.printStackTrace();
		} finally {
			getOutOver = true;
			if (in != null && (is != null)) {
				try {
				    in.close();
				    in = null;
				} catch (IOException e) {
					/* do nothing. */
				}
			}
			
			exitLatch.countDown();
		}
	}
	
	public String getResult() {
		String  resultStr = null;
		synchronized( getResultLock) {
			resultStr =  sb.toString();
		}
		return resultStr;
	}
	
	public ArrayList<String> getLines() {
		if(getOutOver){
			return lines;
		} else {
			return null;
		}
	}
}