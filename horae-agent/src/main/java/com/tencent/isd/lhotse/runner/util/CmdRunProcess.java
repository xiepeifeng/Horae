package com.tencent.isd.lhotse.runner.util;

import com.tencent.isd.lhotse.runner.AbstractTaskRunner;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CmdRunProcess {

	protected Process process;
	private final CountDownLatch normalExitLatch = new CountDownLatch(1);
	private final CountDownLatch errExitLatch = new CountDownLatch(1);
	protected final String cmdStr;
	protected final String[] envStr;
	private final AbstractTaskRunner runner;
	protected File workPathFile = null;

	private InputStream es;
	private InputStream os;
	private CmdOutputReadThread outputThread;
	private CmdOutputReadThread errputThread;

	private int overMin = 0;
	private boolean bOverTime = false;
	private int exitCode = 0;
	private String[] cmdArr;
	
	private static String regStr = "\"[^\"]*\"";
	private static Pattern stringPat = Pattern.compile(regStr);
	public CmdRunProcess(String cmdStr, String[] envStr, String workDirStr, int overMin, AbstractTaskRunner runner) {
		
		String foundParam = null;
		String newStr = null;
		
		this.runner = runner;
		
		this.cmdStr = cmdStr;
		this.overMin = overMin;
		this.envStr = envStr;
		this.workPathFile = new File(workDirStr);
		
		Matcher mtStr = stringPat.matcher(cmdStr);
		while( mtStr.find() ) {
			foundParam = mtStr.group();
			newStr = foundParam.replaceAll(" ", "\001");
			cmdStr = cmdStr.replace( foundParam, newStr);
		}
		String[] cmdTempArr = cmdStr.split(" +");
		for(int i = 0; i< cmdTempArr.length; i++ ){
			cmdTempArr[i] = cmdTempArr[i].replaceAll("\\001", " ");
			cmdTempArr[i] = cmdTempArr[i].replaceAll("\"", " ");
			cmdTempArr[i] = cmdTempArr[i].trim();
		}
		cmdArr = cmdTempArr;
	}
	
	public CmdRunProcess(String[] cmdArr, String[] envStr, String workDirStr, int overMin, AbstractTaskRunner runner) {
		this.cmdArr = cmdArr;
		this.cmdStr = cmdArr.toString();
		this.overMin = overMin;
		this.envStr = envStr;
		this.runner = runner;
		this.workPathFile = new File(workDirStr);
	}
	
	public CmdRunProcess(String cmdStr, int overMin, AbstractTaskRunner runner) {
		this.cmdStr = cmdStr;
		this.overMin = overMin;
		this.envStr = null;
		this.runner = runner;
		this.workPathFile = new File(System.getProperty("user.dir"));
	}

	/* Run the process of the commands. */
	public void startProcess() throws Exception {
		Runtime runTime = Runtime.getRuntime();

		process = runTime.exec(cmdArr, envStr, workPathFile);
		es = process.getErrorStream();
		os = process.getInputStream();

		errputThread = new CmdOutputReadThread(es, errExitLatch, runner);
		errputThread.setDaemon(true);
		errputThread.start();

		outputThread = new CmdOutputReadThread(os, normalExitLatch, runner);
		outputThread.setDaemon(true);
		outputThread.start();

	}

	public void waitAndDestroyProcess() throws Exception {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MINUTE, overMin);
		try {
			do{
				try {
					exitCode = process.exitValue();
					bOverTime = true;
				} catch (IllegalThreadStateException ille) {
					bOverTime = false;
					try {
						Thread.sleep(10 * 1000);
					} catch (InterruptedException ie) {
						/* do nothing */
					}
				}
			} while(!bOverTime && (Calendar.getInstance().compareTo(cal) < 0));
			
			if (!bOverTime) {
				try {
					process.destroy();
				} catch (Exception ex) {

				}
			}
		} finally {
			close();
		}
	}
	
	public String getOutputStr() {
		return outputThread.getResult();
	}
	
	public ArrayList<String> getOutputLines() {
		return outputThread.getLines();
	}
	
	public String getErrStr() {
		return errputThread.getResult();
	}
	
	public boolean isOverTime() {
		return bOverTime;
	}
	
	public int getExitCode() {
		return exitCode;
	}
	
	private void close() throws IOException, InterruptedException {

		try {
			if (es != null) {
				errExitLatch.await();
				es.close();
				es = null;
			}

			if (os != null) {
				normalExitLatch.await();
				os.close();
				os = null;
			}
		} finally {

			/*
			 * Must call destroy method, otherwise the file handle will exceed
			 * the system limit.
			 */
			if (process != null) {
				process.destroy();
			}
		}
	}
}
