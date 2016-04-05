package com.tencent.isd.lhotse.runner.util;

import com.tencent.isd.lhotse.runner.AbstractTaskRunner;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

public class DDCRunTaskProcess {
	private static final String PROC_NAME = "java.lang.UNIXProcess";
	private static final String PID_KEY = "pid";
	private final boolean saveInfo;
	private final String[] cmdArray;
	private final CountDownLatch exitLatch = new CountDownLatch(1);
	private final AbstractTaskRunner runner;
	
	private Process process;	
	private int processId;
	private InputStream os;
	private DDCRunTaskThread outputThread;	

	public DDCRunTaskProcess(String[] cmdArray, AbstractTaskRunner runner) {
		this(cmdArray, runner, false);
	}
	
	public DDCRunTaskProcess(String[] cmdArray, 
			                 AbstractTaskRunner runner, 
			                 boolean saveInfo) {
		this.cmdArray = cmdArray;
		this.runner = runner;
		this.saveInfo = saveInfo;
	}

	/* Run the process of the commands. */
	public void startProcess() 
		throws Exception {
		
		ProcessBuilder builder = new ProcessBuilder(cmdArray);
		builder.redirectErrorStream(true);
		process = builder.start();

		/* Print out the error and standard output for the process. */
		os = process.getInputStream();

		outputThread = 
			new DDCRunTaskThread(os, saveInfo, exitLatch, runner);
		outputThread.start();

		/* Get the process id. */
		/* Get the process id. */
		if (PROC_NAME.equals(process.getClass().getName())) {

			Field pidField = process.getClass().getDeclaredField(PID_KEY);
			pidField.setAccessible(true);
			processId = pidField.getInt(process);
		}
	}

	/* Wait the process finish and finally destory the process. */
	public void waitAndDestroyProcess() 
		throws Exception {

		try {
			process.waitFor();
		} finally {
			close();
		}
	}

	/* Return the process id. */
	public int getProcessId() {
		return processId;
	}

	/* Return the return value of the process. */
	public int getExitVal() {
		return process.exitValue();
	}

	/* Close the output thread buffer. */
	public void close() 
		throws IOException, InterruptedException {
		
		try {
			if (os != null) {
				exitLatch.await();
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
	
	public ArrayList<String> getAllLines() {
		return outputThread.getAllLines();
	}
	
	public String getLastLine() {
		return outputThread.getLastLine();
	}
	
	public int getLineCount() {
		return outputThread.getOutputCount();
	}
}
