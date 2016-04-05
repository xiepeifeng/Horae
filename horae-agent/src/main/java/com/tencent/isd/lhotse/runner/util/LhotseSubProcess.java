package com.tencent.isd.lhotse.runner.util;

import com.tencent.isd.lhotse.runner.AbstractTaskRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

public abstract class LhotseSubProcess {
	protected final String[] cmdArray;
	protected Process process;
	
	private final CountDownLatch normalExitLatch = new CountDownLatch(1);
	private final CountDownLatch errExitLatch = new CountDownLatch(1);
	private final AbstractTaskRunner runner;
	private final boolean saveInfo;
	
	private InputStream es;
	private InputStream os;
	private RunnerOutputThread normalThread;
	private RunnerOutputThread errThread;

	public LhotseSubProcess(String[] cmdArray, AbstractTaskRunner runner) {
		this(cmdArray, runner, false);
	}
	
	public LhotseSubProcess(String[] cmdArray, 
			                AbstractTaskRunner runner, 
			                boolean saveInfo) {
		this.cmdArray = cmdArray;
		this.runner = runner;
		this.saveInfo = saveInfo;
	}

	/* Run the process of the commands. */
	public void startProcess(boolean printInfo) 
		throws Exception {
		
		Runtime runTime = Runtime.getRuntime();

		process = runTime.exec(cmdArray);

		/* Print out the error and standard output for the process. */
		es = process.getErrorStream();
		os = process.getInputStream();

		errThread = 
			new RunnerOutputThread(es, printInfo, saveInfo, errExitLatch, runner);
		errThread.start();

		normalThread = 
			new RunnerOutputThread(os, printInfo, saveInfo, normalExitLatch, runner);
		normalThread.start();

		/* Get the process id. */
		generateProcessId();
	}

	public InputStream getErrorStream() {
		return process.getErrorStream();
	}

	public InputStream getInputStream() {
		return process.getInputStream();
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

	/* Generate the process id. */
	public abstract void generateProcessId() throws Exception;

	/* Return the process id, kill process doesn't need it. */
	public abstract int getProcessId();

	/* Return the return value of the process. */
	public int getExitVal() {
		return process.exitValue();
	}

	/* Return the error result. */
	public String getErrResult() {
		return errThread.getResult();
	}

	/* Return the normal result. */
	public String getNormalResult() {
		return normalThread.getResult();
	}

	/* Close the output thread buffer. */
	public void close() 
		throws IOException, InterruptedException {
		
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
	
	public ArrayList<String> getAllLines() {
		return normalThread.getLines();
	}
	
	public String getNormalLastLine() {
		return normalThread.getLastLine();
	}
	
	public String getErrorLastLine() {
		return errThread.getLastLine();
	}
}
