package com.tencent.isd.lhotse.runner.util;

import com.tencent.isd.lhotse.runner.AbstractTaskRunner;

import java.lang.reflect.Field;

public class RunTaskProcess extends LhotseSubProcess {
	private static final String PROC_NAME = "java.lang.UNIXProcess";
	private static final String PID_KEY = "pid";

	private int processId;

	public RunTaskProcess(String[] cmdArray, AbstractTaskRunner runner) {
		super(cmdArray, runner);
	}
	
	public RunTaskProcess(String[] cmdArray, 
			              AbstractTaskRunner runner, 
			              boolean saveInfo) {
		super(cmdArray, runner, saveInfo);
	}

	@Override
	public void generateProcessId() throws Exception {
		/* Get the process id. */
		if (PROC_NAME.equals(process.getClass().getName())) {

			Field pidField = process.getClass().getDeclaredField(PID_KEY);
			pidField.setAccessible(true);
			processId = pidField.getInt(process);
		}
	}

	/* Return the process id. */
	public int getProcessId() {
		return processId;
	}
}
