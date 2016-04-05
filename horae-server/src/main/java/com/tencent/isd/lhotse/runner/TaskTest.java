package com.tencent.isd.lhotse.runner;

import java.io.IOException;
import java.util.logging.Level;

import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;

public class TaskTest extends PseudoTaskRunner {

	public static void main(String[] args) {
		TaskRunnerLoader.startRunner(TaskTest.class, (byte) 85);
		/*
		 * TaskTest test = new TaskTest(); test.makeTask("20120827", 1, "test",
		 * "D", 1, "2012082600", "2012082700", false); try { test.execute(); }
		 * catch (IOException e) { e.printStackTrace(); }
		 */
	}

	@Override
	public void execute() throws IOException {
		System.out.println(new java.util.Date());
		LTask task = getTask();
		commitTask(LState.RUNNING, "fuck2012", "Start running " + task.getId());
		writeLocalLog(Level.SEVERE, "shit! Error encountered!");
		commitTask(LState.FAILED, null, "Completed.");
		writeLocalLog(Level.SEVERE, "shit! failed!");
	}

	@Override
	public void kill() throws IOException {
	}
}
