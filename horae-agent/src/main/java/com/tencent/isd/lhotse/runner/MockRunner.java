package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.AbstractTaskRunner;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.util.CommonUtils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;

public class MockRunner extends AbstractTaskRunner {

	public static void main(String[] args) throws Exception {
		String taskType = args[0];
		int taskTypeId = Integer.parseInt(taskType);
		TaskRunnerLoader.startRunner(MockRunner.class, (byte) taskTypeId);
	}

	@Override
	public void execute() throws IOException {
		LTask task = getTask();
		// long taskTime = 30 * 1000; // wait 30s
		boolean exceptionFound = false;
		boolean success = true;
		try {
			this.writeLocalLog(Level.INFO, "=====================");
			String startLogString = "Start running MockRunner:"
					+ task.getId()
					+ ", system date:"
					+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(new Date(
							System.currentTimeMillis()));
			System.out.println(startLogString);

			// commitTask(LState.RUNNING, null, "Task waiting time=" +
			// taskTime);
			// this.writeLocalLog(Level.INFO, "Task waiting time=" + taskTime);
			// Thread.sleep(taskTime);
			this.writeLocalLog(Level.INFO, "=====================\n");
			int rad = Double.valueOf(10*Math.random()).intValue();
			if(rad%2 == 0){
			    success = true;
			}else{
				success = false;
			}
		} catch (Exception e) {
			String st = CommonUtils.stackTraceToString(e);
			this.writeLocalLog(Level.SEVERE, "Exception stackTrace: " + st);
			this.writeLocalLog(Level.INFO, "=====================\n");
			exceptionFound = true;
			throw new IOException(e);
		} finally {
			if (!success)
				commitTask(LState.FAILED, null, "Failed with exception.");
			else
				commitTask(LState.SUCCESSFUL, null, "Completed.");
		}
	}

	@Override
	public void kill() throws IOException {
		// TODO Auto-generated method stub

	}

}
