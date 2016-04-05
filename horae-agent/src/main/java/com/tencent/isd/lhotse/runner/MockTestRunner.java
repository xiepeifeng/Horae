package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LState;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class MockTestRunner extends AbstractTaskRunner {
	public static void main(String[] args) 
		throws Exception {
	  File f = new File(new File(".").getAbsolutePath());
	  System.out.println(f);
	  TaskRunnerLoader.startRunner(MockTestRunner.class, (byte) 1);
	}

	@Override
	public void execute() throws IOException {
		System.out.println("==========================");
		boolean success = false;
		try {
			Random random = new Random();
			long sleepTime = random.nextInt(180000);
			Thread.sleep(sleepTime);
			int rad = Double.valueOf(10*Math.random()).intValue();
			if(rad%2 == 0){
			    success = true;
			}else{
				success = false;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (success) {
				this.commitTask(LState.SUCCESSFUL, "Mock runner", "Mock runner finished successfully.");
			} else {
				this.commitTask(LState.FAILED, "Mock runner", "Mock runner failed");
			}
		}
	}

	@Override
	public void kill() throws IOException {
		// TODO Auto-generated method stub

	}
}
