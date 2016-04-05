package com.tencent.isd.lhotse.runner.util;

import com.tencent.isd.lhotse.runner.AbstractTaskRunner;

public class KillTaskProcess extends LhotseSubProcess {
	public KillTaskProcess(String[] cmdArray, AbstractTaskRunner runner) {
		super(cmdArray, runner);		
	}

	@Override
	public void generateProcessId() {
		/* Do nothing if it's a killing process. */
	}
	
	@Override
	public int getProcessId() {
		throw new UnsupportedOperationException("Killing process doesn't support this behaivor");
	}
}
