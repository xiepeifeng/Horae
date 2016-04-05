package com.tencent.isd.lhotse.runner.util;

import com.tencent.isd.lhotse.runner.AbstractTaskRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;

public class RunnerOutputThread extends Thread {
	private final boolean printInfo;
	private final boolean saveInfo;
	private final StringBuffer sb = new StringBuffer();
	private final CountDownLatch exitLatch;
	private final AbstractTaskRunner runner;

	private InputStream is;
	private final ArrayList<String> lines = new ArrayList<String>();
	private String lastLine = null;

	public RunnerOutputThread(InputStream is, boolean printInfo, CountDownLatch exitLatch,
			AbstractTaskRunner runner) {
		this(is, printInfo, false, exitLatch, runner);
	}

	public RunnerOutputThread(InputStream is, boolean printInfo, boolean saveInfo,
			CountDownLatch exitLatch, AbstractTaskRunner runner) {
		this.is = is;
		this.printInfo = printInfo;
		this.saveInfo = saveInfo;
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
				if (temp.startsWith("TDW-")) {
					lastLine = temp;
				}

				if (printInfo) {
					runner.writeLocalLog(Level.INFO, temp);
					if (saveInfo) {
						lines.add(temp);
					}
				}
				else {
					sb.append(temp + "\n");
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			if (in != null && (is != null)) {
				try {
					in.close();
					in = null;
				}
				catch (IOException e) {
					/* do nothing. */
				}
			}

			exitLatch.countDown();
		}
	}

	public String getResult() {
		return sb.toString();
	}

	public ArrayList<String> getLines() {
		return lines;
	}

	public String getLastLine() {
		if (lastLine == null) {
			return "";
		}
		else {
			return lastLine;
		}
	}
}
