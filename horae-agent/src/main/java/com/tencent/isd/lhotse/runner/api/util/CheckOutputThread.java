package com.tencent.isd.lhotse.runner.api.util;

import com.tencent.isd.lhotse.runner.AbstractTaskRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;

public class CheckOutputThread extends Thread {
	private final String STAGE_START = "Stage-1";
	private final InputStream is;
	private final String commandStart;
	private final String checkValue;
	private final CountDownLatch exitLatch;
	private final ArrayList<String> stageOutput = new ArrayList<String>();
	private final AbstractTaskRunner runner;

	private boolean findValue = false;
	private String lastLine = null;

	public CheckOutputThread(InputStream is, String commandStart, String checkValue,
			CountDownLatch exitLatch, AbstractTaskRunner runner) {
		this.is = is;
		this.commandStart = commandStart;
		this.checkValue = checkValue;
		this.exitLatch = exitLatch;
		this.runner = runner;
	}

	@Override
	public void run() {
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(is));
			boolean started = false;
			boolean stageStarted = false;
			String temp = null;
			int stageCounter = 0;
			while ((temp = in.readLine()) != null) {
				runner.writeLocalLog(Level.INFO, temp);
				if (temp.startsWith(commandStart.toLowerCase())) {
					started = true;
				}

				if (started && !findValue && temp.equalsIgnoreCase(checkValue)) {
					// if (!findValue && temp.equalsIgnoreCase(checkValue)) {
					findValue = true;
				}

				if (started && temp.startsWith("TDW-")) {
					// if (temp.startsWith("TDW-")) {
					lastLine = temp;
				}

				if (temp.startsWith(STAGE_START)) {
					stageStarted = true;
				}

				if (started && stageStarted && (stageCounter <= 6)) {
					// if (stageStarted && (stageCounter <= 6)) {
					stageOutput.add(temp.trim());
					stageCounter++;
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (in != null && is != null) {
					in.close();
					in = null;
				}
			}
			catch (IOException e) {
				e.printStackTrace();
			}

			exitLatch.countDown();
		}
	}

	public boolean findValue() {
		return findValue;
	}

	public String getLastLine() {
		if (lastLine == null) {
			return "";
		}
		else {
			return lastLine;
		}
	}

	public ArrayList<String> getStageOutput() {
		return stageOutput;
	}
}
