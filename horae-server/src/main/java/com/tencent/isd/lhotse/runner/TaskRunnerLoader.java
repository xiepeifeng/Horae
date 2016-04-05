package com.tencent.isd.lhotse.runner;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Properties;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tencent.isd.lhotse.message.MessageBroker;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.proto.LhotseObject.MessageType;

/**
 * @author: cpwang
 * @date: 2012-5-17
 */
public class TaskRunnerLoader {
	private static final String CFG_PATH = "./cfg/lhotse_base.properties";
	private static String serverIP = "localhost";
	private static int serverPort = 8516;
	private static final SimpleDateFormat sdf = new SimpleDateFormat("[yyyy-MM-dd:HH:mm:ss] ");

	static {
		boolean b1 = false;
		boolean b2 = false;
		// get server address from configuration file
		try {
			FileInputStream fis = new FileInputStream(CFG_PATH);
			Properties props = new Properties();
			props.load(fis);
			String strIP = props.getProperty("base_ip");
			String strPort = props.getProperty("base_port");
			if ((strIP != null) && (!strIP.trim().equals(""))) {
				serverIP = strIP.trim();
				b1 = true;
			}
			if ((strPort != null) && (!strPort.trim().equals(""))) {
				try {
					serverPort = Integer.parseInt(strPort);
					b2 = true;
				}
				catch (NumberFormatException e) {
				}
			}

		}
		catch (FileNotFoundException e) {
		}
		catch (IOException e) {
		}

		if (!b1) {
			printErrorLog("Failed to read [base_ip] from " + CFG_PATH + ",exit!");
			System.exit(0);
		}

		if (!b2) {
			printErrorLog("Failed to read [base_port] from " + CFG_PATH + ",exit!");
			System.exit(0);
		}
	}

	public static void startRunner(Class<? extends AbstractTaskRunner> cls, int taskType) {
		final MessageBroker messageBroker = new MessageBroker();
		// at beginning, recover hanged(running) tasks
		byte[] retVal = null;
		try {
			retVal = messageBroker
					.sendAndReceive(serverIP, serverPort, (byte) MessageType.START_RUNNER_VALUE, taskType);
		}
		catch (IOException e) {
			printErrorLog(e.getMessage());
			return;
		}

		if ((retVal == null) || (retVal.length < 1)) {
			printErrorLog("Start runner " + taskType + ":Illegal message length!");
			return;
		}

		if (retVal[0] != 1) {
			printErrorLog("Type " + taskType + " is not registered, runners can not be started up!");
			return;
		}

		int pollingSeconds = 60;
		while (true) {
			byte[] messageData = null;
			boolean bGot = false;
			try {
				messageData = messageBroker.sendAndReceive(serverIP, serverPort, (byte) MessageType.REQUEST_TASK_VALUE,
						taskType);

				if ((messageData == null) || (messageData.length == 0)) {
					printErrorLog("Request task of type " + taskType + ":Illeage message length!");
				}
				else {
					int len = messageData.length;
					if (len == 1) {
						// null message
						if (messageData[0] < 0) {
							printErrorLog("Base is not available!");
						}
					}
					else {
						LTask task = LTask.parseFrom(messageData);
						if (task != null) {
							bGot = true;
							pollingSeconds = task.getPollingSeconds();
							AbstractTaskRunner runner = cls.newInstance();
							runner.setTask(task);
							Thread thread = new Thread(runner);
							thread.start();
						}
					}
				}
			}
			catch (InvalidProtocolBufferException e) {
				printErrorLog("Request task:" + getStackTraceString(e));
				e.printStackTrace();
			}
			catch (IOException e) {
				printErrorLog("Request task: " + getStackTraceString(e));
				e.printStackTrace();
			}
			catch (Exception e) {
				printErrorLog(e.getMessage());
				e.printStackTrace();
			}
			// sleep
			try {
				if (!bGot) {
					pollingSeconds = 60;
				}
				Thread.sleep(pollingSeconds * 1000);
			}
			catch (InterruptedException e1) {
			}
		}
	}

	public static String getServerIP() {
		return serverIP;
	}

	public static int getServerPort() {
		return serverPort;
	}

	private static String getStackTraceString(Exception e) {
		if (e == null) {
			return "";
		}

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		pw.flush();
		return sw.toString();
	}

	private static synchronized void printErrorLog(String log) {
		System.err.println(sdf.format(new java.util.Date()) + log);
	}
}
