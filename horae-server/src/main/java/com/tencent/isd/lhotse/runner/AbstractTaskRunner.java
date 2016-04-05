package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.message.MessageBroker;
import com.tencent.isd.lhotse.proto.LhotseObject.*;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

//import java.util.logging.Handler;
//import java.util.logging.FileHandler;
//import java.util.logging.Handler;
//import java.util.logging.Level;
//import java.util.logging.LogRecord;
//import java.util.logging.Logger;
//import java.util.logging.SimpleFormatter;

/**
 * @author: cpwang
 * @date: 2012-3-23
 */
public abstract class AbstractTaskRunner implements Runnable {
	LTask task;
	LState laststate;
	private final byte b_COMMIT_TASK_VALUE = (byte) MessageType.COMMIT_TASK_VALUE;
	private final byte b_REQUEST_PARAMETER_VALUE = (byte) MessageType.REQUEST_PARAMETER_VALUE;
	// write local log
	private Logger logger = null;
	private String logName = null;
	private final static String LOG_DIR = "./log/tasklog/";
	private final static SimpleDateFormat sdf = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]");

	/**
	 * 
	 * @param state
	 * @param runTimeId
	 * @param desc
	 * @return
	 * @throws IOException
	 */
	protected byte commitTask(LState state, String runTimeId, String desc) throws IOException {
		if (task == null) {
			return 0;
		}
		laststate = state;
		// illegal state
		if (state == LState.READY) {
			throw new IOException("Illegal task state!");
		}
		LTask.Builder lb = LTask.newBuilder();
		lb.setId(task.getId());
		lb.setTries(task.getTries());
		lb.setType(task.getType());
		lb.setCurRunDate(task.getCurRunDate());
		lb.setNextRunDate(task.getNextRunDate());
		lb.setState(state);
		lb.setRuntimeId(runTimeId == null ? "" : runTimeId);

		if (desc != null && desc.length() > 4000) {
			desc = desc.substring(0, 4000);
		}

		lb.setLogDesc(desc == null ? "" : desc);
		LTask tmpTask = lb.build();
		// send to server
		byte[] taskData = tmpTask.toByteArray();

		Exception commitException = null;
		for (int tries = 0; tries < 10; tries++) {
			// set state and log
			// try 10 times mostly
			try {
				MessageBroker messageBroker = new MessageBroker();
				byte[] messageData = messageBroker.sendAndReceive(TaskRunnerLoader.getServerIP(),
						TaskRunnerLoader.getServerPort(), b_COMMIT_TASK_VALUE, taskData);
				if ((messageData == null) || (messageData.length < 1)) {
					throw new SocketException("Illegal message length!");
				}
				if (messageData[0] < 0) {
					throw new SocketException("Corrupt data!");
				}
				return messageData[0];
			}
			catch (SocketException e) {
				// wait 6 seconds to commit again
				try {
					Thread.sleep(6000);
				}
				catch (InterruptedException e1) {
				}
				if (commitException == null)
					commitException = e;
			}
		}

		String exceptionMsg = "Timeout to commit task:" + tmpTask.getId() + "[" + tmpTask.getCurRunDate() + ","
				+ tmpTask.getNextRunDate() + "),bytes:" + taskData.length;

		if (commitException != null)
			throw new IOException(exceptionMsg, commitException);
		else
			throw new IOException(exceptionMsg);

	}

	protected byte reportProgress(String desc) throws IOException {
		return commitTask(LState.RUNNING, null, desc);
	}

	public abstract void execute() throws IOException;

	public abstract void kill() throws IOException;

	/**
	 * peek task instance alive or not
	 */
	// public abstract void peek() throws IOException;

	public void run() {
		laststate = LState.READY;
		try {
			if (task.getState() == LState.SENTENCED) {
				// cpwang,20120816,change KILLING to SENTENCED
				kill();
			}
			else {
				execute();
			}
		}
		catch (IOException e) {
			try {
				commitTask(LState.FAILED, null, e.getMessage());
			}
			catch (IOException e1) {
				printErrorLog(e1.getMessage());
				e1.printStackTrace();
			}
		}
		catch (Exception e) {
			printErrorLog(e.getMessage());
			e.printStackTrace();
			try {
				commitTask(LState.FAILED, null, e.getMessage());
			}
			catch (IOException e1) {
				printErrorLog(e1.getMessage());
				e1.printStackTrace();
			}
		}
		finally {
			// no reasonable state after run, set to FAILED
			// cpwang,20120816. add KILLED and HANGED state
			if ((laststate != LState.SUCCESSFUL) && (laststate != LState.FAILED) && (laststate != LState.KILLED)
					&& (laststate != LState.HANGED)) {
				try {
					commitTask(LState.FAILED, null, "Unexpected state:" + laststate);
				}
				catch (IOException e) {
					printErrorLog(e.getMessage());
					e.printStackTrace();
				}
			}
		}

//		logger.getAppender(logName).close();
		// if (logger != null) {
		// for (Handler h : logger.getHandlers()) {
		// h.close();
		// }
		// // clear LCK file
		// }
	}

	private LServer getServer(String tag, List<LServer> servers) {
		if (task == null) {
			return null;
		}

		if ((servers == null) || (servers.size() == 0)) {
			return null;
		}
		Iterator<LServer> iter = servers.iterator();
		LServer serv = null;
		while (iter.hasNext()) {
			serv = iter.next();
			if (serv.getTag().equals(tag)) {
				return serv;
			}
		}
		return null;
	}

	protected LServer getSourceServer(String tag) {
		List<LServer> servers = this.task.getSourceServersList();
		return getServer(tag, servers);
	}

	protected LServer getSourceServer(int idx) {
		List<LServer> servers = this.task.getSourceServersList();
		return servers.get(idx);
	}

	protected LServer getTargetServer(String tag) {
		List<LServer> servers = this.task.getTargetServersList();
		return getServer(tag, servers);
	}

	protected LServer getTargetServer(int idx) {
		List<LServer> servers = this.task.getTargetServersList();
		return servers.get(idx);
	}

	protected String getExtPropValue(String key) {
		if (task == null) {
			return null;
		}
		List<LKV> eps = this.task.getExtPropsList();
		if ((eps == null) || (eps.size() == 0)) {
			return null;
		}
		Iterator<LKV> iter = eps.iterator();
		while (iter.hasNext()) {
			LKV ep = iter.next();
			if (ep.getKey().equals(key)) {
				// change LKV.value from string to bytes
				// return ep.getValue();
				return ep.getValue();
			}
		}
		return null;
	}

	protected HashMap<String, String> requestGlobalParameters() throws IOException {
		MessageBroker messageBroker = new MessageBroker();
		System.out.println(TaskRunnerLoader.getServerIP() + "---------------");
		System.out.println(TaskRunnerLoader.getServerPort() + "------------");
		System.out.println(task.getType() + "---------------");
		byte[] messageData = messageBroker.sendAndReceive(TaskRunnerLoader.getServerIP(),
				TaskRunnerLoader.getServerPort(), b_REQUEST_PARAMETER_VALUE, task.getType());
		if ((messageData == null) || (messageData.length <= 1)) {
			// throw new IOException("Illegal message length!");
			return null;
		}
		LParameters.Builder lpb = LParameters.newBuilder();
		lpb.mergeFrom(messageData);
		LParameters lps = lpb.build();
		if (lps == null) {
			return null;
		}
		List<LKV> kvs = lps.getParametersList();
		Iterator<LKV> iter = kvs.iterator();
		HashMap<String, String> params = new HashMap<String, String>();
		while (iter.hasNext()) {
			LKV kv = iter.next();
			// change LKV.value from string to bytes
			params.put(kv.getKey(), kv.getValue());
		}
		return params;
	}

	// public void writeLocalLog(Level level, String log) {
	// if (logger == null) {
	// // log path: ./log/${task_id}/${cur_run_date}.log
	// String logDir = LOG_DIR + task.getId() + "/";
	// File dir = new File(logDir);
	// if (!dir.exists()) {
	// dir.mkdirs();
	// }
	// String logName = LDateUtil.getFullDateString(new java.util.Date(task.getCurRunDate())) + ".log";
	//
	// try {
	// logger = Logger.getLogger(task.getId() + "-" + task.getCurRunDate());
	// logger.setLevel(Level.INFO);
	// Handler handler = new FileHandler(logDir + logName);
	// handler.setEncoding("UTF-8");
	// handler.setFormatter(new SimpleFormatter() {
	// public String format(LogRecord record) {
	// return sdf.format(new java.util.Date()) + "-[" + record.getLevel() + "] " + record.getMessage()
	// + "\n";
	// }
	// });
	// logger.setUseParentHandlers(false);
	// logger.addHandler(handler);
	// }
	// catch (SecurityException e) {
	// printErrorLog(e.getMessage());
	// return;
	// }
	// catch (IOException e) {
	// printErrorLog(e.getMessage());
	// return;
	// }
	// }
	//
	// logger.log(level, log);
	// }

	public void writeLocalLog(java.util.logging.Level level, String log) {
		if (logger == null) {

			String logDir = LOG_DIR + task.getId() + "/";
			File dir = new File(logDir);
			if (!dir.exists()) {
				dir.mkdirs();
			}
			String curTime = String.valueOf(System.currentTimeMillis());
			logName = logDir + task.getId() + "-" + task.getCurRunDate() + "." + task.getTries() + "." + curTime
					+ ".log";
			logger = Logger.getLogger(logName);
			logger.setLevel(Level.INFO);

			PatternLayout infoLayout = new PatternLayout("[%-d{yyyy-MM-dd HH:mm:ss}]-[%p] %m %n");
			try {

				DailyRollingFileAppender infoAppender = new DailyRollingFileAppender(infoLayout, logName,
						"'.'yyyy-MM-dd");
				infoAppender.setAppend(true);
				infoAppender.setThreshold(Level.INFO);
				infoAppender.activateOptions();
				logger.addAppender(infoAppender);
				infoAppender.setName(logName);
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}

		logger.log(levelChangle(level), log);
	}

	private Level levelChangle(java.util.logging.Level level) {
		Level t = Level.ALL;
		if (level == java.util.logging.Level.WARNING) {
			t = Level.WARN;
		}
		else if (level == java.util.logging.Level.CONFIG) {
			t = Level.DEBUG;
		}
		else if (level == java.util.logging.Level.SEVERE) {
			t = Level.FATAL;
		}
		else {
			t = Level.INFO;
		}

		return t;
	}

	protected LTask getTask() {
		return task;
	}

	public void setTask(LTask t) {
		this.task = t;
	}

	private void printErrorLog(String log) {
		System.err.println(sdf.format(new java.util.Date()) + log);
	}
}
