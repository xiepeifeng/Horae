package com.tencent.isd.lhotse.runner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.tencent.isd.lhotse.proto.LhotseObject.LKV;
import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;

/**
 * pseudo class for local test and debug
 * 
 * @author: cpwang
 * @date: 2012-5-8
 */
public abstract class PseudoTaskRunner extends AbstractTaskRunner {
	private SimpleDateFormat sdf = new SimpleDateFormat();
	private final String DATE_FORMAT = "yyyyMMddHHmm";

	public PseudoTaskRunner() {

	}

	protected byte commitTask(LState state, String runTimeId, String desc) throws IOException {
		laststate = state;
		if (this.task == null) {
			System.err.println("Task not initialized.");
			return 0;
		}
		else {
			System.out.println("Task commited: " + state + "," + runTimeId + "," + desc);
			return 1;
		}
	}

	protected void makeTask(String id, int type, String action, String cycleUnit, int cycleNumber, String curDate,
			String nextDate, boolean redo) {
		LTask.Builder ltb = LTask.newBuilder();
		ltb.setState(LState.READY);
		ltb.setId(id);
		ltb.setType(type);
		ltb.setAction(action);
		ltb.setCycleUnit(cycleUnit);
		ltb.setCycleNumber(cycleNumber);
		ltb.setTries(0);

		int len = curDate.length();
		String fmt = DATE_FORMAT.substring(0, len);
		sdf.applyPattern(fmt);
		try {
			java.util.Date dtCur = sdf.parse(curDate);
			java.util.Date dtNext = sdf.parse(nextDate);
			ltb.setCurRunDate(dtCur.getTime());
			ltb.setNextRunDate(dtNext.getTime());
		}
		catch (ParseException e) {
			e.printStackTrace();
			return;
		}
		ltb.setRedoFlag(redo);
		this.task = ltb.build();
	}

	protected void addServer(boolean sourceOrTarget, String tag, String type, String host, int port, String user,
			String pswd, String group, String service) {
		LServer.Builder lsb = LServer.newBuilder();
		lsb.setTag(tag);
		lsb.setType(type);
		lsb.setHost(host);
		lsb.setPort(port);
		lsb.setService(service);
		lsb.setUserName(user);
		lsb.setPassword(pswd);
		lsb.setUserGroup(group);

		LTask.Builder ltb = LTask.newBuilder();
		ltb.mergeFrom(this.task);
		if (sourceOrTarget) {
			ltb.addSourceServers(lsb.build());
		}
		else {
			ltb.addTargetServers(lsb.build());
		}
		this.task = ltb.build();
	}

	protected void addExtProp(String key, String value) {
		LKV.Builder leb = LKV.newBuilder();
		leb.setKey(key);
		leb.setValue(value);
		LTask.Builder ltb = LTask.newBuilder();
		ltb.mergeFrom(this.task);
		ltb.addExtProps(leb.build());
		this.task = ltb.build();
	}
}
