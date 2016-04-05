package com.tencent.isd.lhotse.dao;

/**
 * @author: cpwang
 * @date: 2012-3-15
 */
public class SysLog extends DBLog {
	private static final String SYS_TRIGGER = "LHOTSE";
	private String brokerIP;
	private String source;
	private String description;
	private String trigger;
	private String type;
	private long logTime;
	private long timeElapsed;
	private int taskType;

	public SysLog(String src, String type, String desc, String sub, String broker, long te, int taskType) {
		this.source = src;
		this.description = desc;
		this.trigger = sub;
		this.type = type;
		this.brokerIP = broker;
		this.timeElapsed = te;
		this.logTime = System.currentTimeMillis();
		this.taskType = taskType;
	}

	public SysLog(String src, String type, String desc, String broker, long te) {
		this(src, type, desc, SYS_TRIGGER, broker, te, -1);
	}

	public SysLog(String src, String type, String desc, String broker) {
		this(src, type, desc, SYS_TRIGGER, broker, 0, -1);
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public long getLogTime() {
		return logTime;
	}

	public void setLogTime(long logTime) {
		this.logTime = logTime;
	}

	public String getTrigger() {
		return trigger;
	}

	public void setTrigger(String trigger) {
		this.trigger = trigger;
	}

	public String getBrokerIP() {
		return brokerIP;
	}

	public void setBrokerIP(String brokerIP) {
		this.brokerIP = brokerIP;
	}

	public String toString() {
		return "[" + this.brokerIP + "] [" + this.timeElapsed + "] " + this.description;
	}

	public long getTimeElapsed() {
		return timeElapsed;
	}

	public void setTimeElapsed(long timeElapsed) {
		this.timeElapsed = timeElapsed;
	}

	public int getTaskType() {
		return taskType;
	}

	public void setTaskType(int taskType) {
		this.taskType = taskType;
	}

}
