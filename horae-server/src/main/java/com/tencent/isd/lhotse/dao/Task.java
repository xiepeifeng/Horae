package com.tencent.isd.lhotse.dao;

import java.util.HashMap;


public class Task {
	// cycle unit
	public static final String MINUTE_CYCLE = "I";
	public static final String HOUR_CYCLE = "H";
	public static final String DAY_CYCLE = "D";
	public static final String WEEK_CYCLE = "W";
	public static final String MONTH_CYCLE = "M";
	public static final String ONEOFF_CYCLE = "O";

	private static final HashMap<String, Integer> cycleUnits = new HashMap<String, Integer>();

	private String id;
	private int type;
	private String name;
	private String brokerIP;
	// private String serverTags;
	private String sourceServer;
	private String targetServer;
	private String cycleUnit;
	private int cycleNumber;
	private java.sql.Timestamp startDate;
	private java.sql.Timestamp endDate;
	private int selfDepend = 1;
	private String action;
	private int delayTime = 0;
	private int startupTime = 0;
	private int tryLimit = -1;
	private String inCharge;
	private int aliveWait = 30;
	private String status;
	private int priority;
	private java.sql.Timestamp lastRunDate;

	static {
		cycleUnits.put(MINUTE_CYCLE, 1);
		cycleUnits.put(HOUR_CYCLE, 2);
		cycleUnits.put(DAY_CYCLE, 3);
		cycleUnits.put(WEEK_CYCLE, 4);
		cycleUnits.put(MONTH_CYCLE, 5);
		cycleUnits.put(ONEOFF_CYCLE, 9);
	}

	public Task() {
	}

	public String getId() {
		return id;
	}

	public int getType() {
		return type;
	}

	public String getName() {
		return name;
	}

	public String getBrokerIP() {
		return brokerIP;
	}

	public int getCycleNumber() {
		return cycleNumber;
	}

	public String getAction() {
		return action;
	}

	public int getDelayTime() {
		return delayTime;
	}

	public int getStartupTime() {
		return startupTime;
	}

	public String getInCharge() {
		return inCharge;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setType(int type) {
		this.type = type;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setBrokerIP(String brokerIP) {
		this.brokerIP = brokerIP;
	}

	public void setCycleNumber(int cycleNumber) {
		this.cycleNumber = cycleNumber;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public void setDelayTime(int delayTime) {
		this.delayTime = delayTime;
	}

	public void setStartupTime(int startupTime) {
		this.startupTime = startupTime;
	}

	public void setInCharge(String inCharge) {
		this.inCharge = inCharge;
	}

	public int getSelfDepend() {
		return selfDepend;
	}

	public void setSelfDepend(int selfDepend) {
		this.selfDepend = selfDepend;
	}

	public int getAliveWait() {
		return aliveWait;
	}

	public void setAliveWait(int aliveWait) {
		this.aliveWait = aliveWait;
	}

	public String getCycleUnit() {
		return cycleUnit;
	}

	public void setCycleUnit(String cycleUnit) {
		this.cycleUnit = cycleUnit;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	// public String getServerTags() {
	// return serverTags;
	// }

	// public void setServerTags(String serverTags) {
	// this.serverTags = serverTags;
	// }

	public static int getCycleUnitValue(String c) {
		return cycleUnits.get(c);
	}

	public String getSourceServer() {
		return sourceServer;
	}

	public void setSourceServer(String sourceServer) {
		this.sourceServer = sourceServer;
	}

	public String getTargetServer() {
		return targetServer;
	}

	public void setTargetServer(String targetServer) {
		this.targetServer = targetServer;
	}

	/**
	 * @return the startDate
	 */
	public java.sql.Timestamp getStartDate() {
		return startDate;
	}

	/**
	 * @param startDate
	 *            the startDate to set
	 */
	public void setStartDate(java.sql.Timestamp startDate) {
		this.startDate = startDate;
	}

	/**
	 * @return the endDate
	 */
	public java.sql.Timestamp getEndDate() {
		return endDate;
	}

	/**
	 * @param endDate
	 *            the endDate to set
	 */
	public void setEndDate(java.sql.Timestamp endDate) {
		this.endDate = endDate;
	}

	public int getTryLimit() {
		return tryLimit;
	}

	public void setTryLimit(int tryLimit) {
		this.tryLimit = tryLimit;
	}

	public int getPriority() {
		return priority;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public java.sql.Timestamp getLastRunDate() {
		return lastRunDate;
	}

	public void setLastRunDate(java.sql.Timestamp lastRunDate) {
		this.lastRunDate = lastRunDate;
	}

}
