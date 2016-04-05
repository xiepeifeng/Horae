package com.tencent.isd.lhotse.dao;

/**
 * @author: cpwang
 * @date: 2012-11-7
 */
public class Runner {
	private int taskType;
	private String brokerIP;
	private String inCharge;
	private String status;
	private String executablePath;
	private java.sql.Timestamp lastHeartbeat;
	private int priorityLimit;

	public String getBrokerIP() {
		return brokerIP;
	}

	public void setBrokerIP(String brokerIP) {
		this.brokerIP = brokerIP;
	}

	public String getInCharge() {
		return inCharge;
	}

	public void setInCharge(String inCharge) {
		this.inCharge = inCharge;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public int getPriorityLimit() {
		return priorityLimit;
	}

	public void setPriorityLimit(int priorityLimit) {
		this.priorityLimit = priorityLimit;
	}

	public int getTaskType() {
		return taskType;
	}

	public void setTaskType(int taskType) {
		this.taskType = taskType;
	}

	public java.sql.Timestamp getLastHeartbeat() {
		return lastHeartbeat;
	}

	public void setLastHeartbeat(java.sql.Timestamp lastHeartbeat) {
		this.lastHeartbeat = lastHeartbeat;
	}

	public String getExecutablePath() {
		return executablePath;
	}

	public void setExecutablePath(String executablePath) {
		this.executablePath = executablePath;
	}
}
