package com.tencent.isd.lhotse.dao;

import com.tencent.isd.lhotse.proto.LhotseObject.LTask;

/**
 * @author cpwang
 * @since 2009-3-8
 */
public class TaskLog extends DBLog {
	private String taskId;
	private String type;
	private int state;
	private String runTimeId;
	private java.sql.Timestamp curRunDate;
	private java.sql.Timestamp nextRunDate;
	private String description;
	private String runtimeBroker;
	private long logTime;
	private int tries;

	public TaskLog(String type, LTask task, String runtimeBroker) {
		this.taskId = task.getId();
		this.runTimeId = task.getRuntimeId();
		this.curRunDate = new java.sql.Timestamp(task.getCurRunDate());
		this.nextRunDate = new java.sql.Timestamp(task.getNextRunDate());
		this.type = type;
		this.state = task.getState().getNumber();
		this.description = task.getLogDesc();
		this.runtimeBroker = runtimeBroker;
		this.logTime = System.currentTimeMillis();
		this.tries = task.getTries();
	}

	public TaskLog(String type, TaskRun tr, String desc) {
		this.taskId = tr.getId();
		this.runTimeId = tr.getRuntimeId();
		this.curRunDate = tr.getCurRunDate();
		this.nextRunDate = tr.getNextRunDate();
		this.type = type;
		this.state = tr.getState();
		this.description = desc;
		this.logTime = System.currentTimeMillis();
		this.tries = tr.getTries();
	}

	public TaskLog() {
		this.logTime = System.currentTimeMillis();
	}

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getRunTimeId() {
		return runTimeId;
	}

	public void setRunTimeId(String runTimeId) {
		this.runTimeId = runTimeId;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public long getLogTime() {
		return logTime;
	}

	public void setLogTime(long logTime) {
		this.logTime = logTime;
	}

	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}

	public String getRuntimeBroker() {
		return runtimeBroker;
	}

	public void setRuntimeBroker(String runtimeBroker) {
		this.runtimeBroker = runtimeBroker;
	}

	public int getTries() {
		return tries;
	}

	public void setTries(int tries) {
		this.tries = tries;
	}

	public java.sql.Timestamp getCurRunDate() {
		return curRunDate;
	}

	public void setCurRunDate(java.sql.Timestamp curRunDate) {
		this.curRunDate = curRunDate;
	}

	public java.sql.Timestamp getNextRunDate() {
		return nextRunDate;
	}

	public void setNextRunDate(java.sql.Timestamp nextRunDate) {
		this.nextRunDate = nextRunDate;
	}

}
