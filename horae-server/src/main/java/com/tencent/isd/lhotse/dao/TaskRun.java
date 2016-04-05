package com.tencent.isd.lhotse.dao;

/**
 * @author: cpwang
 * @date: 2012-5-16
 */
public class TaskRun implements Comparable<TaskRun> {
	private Task task;
	private String id;
	private int type;
	private String sourceServer;
	private String targetServer;
	private String action;
	private String cycleUnit;
	private int cycleNumber;
	private int selfDepend;
	private java.sql.Timestamp curRunDate;
	private java.sql.Timestamp nextRunDate;
	private int state;
	private int redoFlag;
	private int tries;
	private int tryLimit;
	private String runtimeId;
	private String runtimeBroker;
	private int runPriority;
	private java.sql.Timestamp startTime;
	private java.sql.Timestamp endTime;
	private java.sql.Timestamp lastUpdate;
	// for statistics
	private String name;
	private String inCharge;
	private float duration;
	private float flutter;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public String getCycleUnit() {
		return cycleUnit;
	}

	public void setCycleUnit(String cycleUnit) {
		this.cycleUnit = cycleUnit;
	}

	public int getCycleNumber() {
		return cycleNumber;
	}

	public void setCycleNumber(int cycleNumber) {
		this.cycleNumber = cycleNumber;
	}

	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}

	public int getTries() {
		return tries;
	}

	public void setTries(int tries) {
		this.tries = tries;
	}

	public String getRuntimeId() {
		return runtimeId;
	}

	public void setRuntimeId(String runtimeId) {
		this.runtimeId = runtimeId;
	}

	public int getRedoFlag() {
		return redoFlag;
	}

	public void setRedoFlag(int redoFlag) {
		this.redoFlag = redoFlag;
	}

	public String getRuntimeBroker() {
		return runtimeBroker;
	}

	public void setRuntimeBroker(String runtimeBroker) {
		this.runtimeBroker = runtimeBroker;
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

	public java.sql.Timestamp getEndTime() {
		return endTime;
	}

	public void setEndTime(java.sql.Timestamp endTime) {
		this.endTime = endTime;
	}

	public int getSelfDepend() {
		return selfDepend;
	}

	public void setSelfDepend(int selfDepend) {
		this.selfDepend = selfDepend;
	}

	public java.sql.Timestamp getLastUpdate() {
		return lastUpdate;
	}

	public void setLastUpdate(java.sql.Timestamp lastUpdate) {
		this.lastUpdate = lastUpdate;
	}

	public java.sql.Timestamp getStartTime() {
		return startTime;
	}

	public void setStartTime(java.sql.Timestamp startTime) {
		this.startTime = startTime;
	}

	public int getRunPriority() {
		return runPriority;
	}

	public void setRunPriority(int runPriority) {
		this.runPriority = runPriority;
	}

	public String getInCharge() {
		return inCharge;
	}

	public void setInCharge(String inCharge) {
		this.inCharge = inCharge;
	}

	public float getDuration() {
		return duration;
	}

	public void setDuration(float duration) {
		this.duration = duration;
	}

	public float getFlutter() {
		return flutter;
	}

	public void setFlutter(float flutter) {
		this.flutter = flutter;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
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

	public int getTryLimit() {
		return tryLimit;
	}

	public void setTryLimit(int tryLimit) {
		this.tryLimit = tryLimit;
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(TaskRun tr2) {
		// tries
		if (this.getTries() < tr2.getTries()) {
			return -1;
		}
		else if (this.getTries() > tr2.getTries()) {
			return 1;
		}

		// priority
		if (this.getRunPriority() < tr2.getRunPriority()) {
			return -1;
		}
		else if (this.getRunPriority() > tr2.getRunPriority()) {
			return 1;
		}

		// cycle unit
		Integer cycleVal1 = Task.getCycleUnitValue(this.getCycleUnit().toUpperCase());
		if (cycleVal1 == null) {
			return 1;
		}
		Integer cycleVal2 = Task.getCycleUnitValue(tr2.getCycleUnit().toUpperCase());
		if (cycleVal2 == null) {
			return -1;
		}
		if (cycleVal1.intValue() < cycleVal2.intValue()) {
			return -1;
		}
		else if (cycleVal1.intValue() > cycleVal2.intValue()) {
			return 1;
		}

		// run date
		if (this.getCurRunDate().before(tr2.getCurRunDate())) {
			return -1;
		}
		else if (this.getCurRunDate().after(tr2.getCurRunDate())) {
			return 1;
		}

		if (this.getId().equals(tr2.getId())) {
			return 0;
		}
		return 1;
	}

	public Task getTask() {
		return task;
	}

	public void setTask(Task task) {
		this.task = task;
	}
}
