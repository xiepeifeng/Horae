package com.tencent.isd.lhotse.dao;

/**
 * @author: cpwang
 * @date: 2012-10-8
 */
public class TaskRunSimple implements Comparable<TaskRunSimple> {
	private String id;
	private int type;
	private java.sql.Timestamp curRunDate;
	private java.sql.Timestamp nextRunDate;
	private int state;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
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

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(TaskRunSimple tr2) {
		if (this.getCurRunDate() == null) {
			return 1;
		}

		if (tr2.getCurRunDate() == null) {
			return -1;
		}

		return this.getCurRunDate().compareTo(tr2.getCurRunDate());
	}
}
