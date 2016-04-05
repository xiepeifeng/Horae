package com.tencent.isd.lhotse.dao;

/**
 * 
 * @author cpwang
 * @since 2012-3-15
 */
public class TaskLink implements Comparable<TaskLink> {
	private String taskFrom;
	private String taskTo;
	private int dependenceType;
	private String status;

	public String getTaskFrom() {
		return taskFrom;
	}

	public void setTaskFrom(String taskFrom) {
		this.taskFrom = taskFrom;
	}

	public String getTaskTo() {
		return taskTo;
	}

	public void setTaskTo(String taskTo) {
		this.taskTo = taskTo;
	}

	public int getDependenceType() {
		return dependenceType;
	}

	public void setDependenceType(int dependenceType) {
		this.dependenceType = dependenceType;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(TaskLink tl) {
		if ((this.getTaskFrom() == null) || (this.getTaskTo() == null)) {
			return 1;
		}

		if ((tl.getTaskFrom() == null) || (tl.getTaskTo() == null)) {
			return 1;
		}

		if ((this.getTaskFrom().equalsIgnoreCase(tl.getTaskFrom()))
				&& (this.getTaskTo().equalsIgnoreCase(tl.getTaskTo()))) {
			return 0;
		}
		return 1;
	}
}
