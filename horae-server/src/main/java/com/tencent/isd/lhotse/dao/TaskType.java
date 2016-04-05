package com.tencent.isd.lhotse.dao;

/**
 * @author: cpwang
 * @date: 2012-3-15
 * @since: add serverTypes, killable, maxParallelism
 */
public class TaskType {
	private int id;
	private String description;
	private String sort;
	private boolean killable;
	private int serverParallelism;
	private int brokerParallelism;
	private int taskParallelism;
	private int pollingSeconds;
	private int retryWait;
	private String paramList;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getSort() {
		return sort;
	}

	public void setSort(String sort) {
		this.sort = sort;
	}

	public boolean isKillable() {
		return killable;
	}

	public void setKillable(boolean killable) {
		this.killable = killable;
	}

	public int getBrokerParallelism() {
		return brokerParallelism;
	}

	public void setBrokerParallelism(int brokerParallelism) {
		this.brokerParallelism = brokerParallelism;
	}

	public int getTaskParallelism() {
		return taskParallelism;
	}

	public void setTaskParallelism(int taskParallelism) {
		this.taskParallelism = taskParallelism;
	}

	public int getRetryWait() {
		return retryWait;
	}

	public void setRetryWait(int retryWait) {
		this.retryWait = retryWait;
	}

	public int getPollingSeconds() {
		return pollingSeconds;
	}

	public void setPollingSeconds(int pollingSeconds) {
		this.pollingSeconds = pollingSeconds;
	}

	public int getServerParallelism() {
		return serverParallelism;
	}

	public void setServerParallelism(int serverParallelism) {
		this.serverParallelism = serverParallelism;
	}

	public String getParamList() {
		return paramList;
	}

	public void setParamList(String paramList) {
		this.paramList = paramList;
	}

}
