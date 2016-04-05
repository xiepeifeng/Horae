package com.tencent.isd.lhotse.base.socket;

import java.util.HashMap;

/**
 * @author: cpwang
 * @date: 2012-11-26
 */
public class IssueProbe {
	public static final byte ALL_OK = 0;
	public static final byte PROTOBUF_BUILD_ERROR = -4;
	public static final byte INVALID_RUNNER_STATUS = -3;
	public static final byte RUNNER_NOT_AVAILABLE = -2;
	public static final byte TYPE_NOT_REGISTERED = -1;
	public static final byte REQUESTS_FULL = 1;
	public static final byte DATABASE_ERROR = 2;
	public static final byte NO_TASK_FOUND = 3;
	public static final byte NO_INSTANCE_FOUND = 4;
	public static final byte INVALID_RUN_DATE = 5;
	public static final byte INVALID_TASK_STATUS = 6;
	public static final byte BROKER_NOT_MATCHED = 7;
	public static final byte ILLEGAL_PRIORITY = 8;
	public static final byte PRIORITY_NOT_MATCHED = 9;
	public static final byte EXECUTE_TIME_NOT_ARRIVED = 10;
	public static final byte DELAY_TIME_NOT_ARRIVED = 11;
	public static final byte STARTUP_TIME_NOT_ARRIVED = 12;
	public static final byte ILLEGAL_SELF_DEPEND = 13;
	public static final byte NOT_EARLIEST_INSTANCE = 14;
	public static final byte TRY_OVER = 15;
	public static final byte RETRY_WAIT_NOT_ARRIVE = 16;
	public static final byte BROKER_FULL = 17;
	public static final byte SERVER_NOT_AVAILABLE = 18;
	public static final byte SERVER_FULL = 19;
	public static final byte TASK_FULL = 20;
	public static final byte HAS_RUNNING_INSTANCE = 21;
	public static final byte PARENT_TASK_NOT_FOUND = 22;
	public static final byte PARENT_INSTANCE_NOT_FOUND = 23;
	public static final byte PARENT_NOT_SUCCESSFUL = 24;
	public static final byte PARENT_CURRENT_DATE_NOT_MATCHED = 25;
	public static final byte PARENT_NEXT_DATE_NOT_MATCHED = 26;
	public static final byte INSTANCE_NOT_ENOUGH = 27;
	public static final byte INVALID_DEPEND_TYPE = 28;

	private static HashMap<Byte, String> descriptions = new HashMap<Byte, String>();

	static {
		descriptions.put(ALL_OK, "满足所有条件");
		descriptions.put(TYPE_NOT_REGISTERED, "类型未注册");
		descriptions.put(REQUESTS_FULL, "请求队列已满");
		descriptions.put(DATABASE_ERROR, "数据库连接失败");
		descriptions.put(NO_TASK_FOUND, "没有找到该类型的任务");
		descriptions.put(NO_INSTANCE_FOUND, "任务没有被实例化");
		descriptions.put(RUNNER_NOT_AVAILABLE, "Runner不存在");
		descriptions.put(INVALID_RUNNER_STATUS, "Runner状态无效（不为Y）");
		descriptions.put(INVALID_RUN_DATE, "数据时间无效");
		descriptions.put(INVALID_TASK_STATUS, "任务状态无效（不为Y）");
		descriptions.put(BROKER_NOT_MATCHED, "请求Broker与该任务所要求的IP不匹配");
		descriptions.put(ILLEGAL_PRIORITY, "优先级配置错误");
		descriptions.put(PRIORITY_NOT_MATCHED, "优先级不满足Broker优先级限制");
		descriptions.put(EXECUTE_TIME_NOT_ARRIVED, "任务未到指定的调度时间");
		descriptions.put(DELAY_TIME_NOT_ARRIVED, "未达到任务调度时间");
		descriptions.put(STARTUP_TIME_NOT_ARRIVED, "未达到任务每天可启动时间");
		descriptions.put(ILLEGAL_SELF_DEPEND, "自依赖配置错误");
		descriptions.put(NOT_EARLIEST_INSTANCE, "自身依赖任务的前一个实例尚未完成");
		descriptions.put(TRY_OVER, "运行次数达到上限");
		descriptions.put(RETRY_WAIT_NOT_ARRIVE, "没有达到重试时间间隔");
		descriptions.put(BROKER_FULL, "Broker已达到最大并发");
		descriptions.put(SERVER_NOT_AVAILABLE, "任务执行所用的服务器信息未配置");
		descriptions.put(SERVER_FULL, "服务器已达到最大并发");
		descriptions.put(TASK_FULL, "任务实例已达到最大并发");
		descriptions.put(HAS_RUNNING_INSTANCE, "该任务已有一个实例正在运行");
		descriptions.put(PARENT_TASK_NOT_FOUND, "未找到父任务");
		descriptions.put(PARENT_INSTANCE_NOT_FOUND, "未找到该父任务实例");
		descriptions.put(PARENT_NOT_SUCCESSFUL, "父任务中有实例未成功");
		descriptions.put(PARENT_CURRENT_DATE_NOT_MATCHED, "该任务与父任务开始时间窗口不匹配");
		descriptions.put(PARENT_NEXT_DATE_NOT_MATCHED, "该任务与父任务结束时间窗口不匹配");
		descriptions.put(INSTANCE_NOT_ENOUGH, "父任务实例数目不够");
		descriptions.put(INVALID_DEPEND_TYPE, "依赖类型配置错误");
	}

	private byte issueCode;
	private String errorValue;
	private String taskId;
	private java.sql.Timestamp runDate;

	public IssueProbe(String taskId, java.sql.Timestamp runDate) {
		this.taskId = taskId;
		this.runDate = runDate;
		this.issueCode = IssueProbe.ALL_OK;
	}

	private String getIssueCodeDescription(byte code) {
		return descriptions.get(code);
	}

	public void setFeedback(byte code, String value) {
		this.issueCode = code;
		this.errorValue = value;
	}

	public byte getIssueCode() {
		return issueCode;
	}

	public String getErrorValue() {
		return errorValue;
	}

	public String toString() {
		return getIssueCodeDescription(issueCode) + ":" + errorValue;
	}

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public java.sql.Timestamp getRunDate() {
		return runDate;
	}

	public void setRunDate(java.sql.Timestamp runDate) {
		this.runDate = runDate;
	}
}
