package com.tencent.isd.lhotse.runner.util;

import com.tencent.isd.lhotse.runner.AbstractTaskRunner;

import java.util.logging.Level;

public abstract class DataQualityOperator {

	protected String taskId = null;
	protected String dbName = null;
	protected String tableName = null;
	protected String partitionKey = null;
	protected String qaType = null;
	protected String qaField = null;
	protected String parameters = null;
	protected String partitionValue = null;
	protected int interfaceType = 0;
	protected String cycleUnit = null;
	protected String intfName = null;

	protected String plcProgram = "default_value";
	protected String serverIp = "default_value";
	protected String serverPort = "default_value";
	protected String plcUserName = "default_value";
	protected String plcPasswd = "default_value";
	public AbstractTaskRunner runner = null;
	
	protected final String tdwDbName = "ta";
	protected final String tdwTableName = "data_quality_record";

	protected String plcParam;

	protected String sqlStr = null;

	public abstract boolean makeSql();

	public boolean execute() {

		String[] cmdArray = new String[] { plcProgram, plcUserName, plcPasswd, serverIp, serverPort + " ", sqlStr };
		runner.writeLocalLog(Level.INFO, plcProgram+":"+ plcUserName+":"+ plcPasswd+":"+ serverIp+":"+ serverPort + " "+":"+ sqlStr);
		try {
			DDCRunTaskProcess process = new DDCRunTaskProcess(cmdArray, runner, true);
			process.startProcess();
			process.waitAndDestroyProcess();
			String lastString = process.getLastLine();
			if(process.getExitVal() == 0 && lastString.startsWith("TDW-00000")){
				runner.writeLocalLog(Level.INFO, "true------------------------------------------------");
				return true;
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}
	

	public void setPlcProgram(String plcProgram) {

		if (plcProgram != null && plcProgram.trim().length() > 0) {
			this.plcProgram = plcProgram;
		}
	}
	
	
	public String getCycleUnit() {
		return cycleUnit;
	}

	public void setCycleUnit(String cycleUnit) {
		this.cycleUnit = cycleUnit;
	}

	public String getServerIp() {
		return serverIp;
	}

	public int getInterfaceType() {
		return interfaceType;
	}

	public void setInterfaceType(int interfaceType) {
		this.interfaceType = interfaceType;
	}

	public void setServerIp(String serverIp) {
		this.serverIp = serverIp;
	}

	public String getPartitionValue() {
		return partitionValue;
	}

	public void setPartitionValue(String partitionValue) {
		this.partitionValue = partitionValue;
	}

	public String getServerPort() {
		return serverPort;
	}

	public void setServerPort(String serverPort) {
		this.serverPort = serverPort;
	}

	public void setPlcParam(String plcParam) {
		this.plcParam = plcParam;
	}

	public void setPlcUserName(String plcUserName) {
		this.plcUserName = plcUserName;
	}

	public void setPlcPasswd(String plcPasswd) {
		this.plcPasswd = plcPasswd;
	}

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getPartitionKey() {
		return partitionKey;
	}

	public void setPartitionKey(String partitionKey) {
		this.partitionKey = partitionKey;
	}

	public String getQaType() {
		return qaType;
	}

	public void setQaType(String qaType) {
		this.qaType = qaType;
	}

	public String getQaField() {
		return qaField;
	}

	public void setQaField(String qaField) {
		this.qaField = qaField;
	}

	public String getParameters() {
		return parameters;
	}

	public void setParameters(String parameters) {
		this.parameters = parameters;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getIntfName() {
		return intfName;
	}
	
	public void setIntfName( String intfName ) {
		this.intfName = intfName;
	}
}
