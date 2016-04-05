package com.tencent.isd.lhotse.runner.util;

import org.json.JSONObject;

import java.util.logging.Level;

public class DataQualityValueDist extends DataQualityOperator {

	@Override
	public boolean makeSql() {

		/*
		 * if (qaField != null) { taskId = getIntfName() + "_" + qaField + "_" +
		 * this.getInterfaceType() + "_" + qaType + "_" + qaField; } else {
		 * taskId = getIntfName() + "_" + "ALL" + "_" + this.getInterfaceType()
		 * + "_" + qaType + "_" + "ALL"; }
		 */

		StringBuffer sb = new StringBuffer();
		sb.append("SET tdw.groupname=nrtgroup;");
		sb.append("SET usp.param=" + plcParam + ";");
		sb.append("USE " + tdwDbName + ";");
		sb.append("INSERT  TABLE " + tdwTableName + " ");
		JSONObject jsonParam = null;
		try {
			if (parameters != null && parameters.trim().length() != 0) {
				jsonParam = new JSONObject(parameters);
			}
			if (jsonParam != null) {
				taskId = getIntfName() + "_" + (qaField != null ? qaField : "ALL") + "_"
						+ this.getInterfaceType() + "_" + qaType + "_"
						+ (jsonParam.isNull("groupby") ? "ALL" : jsonParam.get("groupby"));
			}
			else {
				taskId = getIntfName() + "_" + (qaField != null ? qaField : "ALL") + "_"
						+ this.getInterfaceType() + "_" + qaType + "_" + "ALL";
			}
			if (partitionKey == null || partitionKey.trim().length() == 0) {
				if (parameters == null || parameters.trim().length() == 0 || parameters.trim().equalsIgnoreCase("{}")) {
					sb.append(" select '" + getIntfName() + "', '" + qaField + "', " + qaField
							+ ",");
					sb.append(" count(1), '" + interfaceType + "', '" + qaType + "', '" + qaField
							+ "',");
					sb.append(" '" + taskId + "', '" + partitionValue + "', SYSTIMESTAMP(), '1', '"
							+ cycleUnit + "','" + (qaField != null ? qaField : "ALL") + "' from " + dbName + "::" + tableName);
					sb.append(" group by " + qaField + ";");
				}
				else {
					if (qaType.trim().equalsIgnoreCase("IN")) {
						sb.append(" select '" + getIntfName() + "', '" + qaField + "', " + qaField
								+ ",");
						sb.append(" count(1), '" + interfaceType + "', '" + qaType + "', '"
								+ qaField + "',");
						sb.append(" '" + taskId + "', '" + partitionValue
								+ "', SYSTIMESTAMP(), '1', '" + cycleUnit + "','" + (qaField != null ? qaField : "ALL") + "' from " + dbName
								+ "::" + tableName);
						sb.append(" where " + qaField + " IN (" + jsonParam.getString("rules")
								+ ") ");
						sb.append(" group by " + qaField + ";");
					}
					else {
						String[] paramArr = jsonParam.getString("rules").split(",");
						StringBuffer caseSb = new StringBuffer();
						int i = 1;
						caseSb.append("case ");
						for (String tmpStr : paramArr) {
							caseSb.append(" when " + qaField + " like '%" + tmpStr + "%' ");
							caseSb.append(" then " + i + " ");
							i++;
						}
						caseSb.append(" else " + i + " end ");

						sb.append(" select '" + getIntfName() + "', " + caseSb.toString() + ",");
						sb.append(" count(1), '" + interfaceType + "', '" + qaType + "', '"
								+ qaField + "',");
						sb.append(" '" + taskId + "', '" + partitionValue
								+ "', SYSTIMESTAMP(), '1', '" + cycleUnit + "','" + (qaField != null ? qaField : "ALL") + "' from " + dbName
								+ "::" + tableName);
						sb.append(" group by " + caseSb.toString() + " ;");
					}
				}
			}
			else {
				if (parameters == null || parameters.trim().length() == 0 || parameters.trim().equalsIgnoreCase("{}")) {
					sb.append(" select '" + getIntfName() + "', '" + qaField + "', " + qaField
							+ ",");
					sb.append(" count(1), '" + interfaceType + "', '" + qaType + "', '" + qaField
							+ "',");
					sb.append(" '" + taskId + "', '" + partitionValue + "', SYSTIMESTAMP(), '1', '"
							+ cycleUnit + "','" + (qaField != null ? qaField : "ALL") + "' from " + dbName + "::" + tableName);
					sb.append(" where " + partitionKey + " = '" + partitionValue + "' group by "
							+ qaField + ";");
				}
				else {
					if (qaType.trim().equalsIgnoreCase("IN")) {
						sb.append(" select '" + getIntfName() + "', '" + qaField + "', " + qaField
								+ ",");
						sb.append(" count(1), '" + interfaceType + "', '" + qaType + "', '"
								+ qaField + "',");
						sb.append(" '" + taskId + "', '" + partitionValue
								+ "', SYSTIMESTAMP(), '1', '" + cycleUnit + "','" + (qaField != null ? qaField : "ALL") + "' from " + dbName
								+ "::" + tableName);
						sb.append(" where " + qaField + " IN (" + jsonParam.getString("rules")
								+ ") ");
						sb.append(" and " + partitionKey + " = '" + partitionValue + "' group by "
								+ qaField + ";");
					}
					else {
						String[] paramArr = jsonParam.getString("rules").split(",");
						StringBuffer caseSb = new StringBuffer();
						int i = 1;
						caseSb.append("case ");
						for (String tmpStr : paramArr) {
							caseSb.append(" when " + qaField + " like '%" + tmpStr + "%' ");
							caseSb.append(" then " + i + " ");
							i++;
						}
						caseSb.append(" else " + i + " end ");

						sb.append(" select '" + getIntfName() + "', '" + qaField + "', "
								+ caseSb.toString() + ",");
						sb.append(" count(1), '" + interfaceType + "', '" + qaType + "', '"
								+ qaField + "',");
						sb.append(" '" + taskId + "', '" + partitionValue
								+ "', SYSTIMESTAMP(), '1', '" + cycleUnit + "','" + (qaField != null ? qaField : "ALL") + "' from " + dbName
								+ "::" + tableName);
						sb.append(" where " + partitionKey + " = '" + partitionValue
								+ "' group by " + caseSb.toString() + " ;");
					}
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			runner.writeLocalLog(Level.FINER, e.getMessage() + ":" + this.getClass().getName());
		}
		sqlStr = sb.toString();

		return true;
	}
}
