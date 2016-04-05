package com.tencent.isd.lhotse.runner.util;

import org.json.JSONObject;

public class DataQualityRecordCount extends DataQualityOperator {

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
		boolean success = false;
		JSONObject paramObject = null;
		try {
			paramObject = new JSONObject(this.parameters);
			taskId = getIntfName() + "_" + (qaField != null ? qaField : "ALL") + "_"
					+ this.getInterfaceType() + "_" + qaType + "_"
					+ (paramObject.isNull("groupby") ? "ALL" : paramObject.get("groupby"));
			if (partitionKey == null || partitionKey.trim().length() == 0) {
				if (paramObject.isNull("rules")
						|| !paramObject.getString("rules").equalsIgnoreCase("distinct")) {
					sb.append(" select '" + getIntfName() + "', 'ALL', 'ALL',");
					sb.append(" count(*), '" + interfaceType + "', '" + qaType + "', 'ALL',");
					sb.append(" '" + taskId + "', '" + partitionValue + "', SYSTIMESTAMP(), '1', '"
							+ cycleUnit + "','" + (qaField != null ? qaField : "ALL") + "' from "
							+ dbName + "::" + tableName);
				}
				else {
					sb.append(" select '" + getIntfName() + "', '" + qaField + "', 'ALL',");
					sb.append(" count(distinct " + qaField + "), '" + interfaceType + "', '"
							+ qaType + "', 'ALL',");
					sb.append(" '" + taskId + "', '" + partitionValue + "', SYSTIMESTAMP(), '1', '"
							+ cycleUnit + "','" + (qaField != null ? qaField : "ALL") + "' from "
							+ dbName + "::" + tableName);
				}
			}
			else {
				if (paramObject.isNull("rules")
						|| !paramObject.getString("rules").equalsIgnoreCase("distinct")) {
					sb.append(" select '" + getIntfName() + "', 'ALL', 'ALL',");
					sb.append(" count(*), '" + interfaceType + "', '" + qaType + "', 'ALL',");
					sb.append(" '" + taskId + "', '" + partitionValue + "', SYSTIMESTAMP(), '1', '"
							+ cycleUnit + "','" + (qaField != null ? qaField : "ALL") + "' from "
							+ dbName + "::" + tableName);
					sb.append(" where " + partitionKey + " = '" + partitionValue + "'");
				}
				else {
					sb.append(" select '" + getIntfName() + "', '" + qaField + "', 'ALL',");
					sb.append(" count(distinct " + qaField + "), '" + interfaceType + "', '"
							+ qaType + "', 'ALL',");
					sb.append(" '" + taskId + "', '" + partitionValue + "', SYSTIMESTAMP(), '1', '"
							+ cycleUnit + "','" + (qaField != null ? qaField : "ALL") + "' from "
							+ dbName + "::" + tableName);
					sb.append(" where " + partitionKey + " = '" + partitionValue + "'");
				}
			}
			sqlStr = sb.toString();
			success = true;
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		return success;
	}
}
