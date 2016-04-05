package com.tencent.isd.lhotse.runner.util;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.logging.Level;

public class DataQualityAggreCollector extends DataQualityOperator {

	@Override
	public boolean makeSql() {

		JSONObject jsonParam = null;
		StringBuffer sb = new StringBuffer();
		sb.append("SET tdw.groupname=nrtgroup;");
		sb.append("SET usp.param=" + plcParam + ";");
		sb.append("USE " + tdwDbName + ";");
		sb.append("INSERT  TABLE " + tdwTableName + " ");
		if (parameters != null && parameters.trim().length() > 0) {
			try {
				jsonParam = new JSONObject(parameters);
			}
			catch (JSONException e) {
				runner.writeLocalLog(Level.FINER, e.getMessage());
			}
		}
		try {

			if (jsonParam != null && jsonParam.has("groupby")) {
				taskId = getIntfName() + "_" + (qaField != null ? qaField : "ALL") + "_"
						+ this.getInterfaceType() + "_" + qaType + "_" + jsonParam.get("groupby");
			}
			else {
				taskId = getIntfName() + "_" + (qaField != null ? qaField : "ALL") + "_"
						+ this.getInterfaceType() + "_" + qaType + "_" + "ALL";
			}

			if (qaType.trim().equalsIgnoreCase("mod")) {
				if (partitionKey == null || partitionKey.trim().length() == 0) {
					if (jsonParam != null && jsonParam.has("groupby")) {

						sb.append(" select '" + getIntfName() + "', '" + qaField + "', " + qaType
								+ "(" + qaField + ",");
						sb.append(jsonParam.getInt("params") + ")," + "COUNT(1), ");
						sb.append(" '" + interfaceType + "', '" + qaType + "', '"
								+ jsonParam.getString("groupby") + "',");
						sb.append(" '" + taskId + "', '" + partitionValue
								+ "', SYSTIMESTAMP(), '1', '" + cycleUnit + "','"
								+ (qaField != null ? qaField : "ALL") + "'  from " + dbName + "::"
								+ tableName);
						//sb.append(" group by " + jsonParam.getString("groupby") + ";");

					}
					else {
						sb.append(" select '" + getIntfName() + "', '" + qaField + "', " + "'All'"
								+ ", ");
						sb.append(qaType + "(" + qaField + "," + jsonParam.getInt("params")
								+ "),");
						sb.append(" '" + interfaceType + "', '" + qaType + "', 'ALL',");
						sb.append(" '" + taskId + "', '" + partitionValue
								+ "', SYSTIMESTAMP(), '1', '" + cycleUnit + "','"
								+ (qaField != null ? qaField : "ALL") + "' from " + dbName + "::"
								+ tableName);
					}
				}
				else {
					if (jsonParam != null && jsonParam.has("groupby")) {
						sb.append(" select '" + getIntfName() + "', '" + qaField + "', " + qaType
								+ "(" + qaField + ",");
						sb.append(jsonParam.getInt("params") + ")," + "COUNT(1), ");
						sb.append(" '" + interfaceType + "', '" + qaType + "', '"
								+ jsonParam.getString("groupby") + "',");
						sb.append(" '" + taskId + "', '" + partitionValue
								+ "', SYSTIMESTAMP(), '1', '" + cycleUnit + "','"
								+ (qaField != null ? qaField : "ALL") + "' from " + dbName + "::"
								+ tableName);
						sb.append(" where " + partitionKey + " = '" + partitionValue + "'");
					}
					else {
						sb.append(" select '" + getIntfName() + "', '" + qaField + "', " + "'All'"
								+ ", ");
						sb.append(qaType + "(" + qaField + "," + jsonParam.getInt("params")
								+ "),");
						sb.append(" '" + interfaceType + "', '" + qaType + "', 'ALL',");
						sb.append(" '" + taskId + "', '" + partitionValue
								+ "', SYSTIMESTAMP(), '1', '" + cycleUnit + "','"
								+ (qaField != null ? qaField : "ALL") + "' from " + dbName + "::"
								+ tableName);
						sb.append(" where " + partitionKey + " = '" + partitionValue + "'");
					}
				}
				sb.append(" group by ");
				sb.append(qaType + "(" + qaField + ",");
			    sb.append(jsonParam.getInt("params") + ")");
			}
			else {
				if (partitionKey == null || partitionKey.trim().length() == 0) {
					if (jsonParam != null && jsonParam.has("groupby")) {
						sb.append(" select '" + getIntfName() + "', '" + qaField + "', "
								+ jsonParam.getString("groupby") + ", ");
						sb.append(qaType + "(" + qaField + ")," + " '" + interfaceType + "', '"
								+ qaType);
						sb.append("', '" + jsonParam.getString("groupby") + "',");
						sb.append(" '" + taskId + "', '" + partitionValue
								+ "', SYSTIMESTAMP(), '1', '" + cycleUnit + "','"
								+ (qaField != null ? qaField : "ALL") + "' from " + dbName + "::"
								+ tableName);
						sb.append(" group by " + jsonParam.getString("groupby") + ";");
					}
					else {
						sb.append(" select '" + getIntfName() + "', '" + qaField + "', " + "'All'"
								+ ", ");
						sb.append(qaType + "(" + qaField + ")," + " '" + interfaceType + "', '"
								+ qaType);
						sb.append("', 'ALL'," + " '" + taskId + "', '" + partitionValue
								+ "', SYSTIMESTAMP(), '1', '" + cycleUnit + "','"
								+ (qaField != null ? qaField : "ALL") + "' from " + dbName + "::"
								+ tableName);
					}
				}
				else {
					if (jsonParam != null && jsonParam.has("groupby")) {
						sb.append(" select '" + getIntfName() + "', '" + qaField + "', "
								+ jsonParam.getString("groupby") + ", ");
						sb.append(qaType + "(" + qaField + ")," + " '" + interfaceType + "', '"
								+ qaType);
						sb.append("', '" + jsonParam.getString("groupby") + "',");
						sb.append(" '" + taskId + "', '" + partitionValue
								+ "', SYSTIMESTAMP(), '1', '" + cycleUnit + "','"
								+ (qaField != null ? qaField : "ALL") + "' from " + dbName + "::"
								+ tableName);
						sb.append(" where " + partitionKey + " = " + partitionValue + " group by "
								+ jsonParam.getString("groupby") + ";");
					}
					else {
						sb.append(" select '" + getIntfName() + "', '" + qaField + "', " + "'All'"
								+ ", ");
						sb.append(qaType + "(" + qaField + ")," + " '" + interfaceType + "', '"
								+ qaType);
						sb.append("', 'ALL'," + " '" + taskId + "', '" + partitionValue
								+ "', SYSTIMESTAMP(), '1', '" + cycleUnit + "','"
								+ (qaField != null ? qaField : "ALL") + "' from " + dbName + "::"
								+ tableName);
						sb.append(" where " + partitionKey + " = '" + partitionValue + "';");
					}
				}
			}
		}
		catch (JSONException e) {
			runner.writeLocalLog(Level.FINER, e.getMessage() + ":" + this.getClass().getName());
		}

		sqlStr = sb.toString();

		return true;
	}
}
