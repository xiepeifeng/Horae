package com.tencent.isd.lhotse.runner.util;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.logging.Level;

public class DataQualityRangeDist extends DataQualityOperator {

	@Override
	public boolean makeSql() {

		StringBuffer sb = new StringBuffer();
		sb.append("SET tdw.groupname=nrtgroup;");
		sb.append("SET usp.param=" + plcParam + ";");
		sb.append("USE " + tdwDbName + ";");
		sb.append("INSERT  TABLE " + tdwTableName + " ");
		JSONObject jsonParam = null;
		if (parameters != null && parameters.trim().length() > 0) {
			try {
				jsonParam = new JSONObject(parameters);

				if (jsonParam != null && jsonParam.has("groupby")) {
					taskId = getIntfName() + "_" + (qaField != null ? qaField : "ALL") + "_"
							+ this.getInterfaceType() + "_" + qaType + "_"
							+ jsonParam.get("groupby");
				}
				else {
					taskId = getIntfName() + "_" + (qaField != null ? qaField : "ALL") + "_"
							+ this.getInterfaceType() + "_" + qaType + "_" + "ALL";
				}
				String rules = jsonParam.getString("rules");
				String[] groupByTuple = rules.split("[c|C][A|a][S|s][E|e] .* [E|e][N|n][D|d]");
				String groupByField = null;
				if (groupByTuple.length >= 2) {
					int otherLength = 0;
					for (int ind = 1; ind < groupByTuple.length; ind++)
						otherLength += groupByTuple[ind].length();
					groupByField = rules.substring(0, rules.length() - otherLength);
				}
				else {
					groupByField = rules;
				}
				if (partitionKey == null || partitionKey.trim().length() == 0) {
					sb.append(" select '" + getIntfName() + "', '" + qaField + "', "
							+ jsonParam.getString("rules") + ",");
					sb.append(" count(1), '" + interfaceType + "', '" + qaType + "', '" + qaField
							+ "',");
					sb.append(" '" + taskId + "', '" + partitionValue + "', SYSTIMESTAMP(), '1', '"
							+ cycleUnit + "','" + (qaField != null ? qaField : "ALL") + "' from "
							+ dbName + "::" + tableName);
					sb.append(" group by " + groupByField + ";");
				}
				else {
					sb.append(" select '" + getIntfName() + "', '" + qaField + "', "
							+ jsonParam.getString("rules") + ",");
					sb.append(" count(1), '" + interfaceType + "', '" + qaType + "', '" + qaField
							+ "',");
					sb.append(" '" + taskId + "', '" + partitionValue + "', SYSTIMESTAMP(), '1', '"
							+ cycleUnit + "','" + (qaField != null ? qaField : "ALL") + "' from "
							+ dbName + "::" + tableName);
					sb.append(" where " + partitionKey + " = '" + partitionValue + "' group by "
							+ groupByField + ";");
				}
			}
			catch (JSONException e) {
				// TODO Auto-generated catch block
				runner.writeLocalLog(Level.FINER, e.getMessage() + ":" + this.getClass().getName());
			}
		}

		sqlStr = sb.toString();

		return true;
	}
}
