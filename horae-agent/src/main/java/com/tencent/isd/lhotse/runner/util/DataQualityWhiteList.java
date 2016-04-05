package com.tencent.isd.lhotse.runner.util;

import org.json.JSONObject;

import java.util.logging.Level;

public class DataQualityWhiteList extends DataQualityOperator {

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
		System.out.println(this.getClass().getName() + ":" + parameters);
		System.out.println(this.getClass().getName() + ":" + parameters.trim().length());
		if (parameters != null && parameters.trim().length() > 0) {
			try {
				System.out.println("step 1");
				jsonParam = new JSONObject(parameters);
				taskId = getIntfName() + "_" + (qaField != null ? qaField : "ALL") + "_"
						+ this.getInterfaceType() + "_" + qaType + "_"
						+ (jsonParam.isNull("params") ? "ALL" : jsonParam.get("params"));
				System.out.println("step 2");
				String rules = jsonParam.getString("rules");
				System.out.println("step 3");
				String standard = jsonParam.getString("target");
				System.out.println("step 4");
				String parameter = jsonParam.getString("params");
				System.out.println(this.getClass().getName() + ":" + rules);
				System.out.println(this.getClass().getName() + ":" + standard);
				String[] ruleElements = rules.split(",");
				String[] stdElements = standard.split(",");
				int len = Math.min(ruleElements.length, stdElements.length);
				StringBuffer caseWhenSb = new StringBuffer();
				caseWhenSb.append("case ");
				for (int ind = 0; ind < len; ind++) {
					String ruleElement = ruleElements[ind];
					String stdElement = stdElements[ind];
					String subSql = "when " + parameter + "=" + ruleElement + " and " + qaField
							+ "<>" + stdElement + " then 1 ";
					caseWhenSb.append(subSql);
				}
				caseWhenSb.append("else 0 end");
				if (partitionKey == null || partitionKey.trim().length() == 0) {
					sb.append(" select '" + getIntfName() + "', '" + qaField + "', ");
					sb.append(jsonParam.getString("params") + "," + caseWhenSb.toString() + ",'"
							+ interfaceType);
					sb.append("' " + ", '" + qaType + "', '" + qaField + "','" + taskId + "', '"
							+ partitionValue + "', SYSTIMESTAMP(), '1', '" + cycleUnit + "','"
							+ (qaField != null ? qaField : "ALL") + "' from " + dbName + "::"
							+ tableName);
					sb.append(" where " + jsonParam.getString("params") + " in ("
							+ jsonParam.getString("rules") + ")");
				}
				else {
					sb.append(" select '" + getIntfName() + "', '" + qaField + "', ");
					sb.append(jsonParam.getString("params") + "," + caseWhenSb.toString() + ",'"
							+ interfaceType);
					sb.append("' " + ", '" + qaType + "', '" + qaField + "','" + taskId + "', '"
							+ partitionValue + "', SYSTIMESTAMP(), '1', '" + cycleUnit + "','"
							+ (qaField != null ? qaField : "ALL") + "' from " + dbName + "::"
							+ tableName);
					sb.append(" where " + jsonParam.getString("params") + " in ("
							+ jsonParam.getString("rules") + ")");
					sb.append(" and " + partitionKey + " = '" + partitionValue + "';");
				}
			}
			catch (Exception e) {
				e.printStackTrace();
				runner.writeLocalLog(Level.FINER, e.getMessage() + ":" + this.getClass().getName());
			}
		}
		else {
			return false;
		}

		sqlStr = sb.toString();

		return true;
	}
}
