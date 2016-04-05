package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.runner.AbstractTaskRunner;
import com.tencent.isd.lhotse.runner.util.CommonUtils;
import org.apache.commons.lang.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.logging.Level;

public abstract class WrapperRunner extends AbstractTaskRunner {

	protected void commitTaskAndLog(LState state, String runtimeId, String desc) {
		try {
			commitTask(state, runtimeId, desc);
		}
		catch (Exception e) {
			String st = CommonUtils.stackTraceToString(e);
			this.writeLocalLog(Level.INFO, "Log_desc :" + desc);
			this.writeLocalLog(Level.INFO, "Log_desc length:" + desc.length());
			this.writeLocalLog(Level.SEVERE, "Commit task failed, StackTrace: " + st);
		}
	}

	protected void putJson(JSONObject json, String key, String value) {
		try {
			json.put(key, value);
		}
		catch (JSONException e) {
			this.writeLocalLog(Level.SEVERE, e.getMessage());
		}
	}

	protected String getExtPropValueWithDefault(String key, String defaultValue) {
		String value = this.getExtPropValue(key);
		if (StringUtils.isBlank(value))
			value = defaultValue;
		return value;
	}

	protected void mergekeyValue(StringBuffer sb, String key, Object value) {
		String valueStr = CommonUtils.convertToXMLFormat(String.valueOf(value));
		sb.append("${").append(key).append("}=").append(valueStr).append("\n");
		this.writeLocalLog(Level.INFO, "${" + key + "}=" + valueStr);
	}

}	
