package com.tencent.isd.lhotse.base;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import com.tencent.isd.lhotse.dao.Parameter;
import com.tencent.isd.lhotse.dao.Runner;
import com.tencent.isd.lhotse.dao.Server;
import com.tencent.isd.lhotse.dao.Status;
import com.tencent.isd.lhotse.dao.TaskType;
import com.tencent.isd.lhotse.proto.LhotseObject.LKV;
import com.tencent.isd.lhotse.proto.LhotseObject.LParameters;

/**
 * @author: cpwang
 * @date: 2012-12-12
 */
public class BaseCache {
	private static final String PARAMETER_FILE = "lhotse_param.properties";
	private static Vector<Server> servers = new Vector<Server>();
	private static ArrayList<Parameter> reservedParams = new ArrayList<Parameter>();
	private static ConcurrentHashMap<Integer, TaskType> taskTypes = new ConcurrentHashMap<Integer, TaskType>();
	private static ConcurrentHashMap<Integer, LParameters> typeParameters = new ConcurrentHashMap<Integer, LParameters>();
	private static Vector<Runner> runners = new Vector<Runner>();

	public static void init(LDBBroker broker) throws SQLException, IOException {
		initParameters();
		initTaskTypes(broker);
		initRunners(broker);
		initServers(broker);
	}

	private static void initParameters() throws IOException {
		// read properties file to get reserved parameters
		Properties props = new Properties();
		props.load(LhotseBase.class.getResourceAsStream(PARAMETER_FILE));

		Iterator<Entry<Object, Object>> iter = props.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<Object, Object> ent = iter.next();
			String key = (String) ent.getKey();
			String val = (String) ent.getValue();
			String[] vals = val.split(",");
			if ((vals == null) || (vals.length != 3)) {
				// not the format: default, min, max
				continue;
			}
			Parameter param = new Parameter();
			param.setName(key.trim().toLowerCase());
			try {
				param.setValue(vals[0]);
				param.setDefaultValue(Integer.parseInt(vals[0]));
				param.setMinimumValue(Integer.parseInt(vals[1]));
				param.setMaximumValue(Integer.parseInt(vals[2]));
			}
			catch (NumberFormatException e) {
				continue;
			}
			reservedParams.add(param);
		}
	}

	public static synchronized int initRunners(LDBBroker dbBroker) throws SQLException {
		runners.clear();
		ArrayList<Runner> tmpRunners = dbBroker.getRunners();
		for (Runner r : tmpRunners) {
			// clear runners not normal
			if (r.getStatus().equalsIgnoreCase(Status.NORMAL)) {
				runners.add(r);
			}
		}
		return runners.size();
	}

	public static Vector<Runner> getRunners() {
		return runners;
	}

	public static synchronized int[] initServers(LDBBroker broker) throws SQLException {
		// get single servers
		int s = 0;
		Vector<Server> tmp = broker.getServers();
		servers.clear();
		for (Server serv : tmp) {
			if (!serv.isGroup()) {
				serv.buildServer();
				servers.add(serv);
				++s;
			}
		}

		HashMap<String, ArrayList<String>> groups = broker.getServerGroups();
		Iterator<Entry<String, ArrayList<String>>> iter = groups.entrySet().iterator();
		Entry<String, ArrayList<String>> ent = null;
		int g = 0;
		while (iter.hasNext()) {
			ent = iter.next();
			Server group = null;
			for (Server serv : tmp) {
				if (serv.isGroup() && (ent.getKey().equalsIgnoreCase(serv.getTag()))) {
					group = serv;
					break;
				}
			}

			if (group == null) {
				continue;
			}

			for (String tag : ent.getValue()) {
				for (Server serv : tmp) {
					if (!serv.isGroup() && tag.equalsIgnoreCase(serv.getTag())) {
						group.getServers().add(serv.getServers().get(0));
						break;
					}
				}
			}
			servers.add(group);
			++g;
		}
		int[] r = new int[2];
		r[0] = s;
		r[1] = g;
		return r;
	}

	public static synchronized int initTaskTypes(LDBBroker broker) throws SQLException {
		Vector<TaskType> tmp = broker.getTaskTypes();
		taskTypes.clear();
		typeParameters.clear();

		for (TaskType tt : tmp) {
			taskTypes.put(tt.getId(), tt);
			// parse parameters
			String params = tt.getParamList();
			if ((params == null) || (params.trim().equals(""))) {
				continue;
			}

			try {
				StringReader sr = new StringReader(params);
				SAXBuilder sb = new SAXBuilder();
				Document doc = sb.build(sr);
				Element root = doc.getRootElement();
				if (root == null) {
					continue;
				}
				List<Element> elmParams = root.getChildren("parameter");
				LParameters.Builder lpb = LParameters.newBuilder();
				for (Element elm : elmParams) {
					String name = elm.getChildText("name");
					String value = elm.getChildText("value");
					LKV.Builder builder = LKV.newBuilder();
					builder.setKey(name);
					// builder.setValue(value);
					builder.setValue(value);
					LKV kv = builder.build();
					lpb.addParameters(kv);
				}
				LParameters lp = lpb.build();
				typeParameters.put(tt.getId(), lp);
			}
			catch (IOException e) {
				e.printStackTrace();
			}
			catch (JDOMException e) {
				e.printStackTrace();
			}
		}
		return taskTypes.size();
	}

	public synchronized static ArrayList<TaskType> getTaskTypes() {
		ArrayList<TaskType> tts = new ArrayList<TaskType>();
		Iterator<TaskType> valueIter = taskTypes.values().iterator();
		while (valueIter.hasNext()) {
			tts.add(valueIter.next());
		}
		return tts;
	}

	public synchronized static TaskType getTaskType(int type) {
		return taskTypes.get(type);
	}

	public static synchronized Runner getRunner(int taskType, String brokerIP) {
		for (Runner r : runners) {
			if ((r.getTaskType() == taskType) && (r.getBrokerIP().equalsIgnoreCase(brokerIP))) {
				return r;
			}
		}
		return null;
	}

	public static synchronized String getStringParameter(LDBBroker broker, String key) throws SQLException {
		// read database to get user-defined parameters
		Vector<Parameter> params = broker.getParameters();
		for (Parameter param : params) {
			if (param.getName().equalsIgnoreCase(key)) {
				return param.getValue();
			}
		}
		return null;
	}

	public static int getIntegerParameter(LDBBroker broker, String key) throws SQLException {
		Parameter reservedParam = getReservedParameter(key);
		Parameter param = null;
		Vector<Parameter> params = broker.getParameters();
		for (Parameter p : params) {
			if (p.getName().equalsIgnoreCase(key)) {
				param = p;
				break;
			}
		}

		if (param == null) {
			// no configuration in database,return reserved value
			return reservedParam.getDefaultValue();
		}

		int v = 0;
		try {
			v = Integer.parseInt(param.getValue());
		}
		catch (NumberFormatException e) {
			return reservedParam.getDefaultValue();
		}

		if (v < reservedParam.getMinimumValue()) {
			return reservedParam.getMinimumValue();
		}
		else if (v > reservedParam.getMaximumValue()) {
			return reservedParam.getMaximumValue();
		}

		return v;
	}

	private static Parameter getReservedParameter(String key) {
		for (Parameter resParam : reservedParams) {
			if (resParam.getName().equalsIgnoreCase(key)) {
				return resParam;
			}
		}
		return null;
	}

	public static Vector<Server> getServers() {
		return servers;
	}

	public static ConcurrentHashMap<Integer, LParameters> getTypeParameters() {
		return typeParameters;
	}

	public static LParameters getTypeParameters(int type) {
		return typeParameters.get(type);
	}
}
