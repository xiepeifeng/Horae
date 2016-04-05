package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.util.*;
import org.json.JSONException;
import org.json.JSONObject;
import snaq.db.ConnectionPool;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;

public class DataQualityCollectRunner extends AbstractTaskRunner {

	private final String QUERY_SQL = "select * from qa_task qt left join "
			+ "qa_task_ext qte on qt.task_id = qte.task_id where qt.task_id = ?";
	private ArrayList<DataQualityOperator> taskList = null;
	protected final String PLC_PROGRAM = "plcProgram";

	private static ConnectionPool pools = null;
	private static boolean init = false;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("fuckall");
		TaskRunnerLoader.startRunner(DataQualityCollectRunner.class, (byte) 110);
	}

	public static void init() throws IOException, ClassNotFoundException, InstantiationException,
			IllegalAccessException, SQLException {

		if (!init) {
			FileInputStream fis = new FileInputStream("cfg/dataquality.properties");
			Properties prop = new Properties();
			prop.load(fis);

			String DB_URL = "jdbc:mysql://" + prop.getProperty("host") + ":" + prop.getProperty("port") + "/"
					+ prop.getProperty("dbname") + "?useUnicode=true&characterEncoding=UTF-8";
			Class<?> c = Class.forName("com.mysql.jdbc.Driver");
			Driver driver = (Driver) c.newInstance();
			DriverManager.registerDriver(driver);

			pools = new ConnectionPool("tdbankconfig", 3, 30, 50, 90, DB_URL, prop.getProperty("usename"),
					prop.getProperty("passwd"));

			init = true;
		}
	}

	public static ConnectionPool getConnectionPool() {
		return pools;
	}

	public void execute() throws IOException {

		try {
			DataQualityCollectRunner.init();
		}
		catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		LTask task = this.getTask();
		String result = null;
		String runtimeId = this.getTask().getId() + this.getTask().getCurRunDate();
		Map<String, String> keyValues = new HashMap<String, String>();
		try {
			if (!getVerifyTask(task.getId())) {
				this.writeLocalLog(Level.INFO, "1,Construct taskList Failed!");
			}

			StringBuffer sb = new StringBuffer();
			boolean bSucc = true;
			for (DataQualityOperator op : taskList) {
				sb.append(op.getQaType());
				if (!op.execute()) {
					bSucc = false;
					sb.append(":false;");
				}
				else {
					sb.append(":true;");
				}
				// sb.append(op.getPartitionValue());
				// sb.append(";" + op.getQaType());
				// sb.append(";" + op.getQaField());
				// sb.append(";" + op.getParameters());
			}
			if (sb.length() > 2000) {
				result = sb.substring(0, 2000);
			}
			else {
				result = sb.toString();
			}
			keyValues.put("result", result);
			if (bSucc) {
				commitTaskAndLog(keyValues, Level.INFO, LState.SUCCESSFUL, runtimeId);
			}
			else {
				commitTaskAndLog(keyValues, Level.INFO, LState.FAILED, runtimeId);
			}
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private boolean getVerifyTask(String taskId) throws SQLException, IOException, JSONException {
		taskList = new ArrayList<DataQualityOperator>();

		SimpleDateFormat sdf = null;
		String partitionValue = null;
		String cycleUnit = this.getTask().getCycleUnit();
		Calendar c1 = Calendar.getInstance();
		c1.setTimeInMillis(this.getTask().getCurRunDate());
		if (this.getTask().getCycleUnit().equalsIgnoreCase("M")) {
			sdf = new SimpleDateFormat("yyyyMM");
			partitionValue = sdf.format(c1.getTime());
		}
		else if (this.getTask().getCycleUnit().equalsIgnoreCase("D")) {
			sdf = new SimpleDateFormat("yyyyMMdd");
			partitionValue = sdf.format(c1.getTime());
		}
		else if (this.getTask().getCycleUnit().equalsIgnoreCase("H")) {
			sdf = new SimpleDateFormat("yyyyMMddHH");
			partitionValue = sdf.format(c1.getTime());
		}

		if (cycleUnit.equalsIgnoreCase("M")) {
			cycleUnit = "MonTask";
		}
		else if (cycleUnit.equalsIgnoreCase("W")) {
			cycleUnit = "WeekTask";
		}
		else if (cycleUnit.equalsIgnoreCase("D")) {
			cycleUnit = "DayTask";
		}
		else if (cycleUnit.equalsIgnoreCase("H")) {
			cycleUnit = "HourTask";
		}

		String plcProgram = this.requestGlobalParameters().get(PLC_PROGRAM);

		Connection conn = null;
		PreparedStatement pstat = null;
		ResultSet rs = null;
		try{
		conn = pools.getConnection();
		pstat = conn.prepareStatement(QUERY_SQL);
		LServer server = this.getSourceServer(0);
		pstat.setString(1, taskId);
		rs = pstat.executeQuery();
		while (rs.next()) {
			DataQualityOperator operator = null;
			int intefType = 0;
			String qaType = rs.getString("qa_type");
			String qaParameters = rs.getString("parameters");
			JSONObject object = new JSONObject(qaParameters);
			if(qaType.equals("COUNT") && object.isNull("groupby")){
				operator = new DataQualityRecordCount();
				intefType = 1;
			}
			else if (qaType.equals("WHITE_LIST")) {
				operator = new DataQualityWhiteList();
				intefType = 5;
			}
			else if (qaType.equals("RANGE")) {
				operator = new DataQualityRangeDist();
				intefType = 3;
			}
			else if (qaType.equals("IN") || qaType.equals("LIKE")) {
				operator = new DataQualityValueDist();
				intefType = 2;
			}
			else {
				operator = new DataQualityAggreCollector();
				intefType = 4;
			}

			// hive server info
			operator.setPlcProgram(plcProgram);
			operator.setPlcParam(taskId + "_" + partitionValue);
			operator.setServerIp(server.getHost());
			operator.setServerPort(String.valueOf(server.getPort()));
			operator.setPlcUserName(server.getUserName());
			operator.setPlcPasswd(server.getPassword());

			operator.setPartitionValue(partitionValue);
			operator.setQaType(rs.getNString("Qa_type"));
			operator.setQaField(rs.getNString("Qa_field"));
			operator.setParameters(rs.getNString("parameters"));
			operator.setPartitionKey(rs.getNString("Partition_key"));
			operator.setTableName(rs.getNString("Table_name"));
			operator.setDbName(rs.getNString("Db_name"));
			operator.setTaskId(taskId);
			operator.runner = this;
			operator.setInterfaceType(intefType);
			operator.setCycleUnit(cycleUnit);
			String creator = rs.getString("creater");
			operator.setIntfName(creator + "_" + "tdw_tl" + "_" + rs.getNString("Db_name") + "_" + rs.getNString("Table_name"));

			operator.makeSql();

			taskList.add(operator);
		}
		} finally{
			if(rs != null)
				rs.close();
			if(pstat != null)
				pstat.close();
			if(conn != null)
				conn.close();
		}

		return true;
	}

	private void commitTaskAndLog(Map<String, String> keyValues, Level logLevel, LState state, String runtimeId) {
		JSONObject jsonObject = new JSONObject();
		try {
			for (Map.Entry<String, String> keyValue : keyValues.entrySet()) {
				jsonObject.put(keyValue.getKey(), keyValue.getValue());
			}
			this.writeLocalLog(logLevel, jsonObject.toString());
			this.commitTask(state, runtimeId, jsonObject.toString());
		}
		catch (Exception e) {
			String st = CommonUtils.stackTraceToString(e);
			this.writeLocalLog(Level.INFO, "log_desc :" + jsonObject);
			this.writeLocalLog(Level.INFO, "log_desc length:" + jsonObject.length());
			this.writeLocalLog(Level.SEVERE, "commit task failed, StackTrace: " + st);
		}
	}

	public void kill() {

	}
}
