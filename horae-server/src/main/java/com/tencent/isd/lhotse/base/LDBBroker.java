package com.tencent.isd.lhotse.base;

/**
 * @author cpwang
 * @since 2012-3-23
 */
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;
import com.tencent.isd.lhotse.dao.Command;
import com.tencent.isd.lhotse.dao.Parameter;
import com.tencent.isd.lhotse.dao.Runner;
import com.tencent.isd.lhotse.dao.Server;
import com.tencent.isd.lhotse.dao.StatRange;
import com.tencent.isd.lhotse.dao.Status;
import com.tencent.isd.lhotse.dao.SysLog;
import com.tencent.isd.lhotse.dao.Task;
import com.tencent.isd.lhotse.dao.TaskLink;
import com.tencent.isd.lhotse.dao.TaskLog;
import com.tencent.isd.lhotse.dao.TaskRun;
import com.tencent.isd.lhotse.dao.TaskRunSimple;
import com.tencent.isd.lhotse.dao.TaskType;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.util.LDBUtil;

public class LDBBroker {
    public static final String TASK_LOG_TABLE = "lb_task_log";
    public static final String SYS_LOG_TABLE  = "lb_sys_log";
    private String             url            = null;
    private String             user           = null;
    private String             password       = null;
    private String             database       = null;

    private Connection         conn;


    public LDBBroker() throws ClassNotFoundException, SQLException, IOException {
        this(true);
    }


    public LDBBroker(boolean primaryOrSecondary)
            throws ClassNotFoundException, SQLException, IOException {
        if (primaryOrSecondary) {
            initConn("primary_db_ip", "primary_db_port", "primary_db_database",
                    "primary_db_username", "primary_db_password");
        } else {
            initConn("secondary_db_ip", "secondary_db_port", "secondary_db_database",
                    "secondary_db_username", "secondary_db_password");
        }
    }


    private void initConn(String ipCfg, String portCfg, String databaseCfg, String usernameCfg,
                          String passwordCfg)
                                  throws ClassNotFoundException, SQLException, IOException {
        FileInputStream fis = new FileInputStream(LhotseBase.BASE_CFG_PATH);
        Properties props = new Properties();
        props.load(fis);
        String ip = props.getProperty(ipCfg);
        String port = props.getProperty(portCfg);
        this.database = props.getProperty(databaseCfg);
        this.user = props.getProperty(usernameCfg);
        this.password = props.getProperty(passwordCfg);
        fis.close();
        this.url = "jdbc:mysql://" + ip + ":" + port + "/" + this.database
                + "?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull";
        this.conn = LDBUtil.getConnection(LDBUtil.MYSQL_DRIVER, url, user, password);
    }


    public void setConf(String ip, String port, String db, String user, String pswd) {
        this.database = db;
        this.user = user;
        this.password = pswd;
        this.url = "jdbc:mysql://" + ip + ":" + port + "/" + this.database
                + "?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull";
    }


    public void refreshConnection() throws SQLException, ClassNotFoundException {
        if ((this.conn == null) || this.conn.isClosed()) {
            this.conn = LDBUtil.getConnection(LDBUtil.MYSQL_DRIVER, url, user, password);
        }
    }


    public void close() {
        if (this.conn != null) {
            try {
                if (!this.conn.isClosed()) {
                    this.conn.close();
                }
            } catch (SQLException e) {
                System.err.println("LDBBroker.close():" + e.getMessage());
            }
        }
    }


    public boolean isClosed() throws SQLException {
        if (this.conn != null) {
            return this.conn.isClosed();
        }
        return true;
    }


    private String getSchema() {
        return this.database.toLowerCase();
    }


    public Connection getConnection() {
        return conn;
    }

    // ///////////////////////////////////////////////////////////////////////////////////////////////////
    private static final String SEL_SERV_SQL = "SELECT server_tag, server_type, host, port, service, user_name, "
            + "user_group, password, is_group, server_version, parallelism, special_reserve,"
            + "special_priority FROM lb_server";


    public Vector<Server> getServers() throws SQLException {
        Vector<Server> servers = new Vector<Server>();
        Statement stmt = this.getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(SEL_SERV_SQL);
        while (rs.next()) {
            Server serv = new Server();
            serv.setTag(rs.getString("server_tag").trim());
            serv.setType(rs.getString("server_type"));
            serv.setHost(rs.getString("host"));
            serv.setPort(rs.getInt("port"));
            serv.setService(rs.getString("service"));
            serv.setUserName(rs.getString("user_name"));
            serv.setGroup(rs.getInt("is_group") == 1);
            serv.setPassword(rs.getString("password"));
            serv.setUserGroup(rs.getString("user_group"));
            serv.setVersion(rs.getString("server_version"));
            serv.setParallelism(rs.getInt("parallelism"));
            serv.setSpecialReserve(rs.getInt("special_reserve"));
            serv.setSpecialReserve(rs.getInt("special_priority"));
            servers.add(serv);
        }
        rs.close();
        stmt.close();
        return servers;
    }

    // ///////////////////////////////////////////////////////////////////////////////////////////////////
    private static final String SEL_SERV_GRP_SQL = "SELECT group_tag, server_tag FROM lb_server_group ORDER BY order_id";


    public HashMap<String, ArrayList<String>> getServerGroups() throws SQLException {
        HashMap<String, ArrayList<String>> serverGroups = new HashMap<String, ArrayList<String>>();
        Statement stmt = this.getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(SEL_SERV_GRP_SQL);
        while (rs.next()) {
            String groupTag = rs.getString("group_tag");
            String serverTag = rs.getString("server_tag");
            ArrayList<String> serverTags = serverGroups.get(groupTag);
            if (serverTags == null) {
                serverTags = new ArrayList<String>();
                serverGroups.put(groupTag, serverTags);
            }
            serverTags.add(serverTag);
        }
        rs.close();
        stmt.close();
        return serverGroups;
    }

    // ///////////////////////////////////////////////////////////////////////////////////////////////////
    private static final String SEL_TASKEXT_SQL = "SELECT prop_name,prop_value FROM lb_task_ext WHERE task_id = ?";


    public HashMap<String, String> getTaskExtProps(String taskId) throws SQLException {
        HashMap<String, String> eps = new HashMap<String, String>();
        PreparedStatement pstmt = this.getConnection().prepareStatement(SEL_TASKEXT_SQL);
        pstmt.setString(1, taskId);
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()) {
            String pn = rs.getString("prop_name");
            if (pn == null) {
                pn = "";
            }
            String pv = rs.getString("prop_value");
            if (pv == null) {
                pv = "";
            }
            eps.put(pn, pv);
        }
        rs.close();
        pstmt.close();
        return eps;
    }

    // ///////////////////////////////////////////////////////////////////////////////////////////////////
    private static final String SEL_TASK_BY_TYPE_SQL = "SELECT task_id,task_type,task_name, source_server,target_server, "
            + "cycle_unit,cycle_num, start_date, end_date, self_depend, task_action, delay_time, startup_time, "
            + "task_type, try_limit, task_group, notes, broker_ip, in_charge, create_time, alive_wait,status, "
            + "last_update FROM lb_task WHERE task_type = ?";


    /**
     * get task by id
     * 
     * @param where
     * @return
     * @throws SQLException
     */
    public ArrayList<Task> getTasks(int taskType) throws SQLException {
        ArrayList<Task> tasks = new ArrayList<Task>();
        PreparedStatement pstmt = this.getConnection().prepareStatement(SEL_TASK_BY_TYPE_SQL);
        pstmt.setInt(1, taskType);
        ResultSet rs = pstmt.executeQuery();
        Task t = null;
        while (rs.next()) {
            t = new Task();
            String taskId = rs.getString("task_id");
            t.setId(taskId);
            t.setType(rs.getInt("task_type"));
            t.setName(rs.getString("task_name"));
            t.setSourceServer(rs.getString("source_server"));
            t.setTargetServer(rs.getString("target_server"));
            t.setCycleUnit(rs.getString("cycle_unit"));
            t.setCycleNumber(rs.getInt("cycle_num"));
            t.setStartDate(rs.getTimestamp("start_date"));
            t.setEndDate(rs.getTimestamp("end_date"));
            t.setSelfDepend(rs.getInt("self_depend"));
            t.setAction(rs.getString("task_action"));
            t.setDelayTime(rs.getInt("delay_time"));
            t.setStartupTime(rs.getInt("startup_time"));
            t.setAliveWait(rs.getInt("alive_wait"));
            t.setType(rs.getInt("task_type"));
            t.setTryLimit(rs.getInt("try_limit"));
            t.setInCharge(rs.getString("in_charge"));
            t.setStatus(rs.getString("status"));
            t.setBrokerIP(rs.getString("broker_ip"));

            tasks.add(t);
        }
        rs.close();
        pstmt.close();
        return tasks;
    }

    // ///////////////////////////////////////////////////////////////////////////////////////////////////
    private static final String SEL_TASK_BY_ID_SQL = "SELECT task_id,task_type,task_name, source_server,target_server, "
            + "cycle_unit,cycle_num, start_date, end_date, self_depend, task_action, delay_time, startup_time, "
            + "task_type, try_limit, task_group, notes, broker_ip, in_charge, create_time, "
            + "alive_wait,status, last_update FROM lb_task WHERE task_id=?";


    /**
     * get task by id
     * 
     * @param where
     * @return
     * @throws SQLException
     */
    public Task getTask(String id) throws SQLException {
        PreparedStatement pstmt = this.getConnection().prepareStatement(SEL_TASK_BY_ID_SQL);
        pstmt.setString(1, id);
        ResultSet rs = pstmt.executeQuery();
        Task t = null;
        if (rs.next()) {
            t = new Task();
            t.setId(rs.getString("task_id"));
            t.setType(rs.getInt("task_type"));
            t.setName(rs.getString("task_name"));
            t.setSourceServer(rs.getString("source_server"));
            t.setTargetServer(rs.getString("target_server"));
            t.setCycleUnit(rs.getString("cycle_unit"));
            t.setCycleNumber(rs.getInt("cycle_num"));
            t.setStartDate(rs.getTimestamp("start_date"));
            t.setEndDate(rs.getTimestamp("end_date"));
            t.setSelfDepend(rs.getInt("self_depend"));
            t.setAction(rs.getString("task_action"));
            t.setDelayTime(rs.getInt("delay_time"));
            t.setStartupTime(rs.getInt("startup_time"));
            t.setAliveWait(rs.getInt("alive_wait"));
            t.setType(rs.getInt("task_type"));
            t.setTryLimit(rs.getInt("try_limit"));
            t.setInCharge(rs.getString("in_charge"));
            t.setStatus(rs.getString("status"));
            t.setBrokerIP(rs.getString("broker_ip"));
        }
        rs.close();
        pstmt.close();
        return t;
    }

    // ///////////////////////////////////////////////////////////////////////////////////////////////////
    private static final String SEL_RUNNER_SQL = "SELECT type_id, broker_ip, executable_path, in_charge, "
            + "status, startup_time, last_heartbeat, priority_limit FROM lb_runner";


    public ArrayList<Runner> getRunners() throws SQLException {
        ArrayList<Runner> runners = new ArrayList<Runner>();
        Statement pstmt = this.getConnection().createStatement();
        ResultSet rs = pstmt.executeQuery(SEL_RUNNER_SQL);
        while (rs.next()) {
            Runner r = new Runner();
            r.setTaskType(rs.getInt("type_id"));
            r.setBrokerIP(rs.getString("broker_ip").trim());
            r.setInCharge(rs.getString("in_charge"));
            r.setStatus(rs.getString("status"));
            r.setLastHeartbeat(rs.getTimestamp("last_heartbeat"));
            r.setPriorityLimit(rs.getInt("priority_limit"));
            r.setExecutablePath(rs.getString("executable_path"));
            runners.add(r);
        }
        rs.close();
        pstmt.close();
        return runners;
    }

    // ///////////////////////////////////////////////////////////////////////////////////////////////////
    private static final String SEL_TT_SQL = "SELECT type_id,type_desc,type_sort,killable,retry_wait,"
            + "broker_parallelism,task_parallelism,polling_seconds,param_list FROM lb_task_type";


    public Vector<TaskType> getTaskTypes() throws SQLException {
        Vector<TaskType> tts = new Vector<TaskType>();
        Statement stmt = this.getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(SEL_TT_SQL);
        while (rs.next()) {
            TaskType tt = new TaskType();
            tt.setId(rs.getInt("type_id"));
            tt.setDescription(rs.getString("type_desc"));
            tt.setSort(rs.getString("type_sort"));
            tt.setKillable((rs.getInt("killable") == 1));
            tt.setBrokerParallelism(rs.getInt("broker_parallelism"));
            tt.setTaskParallelism(rs.getInt("task_parallelism"));
            tt.setRetryWait(rs.getInt("retry_wait"));
            tt.setPollingSeconds(rs.getInt("polling_seconds"));
            tt.setParamList(rs.getString("param_list"));
            tts.add(tt);
        }
        rs.close();
        stmt.close();
        return tts;
    }

    // ///////////////////////////////////////////////////////////////////////////////////////////////////
    private static final String SEL_PARAM_SQL = "select param_name,param_value,value_type,param_owner from lb_parameter";


    public Vector<Parameter> getParameters() throws SQLException {
        PreparedStatement pstmt = this.getConnection().prepareStatement(SEL_PARAM_SQL);
        Vector<Parameter> params = new Vector<Parameter>();
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()) {
            Parameter param = new Parameter();
            param.setName(rs.getString("param_name"));
            param.setValue(rs.getString("param_value"));
            param.setValueType(rs.getString("value_type"));
            param.setOwner(rs.getString("param_owner"));
            params.add(param);
        }
        rs.close();
        pstmt.close();
        return params;
    }


    // //////////////////////////////////////////////////////////////////////////////////////////
    public java.sql.Timestamp getSysTimestamp() throws SQLException {
        Statement stmt = this.getConnection().createStatement();
        ResultSet rs = stmt.executeQuery("SELECT now()");
        java.sql.Timestamp ts = null;
        if (rs.next()) {
            ts = rs.getTimestamp(1);
        }
        rs.close();
        stmt.close();
        return ts;
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String SEL_TASKRUN_SQL = "SELECT task_id, cur_run_date, next_run_date, state, "
            + "task_type, redo_flag, tries, runtime_id, runtime_broker, end_time, run_priority, try_limit, "
            + "last_update FROM lb_task_run WHERE task_type = ? AND tries<>try_limit AND state <> "
            + LState.SUCCESSFUL_VALUE;


    public LinkedList<TaskRun> getUnsuccessfulRuns(int type) throws SQLException {
        LinkedList<TaskRun> taskRuns = new LinkedList<TaskRun>();
        PreparedStatement pstmt = this.getConnection().prepareStatement(SEL_TASKRUN_SQL);
        pstmt.setInt(1, type);
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()) {
            TaskRun tr = new TaskRun();
            tr.setId(rs.getString("task_id"));
            tr.setCurRunDate(rs.getTimestamp("cur_run_date"));
            tr.setNextRunDate(rs.getTimestamp("next_run_date"));
            tr.setState(rs.getInt("state"));
            tr.setType(rs.getInt("task_type"));
            tr.setRedoFlag(rs.getInt("redo_flag"));
            tr.setTries(rs.getInt("tries"));
            tr.setRunPriority(rs.getInt("run_priority"));
            tr.setTryLimit(rs.getInt("try_limit"));
            tr.setRuntimeId(rs.getString("runtime_id"));
            tr.setRuntimeBroker(rs.getString("runtime_broker"));
            tr.setEndTime(rs.getTimestamp("end_time"));
            tr.setLastUpdate(rs.getTimestamp("last_update"));
            taskRuns.add(tr);
        }
        rs.close();
        pstmt.close();
        return taskRuns;
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    /*
     * private static final String INIT_TASKRUN_SQL =
     * "INSERT INTO lb_task_run(task_id, cur_run_date, " +
     * "next_run_date, state, redo_flag, tries, last_update,start_time,end_time,runtime_id,"
     * +
     * "runtime_broker,task_type,run_priority,try_limit) SELECT task_id, cur_run_date, next_run_date, "
     * + LState.READY_VALUE +
     * ", 0, 0, NOW(), NULL, NULL, NULL, NULL, task_type, task_priority,try_limit FROM (SELECT task_id, "
     * +
     * "task_type, cur_run_date, (CASE UPPER(cycle_unit) WHEN 'I' THEN DATE_ADD(cur_run_date, "
     * +
     * "INTERVAL cycle_num MINUTE) WHEN 'H' THEN DATE_ADD(cur_run_date, INTERVAL cycle_num HOUR"
     * +
     * ") WHEN 'D' THEN DATE_ADD(cur_run_date, INTERVAL cycle_num DAY) WHEN 'W' THEN DATE_ADD("
     * +
     * "cur_run_date, INTERVAL cycle_num WEEK) WHEN 'M' THEN DATE_ADD(cur_run_date, "
     * + "INTERVAL cycle_num MONTH) WHEN '" + Task.ONEOFF_CYCLE +
     * "' THEN end_date END) AS next_run_date, task_priority, delay_time,try_limit FROM ("
     * +
     * "SELECT v1.task_id, v1.task_type, v1.end_date, v1.cycle_unit, v1.cycle_num, IFNULL("
     * +
     * "max_next_run_date,start_date) AS cur_run_date, max_next_run_date, task_priority, delay_time,"
     * +
     * "try_limit FROM (SELECT task_id, cycle_unit, cycle_num, start_date, end_date, task_type, "
     * +
     * "task_priority, delay_time,try_limit FROM lb_task WHERE task_type = ? AND UPPER(STATUS) = '"
     * + Status.NORMAL + "' AND NOW() >= start_date AND NOT (cycle_unit='" +
     * Task.ONEOFF_CYCLE + "' AND end_date IS NULL )) AS v1 LEFT JOIN (" +
     * "SELECT task_id, MAX(next_run_date) AS max_next_run_date FROM lb_task_run WHERE task_type = "
     * +
     * "? GROUP BY task_id) AS v2 ON (v1.task_id = v2.task_id)) v3 WHERE cur_run_date <= "
     * +
     * "IFNULL(end_date, cur_run_date) AND NOT (max_next_run_date IS NOT NULL AND cycle_unit = '"
     * + Task.ONEOFF_CYCLE +
     * "')) v4 WHERE NOW() >= DATE_ADD(next_run_date,INTERVAL delay_time MINUTE)"
     * ; public int insertTaskRun(int taskType) throws SQLException {
     * PreparedStatement pstmt =
     * this.getConnection().prepareStatement(INIT_TASKRUN_SQL); pstmt.setInt(1,
     * taskType); pstmt.setInt(2, taskType); int rows = pstmt.executeUpdate();
     * pstmt.close(); return rows; }
     */
    private static final String SEL_TASK_INIT_SQL = "SELECT v1.task_id,v1.task_type,v1.start_date, v1.end_date,"
            + "v1.cycle_unit,v1.cycle_num, max_run_date,task_priority,delay_time,try_limit FROM ("
            + "SELECT task_id, cycle_unit,cycle_num,start_date,end_date,task_type,task_priority,delay_time,"
            + "try_limit FROM lb_task WHERE task_type = ? AND UPPER(STATUS) = '" + Status.NORMAL
            + "' AND NOW() >= start_date AND NOT (cycle_unit = '" + Task.ONEOFF_CYCLE
            + "' AND end_date IS NULL)) AS v1 LEFT JOIN (SELECT task_id, "
            + "MAX(cur_run_date) AS max_run_date FROM lb_task_run WHERE task_type = ? GROUP BY task_id"
            + ") AS v2 ON (v1.task_id = v2.task_id)";


    public ArrayList<Task> getTasksToInitialize(int taskType) throws SQLException {
        ArrayList<Task> tasks = new ArrayList<Task>();
        PreparedStatement pstmt = this.getConnection().prepareStatement(SEL_TASK_INIT_SQL);
        pstmt.setInt(1, taskType);
        pstmt.setInt(2, taskType);
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()) {
            Task t = new Task();
            t.setId(rs.getString("v1.task_id"));
            t.setType(rs.getInt("task_type"));
            t.setStartDate(rs.getTimestamp("start_date"));
            t.setEndDate(rs.getTimestamp("end_date"));
            t.setCycleUnit(rs.getString("cycle_unit"));
            t.setCycleNumber(rs.getInt("cycle_num"));
            t.setLastRunDate(rs.getTimestamp("max_run_date"));
            t.setPriority(rs.getInt("task_priority"));
            t.setDelayTime(rs.getInt("delay_time"));
            t.setTryLimit(rs.getInt("try_limit"));
            tasks.add(t);
        }
        rs.close();
        pstmt.close();
        return tasks;
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String UPD_START_SQL = "UPDATE lb_task_run SET state =?,runtime_broker=?,tries=?,last_log=?,"
            + "start_time=now(),last_update=now() WHERE task_id=? AND cur_run_date=? AND task_type=?";


    public int updateTaskRunStart(int taskType, String taskId, long curRunDate, int state,
                                  String brokerIP, int tries, String lastLog) throws SQLException {
        PreparedStatement pstmt = this.getConnection().prepareStatement(UPD_START_SQL);
        pstmt.setInt(1, state);
        pstmt.setString(2, brokerIP);
        pstmt.setInt(3, tries);
        pstmt.setString(4, lastLog);
        pstmt.setString(5, taskId);
        pstmt.setTimestamp(6, new java.sql.Timestamp(curRunDate));
        pstmt.setInt(7, taskType);
        int r = pstmt.executeUpdate();
        pstmt.close();
        return r;
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String UPD_END_SQL = "UPDATE lb_task_run SET state=?, runtime_id=?, last_log=?,"
            + "end_time=now(), last_update=now() WHERE task_id=? AND cur_run_date=? AND task_type=?";


    public int updateTaskRunEnd(int taskType, String taskId, long curRunDate, int state,
                                String runtimeId, String lastLog) throws SQLException {
        PreparedStatement pstmt = this.getConnection().prepareStatement(UPD_END_SQL);
        pstmt.setInt(1, state);
        pstmt.setString(2, runtimeId == null ? "" : runtimeId);
        pstmt.setString(3, lastLog == null ? "" : lastLog);
        pstmt.setString(4, taskId);
        pstmt.setTimestamp(5, new java.sql.Timestamp(curRunDate));
        pstmt.setInt(6, taskType);
        int r = pstmt.executeUpdate();
        pstmt.close();
        return r;
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String UPD_RUNNING_SQL = "UPDATE lb_task_run SET state=?, runtime_id=?,last_log=?, "
            + "last_update=now() WHERE task_id=? AND cur_run_date=? AND task_type=?";


    public int updateTaskRunRunning(int taskType, String taskId, long curRunDate, int state,
                                    String runtimeId, String lastLog) throws SQLException {
        PreparedStatement pstmt = this.getConnection().prepareStatement(UPD_RUNNING_SQL);
        pstmt.setInt(1, state);
        pstmt.setString(2, runtimeId == null ? "" : runtimeId);
        pstmt.setString(3, lastLog == null ? "" : lastLog);
        pstmt.setString(4, taskId);
        pstmt.setTimestamp(5, new java.sql.Timestamp(curRunDate));
        pstmt.setInt(6, taskType);
        int r = pstmt.executeUpdate();
        pstmt.close();
        return r;
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    /*
     * private static final String SEL_OBS_TASK_SQL =
     * "SELECT task_id, cycle_unit FROM lb_task WHERE STATUS = '" +
     * Status.NORMAL + "' AND (CASE cycle_unit WHEN '" + Task.ONEOFF_CYCLE +
     * "' THEN DATE_ADD(start_date, INTERVAL ? DAY) ELSE end_date END) < NOW()";
     * public HashMap<String, String> getObsoleteTasks(int oneoffReservedDays)
     * throws SQLException { HashMap<String, String> taskIds = new
     * HashMap<String, String>(); PreparedStatement pstmt =
     * this.getConnection().prepareStatement(SEL_OBS_TASK_SQL); pstmt.setInt(1,
     * oneoffReservedDays); ResultSet rs = pstmt.executeQuery(); while
     * (rs.next()) { taskIds.put(rs.getString(1), rs.getString(2)); }
     * rs.close(); pstmt.close(); return taskIds; }
     */

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String UPD_TASK_BT_SQL = "UPDATE lb_task_run SET state = "
            + LState.FAILED_VALUE + " WHERE state IN (" + LState.RUNNING_VALUE + ","
            + LState.KILLING_VALUE + ") AND task_type=? AND runtime_broker = ?";


    public int updateTasksRunByBrokerAndType(String brokerIP, int type) throws SQLException {
        PreparedStatement pstmt = this.getConnection().prepareStatement(UPD_TASK_BT_SQL);
        pstmt.setInt(1, type);
        pstmt.setString(2, brokerIP);
        int i = pstmt.executeUpdate();
        pstmt.close();
        return i;
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String UPD_TASK_PRIOR_SQL = "UPDATE lb_task set task_priority=? where task_id=?";


    public void updateTasksPriority(ArrayList<String> taskIds, int priority) throws SQLException {
        PreparedStatement pstmt = this.getConnection().prepareStatement(UPD_TASK_PRIOR_SQL);
        this.getConnection().setAutoCommit(false);
        Iterator<String> iter = taskIds.iterator();
        while (iter.hasNext()) {
            String id = iter.next();
            pstmt.setString(1, id);
            pstmt.setInt(2, priority);
            pstmt.addBatch();
        }
        pstmt.executeBatch();
        this.getConnection().commit();
        pstmt.close();
        this.getConnection().setAutoCommit(true);
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String SEL_CMD_SQL = "SELECT command_id, command_name, setter, task_id, date_from, "
            + "date_to, set_time, state, param1,start_time,end_time FROM lb_command";


    public LinkedList<Command> getCommands() throws SQLException {
        LinkedList<Command> cmds = new LinkedList<Command>();
        Statement stmt = this.getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(SEL_CMD_SQL);
        while (rs.next()) {
            int id = rs.getInt("command_id");
            Command cmd = new Command(id);
            cmd.setName(rs.getString("command_name"));
            cmd.setSetter(rs.getString("setter"));
            cmd.setTaskId(rs.getString("task_id"));
            cmd.setDateFrom(rs.getTimestamp("date_from"));
            cmd.setDateTo(rs.getTimestamp("date_to"));
            cmd.setParam1(rs.getString("param1"));
            cmd.setState(rs.getInt("state"));
            cmd.setSetTime(rs.getTimestamp("set_time"));
            cmd.setStartTime(rs.getTimestamp("start_time"));
            cmd.setEndTime(rs.getTimestamp("end_time"));
            cmds.add(cmd);
        }
        rs.close();
        stmt.close();
        return cmds;
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String UPD_CMD_SQL = "UPDATE lb_command SET state= ?, start_time= ?, end_time=?,"
            + "exec_desc=?, last_update = now() WHERE command_id=?";


    public void updateCommand(Command ce) throws SQLException {
        PreparedStatement pstmt = this.getConnection().prepareStatement(UPD_CMD_SQL);
        pstmt.setInt(1, ce.getState());
        pstmt.setTimestamp(2, ce.getStartTime());
        pstmt.setTimestamp(3, ce.getEndTime());
        pstmt.setString(4, ce.getComment());
        pstmt.setInt(5, ce.getId());
        pstmt.setInt(5, ce.getId());
        pstmt.executeUpdate();
        pstmt.close();
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String DEL_CMD_SQL = "DELETE FROM lb_command WHERE command_id=?";


    public void deleteCommand(int cmdId) throws SQLException {
        PreparedStatement pstmt = this.getConnection().prepareStatement(DEL_CMD_SQL);
        pstmt.setInt(1, cmdId);
        pstmt.executeUpdate();
        pstmt.close();
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String SEL_TASK_TO_SQL = "select t1.task_to, t2.task_type from lb_task_link t1,"
            + "lb_task t2 where t1.task_from=? and t1.task_to=t2.task_id";


    public void getTaskOffsprings(HashMap<String, Integer> tasks, String taskId)
            throws SQLException {
        PreparedStatement pstmt = this.getConnection().prepareStatement(SEL_TASK_TO_SQL);
        pstmt.setString(1, taskId);
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()) {
            String id = rs.getString(1);
            int taskType = rs.getInt(2);
            tasks.put(id, taskType);
            // recursively find
            getTaskOffsprings(tasks, id);
        }
        rs.close();
        pstmt.close();
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String SEL_TASK_FROM_SQL = "select task_from from lb_task_link where task_to=?";


    public void getTaskAncestors(ArrayList<String> taskIds, String taskId) throws SQLException {
        PreparedStatement pstmt = this.getConnection().prepareStatement(SEL_TASK_FROM_SQL);
        pstmt.setString(1, taskId);
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()) {
            String id = rs.getString(1);
            taskIds.add(id);
            // recursively find
            getTaskAncestors(taskIds, id);
        }
        rs.close();
        pstmt.close();
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String DEL_TASK_RUNS_SQL = "DELETE FROM lb_task_run WHERE task_type=? AND task_id="
            + "? AND cur_run_date =?";


    public void deleteTaskRuns(ArrayList<TaskRun> taskRuns) throws SQLException {
        PreparedStatement pstmt = this.getConnection().prepareStatement(DEL_TASK_RUNS_SQL);
        this.getConnection().setAutoCommit(false);
        for (TaskRun tr : taskRuns) {
            pstmt.setInt(1, tr.getType());
            pstmt.setString(2, tr.getId());
            pstmt.setTimestamp(3, tr.getCurRunDate());
            pstmt.addBatch();
        }
        pstmt.executeBatch();
        pstmt.close();
        this.getConnection().commit();
        this.getConnection().setAutoCommit(true);
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String DEL_TASK_LINK_SQL = "DELETE FROM lb_task_link WHERE task_from=? OR task_to=?";
    private static final String DEL_TASK_VIEW_SQL = "DELETE FROM lb_view_task WHERE task_id=?";
    private static final String DEL_TASK_EXT_SQL  = "DELETE FROM lb_task_ext WHERE task_id=?";
    private static final String DEL_TASK_RUN_SQL  = "DELETE FROM lb_task_run WHERE task_id=?";
    private static final String DEL_TASK_SQL      = "DELETE from lb_task WHERE task_id=?";


    public void deleteTasksCascade(String taskId) throws SQLException {
        this.getConnection().setAutoCommit(false);
        PreparedStatement pstmt1 = this.getConnection().prepareStatement(DEL_TASK_LINK_SQL);
        pstmt1.setString(1, taskId);
        pstmt1.setString(2, taskId);
        PreparedStatement pstmt2 = this.getConnection().prepareStatement(DEL_TASK_VIEW_SQL);
        pstmt2.setString(1, taskId);
        PreparedStatement pstmt3 = this.getConnection().prepareStatement(DEL_TASK_EXT_SQL);
        pstmt3.setString(1, taskId);
        PreparedStatement pstmt4 = this.getConnection().prepareStatement(DEL_TASK_RUN_SQL);
        pstmt4.setString(1, taskId);
        PreparedStatement pstmt5 = this.getConnection().prepareStatement(DEL_TASK_SQL);
        pstmt5.setString(1, taskId);

        pstmt1.executeUpdate();
        pstmt2.executeUpdate();
        pstmt3.executeUpdate();
        pstmt4.executeUpdate();
        pstmt5.executeUpdate();
        pstmt1.close();
        pstmt2.close();
        pstmt3.close();
        pstmt4.close();
        pstmt5.close();
        this.getConnection().commit();
        this.getConnection().setAutoCommit(true);
    }


    // //////////////////////////////////////////////////////////////////////////////////////////

    public int updateTaskRun(int state, int redo, int tries, boolean bUpdateEndTime, String lastLog,
                             String taskId, java.sql.Timestamp dateFrom, java.sql.Timestamp dateTo,
                             boolean checkRuntimeBroker) throws SQLException {
        StringBuilder sbUpd = new StringBuilder("UPDATE lb_task_run SET last_update=now(),state=");
        sbUpd.append(state);
        StringBuilder sbWhere = new StringBuilder(
                " WHERE task_id = ? AND cur_run_date BETWEEN ? AND ? AND state<> ?");
        if (redo != -1) {
            sbUpd.append(",redo_flag=");
            sbUpd.append(redo);
        }

        if (tries != -1) {
            sbUpd.append(",tries=");
            sbUpd.append(tries);
        }

        if (bUpdateEndTime) {
            sbUpd.append(",end_time=now()");
        }

        if (lastLog != null) {
            sbUpd.append(",last_log='");
            sbUpd.append(lastLog);
            sbUpd.append("'");
        }

        if (checkRuntimeBroker) {
            sbWhere.append(" AND runtime_broker IS NOT NULL");
        }

        sbUpd.append(sbWhere);

        PreparedStatement pstmt = this.getConnection().prepareStatement(sbUpd.toString());
        pstmt.setString(1, taskId);
        pstmt.setTimestamp(2, dateFrom);
        pstmt.setTimestamp(3, dateTo);
        pstmt.setInt(4, state);
        int r = pstmt.executeUpdate();
        pstmt.close();
        return r;
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String UPD_TASK_STATUS_SQL = "UPDATE lb_task SET last_update = now(), status = "
            + "? WHERE task_id = ? and status = ?";


    public int updateTaskStatus(String taskId, String oldStauts, String newStatus)
            throws SQLException {
        PreparedStatement pstmt = this.getConnection().prepareStatement(UPD_TASK_STATUS_SQL);
        pstmt.setString(1, newStatus);
        pstmt.setString(2, taskId);
        pstmt.setString(3, oldStauts);
        int r = pstmt.executeUpdate();
        pstmt.close();
        return r;
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String ADD_TASK_RUN_SQL = "INSERT INTO lb_task_run (task_id, task_type, cur_run_date,"
            + "next_run_date, state, try_limit, run_priority, redo_flag, tries, last_update, create_time) VALUES ("
            + "?, ?, ?, ?, ?, ?, ?, 0, 0, now(), now())";


    public int insertTaskRuns(ArrayList<TaskRun> taskRuns) throws SQLException {
        this.getConnection().setAutoCommit(false);
        PreparedStatement pstmt = this.getConnection().prepareStatement(ADD_TASK_RUN_SQL);
        for (TaskRun tr : taskRuns) {
            pstmt.setString(1, tr.getId());
            pstmt.setInt(2, tr.getType());
            pstmt.setTimestamp(3, tr.getCurRunDate());
            pstmt.setTimestamp(4, tr.getNextRunDate());
            pstmt.setInt(5, tr.getState());
            pstmt.setInt(6, tr.getTryLimit());
            pstmt.setInt(7, tr.getRunPriority());
            pstmt.addBatch();
        }
        int[] r = pstmt.executeBatch();
        this.getConnection().commit();
        pstmt.close();
        this.getConnection().setAutoCommit(true);
        return r.length;
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String SEL_TASK_LINK_SQL = "SELECT task_from,task_to,status,"
            + "dependence_type FROM lb_task_link WHERE task_to = ?";


    public ArrayList<TaskLink> getTaskLinks(String taskId) throws SQLException {
        ArrayList<TaskLink> tls = new ArrayList<TaskLink>();
        PreparedStatement pstmt = this.getConnection().prepareStatement(SEL_TASK_LINK_SQL);
        pstmt.setString(1, taskId);
        ResultSet rs = pstmt.executeQuery();
        TaskLink tl = null;
        while (rs.next()) {
            tl = new TaskLink();
            tl.setTaskFrom(rs.getString("task_from"));
            tl.setTaskTo(rs.getString("task_to"));
            tl.setDependenceType(rs.getInt("dependence_type"));
            tl.setStatus(rs.getString("status"));
            tls.add(tl);
        }
        rs.close();
        pstmt.close();
        return tls;
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String SEL_SUCC_BETW_SQL = "SELECT cur_run_date, next_run_date, "
            + "state FROM lb_task_run WHERE task_id=? AND task_type=?";


    public ArrayList<TaskRunSimple> getTaskRuns(String taskId, int taskType) throws SQLException {
        ArrayList<TaskRunSimple> trs = new ArrayList<TaskRunSimple>();
        PreparedStatement pstmt = this.getConnection().prepareStatement(SEL_SUCC_BETW_SQL);
        pstmt.setString(1, taskId);
        pstmt.setInt(2, taskType);
        ResultSet rs = pstmt.executeQuery();
        TaskRunSimple tr = null;
        while (rs.next()) {
            tr = new TaskRunSimple();
            tr.setType(taskType);
            tr.setId(taskId);
            tr.setCurRunDate(rs.getTimestamp("cur_run_date"));
            tr.setNextRunDate(rs.getTimestamp("next_run_date"));
            tr.setState(rs.getInt("state"));
            trs.add(tr);
        }
        rs.close();
        pstmt.close();
        return trs;
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String SEL_RUNS_BY_TYPE_SQL = "SELECT task_id,task_type,cur_run_date,next_run_date,"
            + "tries,state,start_time,end_time,last_update FROM lb_task_run WHERE task_type=?";


    public LinkedList<TaskRun> getTaskRuns(int taskType) throws SQLException {
        LinkedList<TaskRun> runs = new LinkedList<TaskRun>();
        PreparedStatement pstmt = this.getConnection().prepareStatement(SEL_RUNS_BY_TYPE_SQL);
        pstmt.setInt(1, taskType);
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()) {
            TaskRun tr = new TaskRun();
            tr.setId(rs.getString("task_id"));
            tr.setType(rs.getInt("task_type"));
            tr.setState(rs.getInt("state"));
            tr.setTries(rs.getInt("tries"));
            tr.setCurRunDate(rs.getTimestamp("cur_run_date"));
            tr.setNextRunDate(rs.getTimestamp("next_run_date"));
            tr.setStartTime(rs.getTimestamp("start_time"));
            tr.setEndTime(rs.getTimestamp("end_time"));
            tr.setLastUpdate(rs.getTimestamp("last_update"));
            runs.add(tr);
        }
        rs.close();
        pstmt.close();
        return runs;
    }

    private final static String SEL_RUN_PART_SQL = "SELECT partition_description FROM information_schema."
            + "partitions WHERE table_schema=? AND table_name='lb_task_run'";


    public ArrayList<String> getRunPartitions() throws SQLException {
        PreparedStatement pstmt = this.getConnection().prepareStatement(SEL_RUN_PART_SQL);
        pstmt.setString(1, this.getSchema());
        ArrayList<String> values = new ArrayList<String>();
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()) {
            String pd = rs.getString("partition_description");
            if (pd != null)
                values.add(pd);
        }
        rs.close();
        pstmt.close();
        return values;
    }

    private final static String ADD_RUN_PART_SQL = "ALTER TABLE lb_task_run ADD PARTITION ("
            + "PARTITION p_? VALUES IN (?))";


    public void addRunPartition(int value) throws SQLException {
        PreparedStatement pstmt = this.getConnection().prepareStatement(ADD_RUN_PART_SQL);
        pstmt.setInt(1, value);
        pstmt.setInt(2, value);
        pstmt.executeUpdate();
        pstmt.close();
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String UPD_RUNNER_SQL_1 = "update lb_runner set startup_time=now(),"
            + "last_heartbeat=now() WHERE status='" + Status.NORMAL
            + "' AND type_id=? AND broker_ip=?";
    private static final String UPD_RUNNER_SQL_2 = "update lb_runner set last_heartbeat="
            + "now() WHERE type_id=? AND broker_ip=?";


    public int updateRunner(Runner runner, boolean startUp) throws SQLException {
        String sql = null;
        if (startUp) {
            sql = UPD_RUNNER_SQL_1;
        } else {
            sql = UPD_RUNNER_SQL_2;
        }
        PreparedStatement pstmt = this.getConnection().prepareStatement(sql);
        pstmt.setInt(1, runner.getTaskType());
        pstmt.setString(2, runner.getBrokerIP());
        int r = pstmt.executeUpdate();
        return r;
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String INS_TASK_LOG = "INSERT INTO lb_task_log (task_id, cur_run_date, "
            + "next_run_date, runtime_id, state, log_type, log_desc, runtime_broker, log_time, log_time_ms,tries"
            + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";


    public void insertTaskLog(ConcurrentLinkedQueue<TaskLog> tls) throws SQLException {
        this.getConnection().setAutoCommit(false);
        PreparedStatement pstmt = this.getConnection().prepareStatement(INS_TASK_LOG);
        TaskLog tl = null;
        Iterator<TaskLog> iter = tls.iterator();
        while (iter.hasNext()) {
            tl = iter.next();
            pstmt.setString(1, tl.getTaskId());
            pstmt.setTimestamp(2, tl.getCurRunDate());
            pstmt.setTimestamp(3, tl.getNextRunDate());
            pstmt.setString(4, tl.getRunTimeId());
            pstmt.setInt(5, tl.getState());
            pstmt.setString(6, tl.getType());
            String desc = tl.getDescription();
            if (desc.length() >= 3900) {
                desc = desc.substring(0, 3900);
            }
            pstmt.setString(7, desc);
            // cpwang,20120601
            pstmt.setString(8, tl.getRuntimeBroker());
            java.sql.Timestamp ts = new java.sql.Timestamp(tl.getLogTime());
            pstmt.setTimestamp(9, ts);
            // cpwang,20120529,insert milliseconds into mysql
            pstmt.setInt(10, ts.getNanos() / 1000000);
            pstmt.setInt(11, tl.getTries());
            pstmt.addBatch();
        }
        pstmt.executeBatch();
        pstmt.close();
        this.getConnection().commit();
        this.getConnection().setAutoCommit(true);
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String INS_SYS_LOG = "INSERT INTO lb_sys_log (log_source, log_desc, log_tigger, "
            + "log_type, broker_ip, time_elapsed, log_time, log_time_ms, task_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";


    public void insertSysLog(ConcurrentLinkedQueue<SysLog> sls) throws SQLException {
        this.getConnection().setAutoCommit(false);
        PreparedStatement pstmt = this.getConnection().prepareStatement(INS_SYS_LOG);
        SysLog sl = null;
        Iterator<SysLog> iter = sls.iterator();
        while (iter.hasNext()) {
            sl = iter.next();
            pstmt.setString(1, sl.getSource());
            pstmt.setString(2, sl.getDescription());
            pstmt.setString(3, sl.getTrigger());
            pstmt.setString(4, sl.getType());
            pstmt.setString(5, sl.getBrokerIP());
            pstmt.setLong(6, sl.getTimeElapsed());
            java.sql.Timestamp ts = new java.sql.Timestamp(sl.getLogTime());
            pstmt.setTimestamp(7, ts);
            // cpwang,20120529,insert milliseconds into mysql
            pstmt.setInt(8, ts.getNanos() / 1000000);
            pstmt.setInt(9, sl.getTaskType());
            pstmt.addBatch();
        }
        pstmt.executeBatch();
        pstmt.close();
        this.getConnection().commit();
        this.getConnection().setAutoCommit(true);
    }


    public void splitMaxPartition(String tabName, String partDate, String partName)
            throws SQLException {
        String sql = "alter table " + this.getSchema() + "." + tabName.toLowerCase()
                + " reorganize partition p_max into (partition p_" + partName
                + " values less than (to_days('" + partDate
                + "')),partition p_max values less than (maxvalue))";
        Statement stmt = this.getConnection().createStatement();
        stmt.executeUpdate(sql);
        stmt.close();
    }


    public void dropLogPartition(String tabName, String partName) throws SQLException {
        String sql = "alter table " + this.getSchema() + "." + tabName.toLowerCase()
                + " drop partition " + partName;
        Statement stmt = this.getConnection().createStatement();
        stmt.executeUpdate(sql);
        stmt.close();
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String SEL_LOG_PARTS_SQL = "select partition_name, from_days(partition_description"
            + ") as part_date from INFORMATION_SCHEMA.PARTITIONS where table_schema = ? and table_name = "
            + "? and partition_description <>'MAXVALUE'";


    public HashMap<String, java.util.Date> getLogPartitions(String tabName) throws SQLException {
        PreparedStatement pstmt = this.getConnection().prepareStatement(SEL_LOG_PARTS_SQL);
        pstmt.setString(1, this.getSchema());
        pstmt.setString(2, tabName.toLowerCase());
        ResultSet rs = pstmt.executeQuery();
        HashMap<String, java.util.Date> parts = new HashMap<String, java.util.Date>();
        while (rs.next()) {
            String pn = rs.getString("partition_name");
            java.util.Date dt = rs.getDate("part_date");
            parts.put(pn, dt);
        }
        rs.close();
        pstmt.close();
        return parts;
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String TRUNC_STAT_SQL = "truncate table lb_run_stat";


    public void truncateRunStatistics() throws SQLException {
        Statement stmt = this.getConnection().createStatement();
        stmt.executeUpdate(TRUNC_STAT_SQL);
        stmt.close();
    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    private static final String INS_STAT_SQL = "INSERT INTO lb_run_stat(task_id, task_type, cur_run_date, "
            + "cycle_unit, order_id, in_charge, task_name, duration_range_id, duration_range, flutter_range_id,"
            + "flutter_range, duration, flutter, last_update) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,now());";


    public void insertRunStatistics(TreeSet<TaskRun> trs, int limit) throws SQLException {
        PreparedStatement pstmt = this.getConnection().prepareStatement(INS_STAT_SQL);
        this.getConnection().setAutoCommit(false);
        int orderId = 1;
        Iterator<TaskRun> iter = trs.descendingIterator();
        while (iter.hasNext()) {
            TaskRun tr = iter.next();
            pstmt.setString(1, tr.getId());
            pstmt.setInt(2, tr.getType());
            pstmt.setTimestamp(3, tr.getCurRunDate());
            pstmt.setString(4, tr.getCycleUnit());
            pstmt.setInt(5, orderId);
            pstmt.setString(6, tr.getInCharge());
            pstmt.setString(7, tr.getName());
            StatRange dr = StatRange.getDurationRange(tr.getDuration());
            pstmt.setInt(8, dr.getId());
            pstmt.setString(9, dr.getDescription());

            StatRange fr = StatRange.getFlutterRange(tr.getFlutter() * 100);
            pstmt.setInt(10, fr.getId());
            pstmt.setString(11, fr.getDescription());

            pstmt.setFloat(12, tr.getDuration());
            pstmt.setFloat(13, tr.getFlutter() * 100);
            pstmt.addBatch();

            if (orderId++ >= limit) {
                break;
            }
        }
        pstmt.executeBatch();
        pstmt.close();
        this.getConnection().commit();
        this.getConnection().setAutoCommit(true);
    }

}
