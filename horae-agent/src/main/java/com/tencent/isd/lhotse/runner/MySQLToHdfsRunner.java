package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.util.CommonUtils;
import com.tencent.isd.lhotse.runner.util.DBUtil;
import com.tencent.isd.lhotse.runner.util.RunTaskProcess;
import com.tencent.isd.lhotse.runner.util.RunnerUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import static com.tencent.isd.lhotse.runner.util.CommonUtils.mergekeyValue;

/**
 * Created by luffy on 2016/3/21.
 */
public class MySQLToHdfsRunner extends AbstractTaskRunner {

    public static void main(String[] args) {
        TaskRunnerLoader.startRunner(MySQLToHdfsRunner.class, (byte) 3);
    }

    @Override
    public void execute() throws IOException {
        Map<String, String> keyValues = new HashMap<String, String>();
        LTask task = getTask();
//        this.commitTask(LhotseObject.LState.READY, "MySQL2HDFS runner", "MySQL2HDFS runner ready!.");

        /* Get the target mysql cluster configuration. */
        LhotseObject.LServer dbServer = task.getSourceServers(0);
        String mysqlUser = dbServer.getUserName();
        String mysqlPasswd = dbServer.getPassword();
        String mysqlHost = dbServer.getHost();
        int mysqlPort = dbServer.getPort();
        String mysqlService = dbServer.getService();

        String connectionString = null;
        try {
            connectionString = DBUtil.getConnectionString("mysql", mysqlHost, mysqlPort,
                    mysqlService);
            this.writeLocalLog(Level.INFO, "connectionString=" + connectionString);
        }
        catch (SQLException e) {
            keyValues.put("exit_code", RunnerUtils.SERVER_CONFIG_ERROR_CODE);
            keyValues.put("task_desc",
                    "Task failed becuase server config(Connection String) is not correct: "
                            + CommonUtils.stackTraceToString(e));
//            commitJsonResult(keyValues, false, "");
            return;
        }

        String selectSql = "";
        String hiveTableName = "";
        String hiveDB = "";
        try {
            selectSql = CommonUtils.standardizeDateString(this.getExtPropValue("select.sql"),
                    task.getCurRunDate());
            hiveTableName = CommonUtils.standardizeDateString(this.getExtPropValue("hive.table.name"),
                    task.getCurRunDate());
            hiveDB = CommonUtils.standardizeDateString(this.getExtPropValue("hive.database"),
                    task.getCurRunDate());
        } catch (Exception e) {
            e.printStackTrace();
        }


        /* hdfs parms */
        LhotseObject.LServer hdfsServer = task.getTargetServers(0);
        String hdfsHost = hdfsServer.getHost();
        int hdfsPort = hdfsServer.getPort();
        if (!hdfsHost.toLowerCase().startsWith("hdfs://")) {
            hdfsHost = "hdfs://" + hdfsHost;
        }
        String hdfsUser = hdfsServer.getUserName();
        String hdfsGroup = hdfsServer.getUserGroup();
        String defaultFS = hdfsHost;
//        String hadoopJobUgi = hdfsUser + "," + hdfsGroup;

        String hdfsPath = "/user/hive/warehouse/" + hiveDB + ".db/" + hiveTableName + "/";

        String targetCharset = "UTF-8";
        String separator = "\\0001";

        /* Set the parameters to a fixed value. */
        String readConcurrency = this.getExtPropValueWithDefault("readConcurrency", "2");
        String writeConcurrency = this.getExtPropValueWithDefault("writeConcurrency", "4");
        String errorThreshold = this.getExtPropValueWithDefault("errorThreshold", "10000");
        String ignoreEmptyDatasource = this.getExtPropValueWithDefault("ignore.empty.datasource", "false");

        StringBuffer sb = new StringBuffer();
        mergekeyValue(sb, "readConcurrency", readConcurrency);
        mergekeyValue(sb, "writeConcurrency", writeConcurrency);
        mergekeyValue(sb, "errorThreshold", errorThreshold);
        mergekeyValue(sb, "select.sql", selectSql);
        mergekeyValue(sb, "source.db.connection.string", connectionString);
        mergekeyValue(sb, "source.db.user", mysqlUser);
        mergekeyValue(sb, "source.db.password", mysqlPasswd);
        mergekeyValue(sb, "hdfs.path", hdfsPath);
        mergekeyValue(sb, "fs.defaultFS", defaultFS);
        mergekeyValue(sb, "HADOOP_USER_NAME", hdfsUser);
        mergekeyValue(sb, "charset.encoding", targetCharset);
        mergekeyValue(sb, "separator", separator);
        mergekeyValue(sb, "ignore.empty.datasource", ignoreEmptyDatasource);


        /** anyloader path */
        String DX_TEMP_PATH = "/anyloader/parms/";
        String DX_HOME = "/anyloader/";
        String DX_TEMPLATE_PATH = "/anyloader/nest/MySQL2Hdfs.xml";

        /**
         * create dir
         */

        File f = new File(DX_TEMP_PATH + task.getId() + '/');
        if (!f.exists())
            f.mkdirs();

        String dxCtrlFileName = task.getId() + "_"
                + new SimpleDateFormat("yyyyMMddHHmmss").format(task.getCurRunDate())
                + ".dxctl";
        String dxCtrlFileFullPath = f.getAbsolutePath() + "/" + dxCtrlFileName;
        dxCtrlFileFullPath = new File(dxCtrlFileFullPath).getAbsolutePath();
        CommonUtils.string2File(sb.toString(), dxCtrlFileFullPath, "UTF-8");

        this.commitTask(LhotseObject.LState.RUNNING, "MySQL2HDFS runner", "MySQL2HDFS runner running.");
        /**
         * Exec command
         */
        String[] cmds = { DX_HOME + "/run.sh", DX_TEMPLATE_PATH, dxCtrlFileFullPath };
        this.writeLocalLog(Level.INFO, "DX Command:" + Arrays.deepToString(cmds));
        RunTaskProcess dxProcess = new RunTaskProcess(cmds, this);
        try {
            dxProcess.startProcess(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String dxPid = String.valueOf(dxProcess.getProcessId());
        this.writeLocalLog(Level.INFO, "DX process is running, pid=" + dxPid);
        try {
            dxProcess.waitAndDestroyProcess();
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.commitTask(LhotseObject.LState.SUCCESSFUL, "MySQL2HDFS runner", "MySQL2HDFS runner finished successfully.");

    }

    @Override
    public void kill() throws IOException {

    }

    private String getExtPropValueWithDefault(String key, String defaultValue) {
        String value = this.getExtPropValue(key);
        if (StringUtils.isBlank(value))
            value = defaultValue;
        return value;
    }
}
