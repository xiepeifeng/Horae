package com.tencent.isd.lhotse.runner;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.TaskRunnerLoader;
import com.tencent.isd.lhotse.runner.util.*;
import com.tencent.isd.lhotse.runner.util.RunnerUtils.LoadDataResult;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.logging.Level;

import static com.tencent.isd.lhotse.runner.util.CommonUtils.mergekeyValue;


public class TDWToPgRunner extends TDWDDCTaskRunner {

    protected static final long MAX_ROWS_LIMIT = Long.MAX_VALUE;


    public static void main(String[] args) {
        TaskRunnerLoader.startRunner(TDWToPgRunner.class, (byte) 96);
    }


    @Override
    public void execute() throws IOException {

        LTask task = getTask();
        Map<String, String> keyValues = new HashMap<String, String>();

        /* Check the server tags configuration for this task. */
        if (task.getSourceServersCount() < 3) {
            keyValues.put("exit_code", RunnerUtils.SERVER_CONFIG_ERROR_CODE);
            keyValues.put("task_desc", "Task server configuration is incorrect, expect at least 3 source "
                    + "HDFS servers configured, but " + task.getSourceServersCount() + " server configured");
            commitJsonResult(keyValues, false, "");
            return;
        }

        if (task.getTargetServersCount() != 1) {
            keyValues.put("exit_code", RunnerUtils.SERVER_CONFIG_ERROR_CODE);
            keyValues.put("task_desc", "Task server configuration is incorrect, expect 1 target "
                    + "servers configured, but " + task.getTargetServersCount() + " server configured");
            commitJsonResult(keyValues, false, "");
            return;
        }

        /* Calculate the real data date, for tasks transferred from USP. */
        runDate = getRealDataDate(task);

        /*
         * Check whether we need to skip the weekly task for those USP weekly
         * and monthly tasks.
         */
        if (skipWeeklyTaskExecution(task, runDate)) {
            this.writeLocalLog(Level.INFO, "skipWeeklyTaskExecution");
            return;
        }

        /* Get the hive server configuration. */
        LServer hiveServer = task.getSourceServers(0);

        /* Get the name node configuration for this TDW. */
        LServer nameNodeServer = task.getSourceServers(1);

        /* Get the target HDFS cluster configuration. */
        LServer postgreServer = task.getTargetServers(0);
        String postgreUser = postgreServer.getUserName();
        String postgrePasswd = postgreServer.getPassword();
        String postgreHost = postgreServer.getHost();
        int postgrePort = postgreServer.getPort();
        String postgreService = postgreServer.getService();

        String connectionString = null;
        try {
            connectionString = DBUtil.getConnectionString("postgre", postgreHost, postgrePort, postgreService);
            this.writeLocalLog(Level.INFO, "connectionString=" + connectionString);
        }
        catch (SQLException e) {
            keyValues.put("exit_code", RunnerUtils.SERVER_CONFIG_ERROR_CODE);
            keyValues.put("task_desc", "Task failed becuase server config(Connection String) is not correct: "
                    + CommonUtils.stackTraceToString(e));
            commitJsonResult(keyValues, false, "");
            return;
        }

        this.writeLocalLog(Level.INFO, "----- Step 1 complete -----");

        /* Get the task configuration. */
        String tdwDbName = this.getExtPropValue("tdwDbName");

        // TODO: oracleTableName --> dbTableName
        // TODO: oracleColumnNames --> dbColumnNames
        String dbTableName = this.getExtPropValue("dbTableName");
        String dbColumnNames = this.getExtPropValue("dbColumnNames");
        this.writeLocalLog(Level.INFO, "dbColumnNames=" + dbColumnNames);

        String loadMode = this.getExtPropValue("loadMode");
        String errorPercentage = this.getExtPropValue("errorPercentage");
        String specialParam = this.getExtPropValue("special_para");

        String filterSQL = null;
        String partitionValue = null;
        try {
            filterSQL = RunnerUtils.replaceDateExpr(this.getExtPropValue("filterSQL"), runDate);
            partitionValue = this.getExtPropValue("partitionValue");
            partitionValue = (partitionValue == null ? "" : RunnerUtils.replaceDateExpr(partitionValue, runDate));
        }
        catch (Exception e) {
            keyValues.put("exit_code", RunnerUtils.DATE_FORMAT_ERROR_CODE);
            keyValues.put("task_desc",
                "Task failed becuase date format is not correct: " + CommonUtils.stackTraceToString(e));
            commitJsonResult(keyValues, false, "");
            return;
        }

        this.writeLocalLog(Level.INFO, "----- Step 2 complete -----");

        int columnLimit = new StringTokenizer(dbColumnNames, ",").countTokens();
        this.writeLocalLog(Level.INFO, "columnLimit: " + columnLimit);
        String errorThreshold = this.getExtPropValueWithDefault("errorThreshold", "0");

        /* Calculate the date formatter for etl_stamp. */
        SimpleDateFormat dateFormat =
                getDateFormat(this.getExtPropValue("offRange"), this.getExtPropValue("offRangeType"), task
                    .getCycleUnit().trim());
        if (dateFormat == null) {
            String failMessage = "Can't find the appropriate date formatter";
            this.writeLocalLog(Level.SEVERE, failMessage);
            commitTask(LState.FAILED, "", failMessage);
            return;
        }

        /* Calculate the etl_time. */
        String etlTime = task.getId() + "_" + dateFormat.format(runDate);
        if (this.getExtPropValue("ddcId") != null) {
            etlTime = this.getExtPropValue("ddcId") + "_" + dateFormat.format(runDate);
        }

        /* Composite the truncate SQL. */
        String truncateSQL = null;
        if (APPEND_FLAG.equalsIgnoreCase(loadMode)) {
            truncateSQL = "DELETE FROM " + dbTableName + " WHERE ETL_STAMP='" + etlTime + "'";
        }
        else if (TRUNCATE_FLAG.equalsIgnoreCase(loadMode)) {
            if ((partitionValue == null) || (partitionValue.length() < 2)) {
                truncateSQL = "TRUNCATE TABLE " + dbTableName;
            }
            else {
                truncateSQL = "TRUNCATE TABLE " + dbTableName + "_" + partitionValue;
            }
        }

        this.writeLocalLog(Level.INFO, "truncateSQL: " + truncateSQL);

        /* Generate the SQL based on the db column names. */
        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        StringTokenizer st = new StringTokenizer(dbColumnNames, ",");
        while (st.hasMoreTokens()) {
            String tokenValue = st.nextToken().trim();

            StringTokenizer newSt = new StringTokenizer(tokenValue, " ");
            int countTokens = newSt.countTokens();
            if (countTokens == 1) {
                sb1.append(tokenValue).append(",");
                sb2.append("?,");
            }
            else {
                String columnName = newSt.nextToken();

                String secondToken = newSt.nextToken();
                if (secondToken.startsWith("char")) {
                    /* Do nothing for char. */
                }
                else if (secondToken.startsWith("filler")) {
                    /* Do nothing for escape. */
                }
                else {
                    String dbDateFormat = "";
                    if ("date".equalsIgnoreCase(secondToken) || "timestamp".equalsIgnoreCase(secondToken)) {
                        while (newSt.hasMoreTokens()) {
                            String newTokenValue = newSt.nextToken();
                            newTokenValue = newTokenValue.replace("\"", "");
                            newTokenValue = newTokenValue.replace("'", "");
                            dbDateFormat = dbDateFormat + newTokenValue + " ";
                        }
                        sb1.append(columnName).append(",");
                        sb2.append("TO_TIMESTAMP(?, '" + dbDateFormat.trim().toUpperCase() + "'),");
                    }
                }
            }
        }

        String columnNames = sb1.toString().substring(0, sb1.toString().length() - 1);
        String columnValues = sb2.toString().substring(0, sb2.toString().length() - 1);
        if (APPEND_FLAG.equalsIgnoreCase(loadMode)) {
            columnNames = columnNames + ",etl_stamp";
            columnValues = columnValues + ",'" + etlTime + "'";
        }

        // insert sql--------------
        String postgreSQLTmp = "";
        if ((partitionValue == null) || (partitionValue.length() < 2)) {
            postgreSQLTmp = "INSERT INTO " + dbTableName + "(" + columnNames + ") VALUES (" + columnValues + ")";
        }
        else {
            postgreSQLTmp =
                    "INSERT INTO " + dbTableName + "_" + partitionValue + "(" + columnNames + ") VALUES ("
                            + columnValues + ")";
        }
        // -------------------

        String postgreSQL = this.getExtPropValueWithDefault("dbSQL", postgreSQLTmp);
        this.writeLocalLog(Level.INFO, "postgreSQL: " + postgreSQL);

        /* Set the parameters to a fixed value. */
        String readConcurrency = this.getExtPropValueWithDefault("readConcurrency", "1");
        String writeConcurrency = this.getExtPropValueWithDefault("writeConcurrency", "4");
        String disableRecursive = "true";
        String charsetEncoding = "UTF-8";
        boolean ignoreEmptyDatasource = new Boolean(this.getExtPropValue("ignoreEmptyDataSource"));
        String ignoredColumns = this.getExtPropValueWithDefault("ignored.columns", "");
        String validatorRegex = "";
        String validatePartitionKey = "false";

        /* Get the global parameter. */
        final String plcProgram = this.requestGlobalParameters().get(PLC_PROGRAM);
        final String hadoopCommand = getHadoopCommandOnVersion(nameNodeServer);
        final String tmpPathPrefix = this.requestGlobalParameters().get(NAME_NODE_TMP_PATH);
        final String externalTableDb = this.requestGlobalParameters().get(EXTERNAL_TABLE_CREATE_DB);
        plcParameter = getPLCParameter(task.getId(), runDate);
        this.writeLocalLog(Level.INFO, "plcparameter:" + plcParameter);
        final String DX_TEMP_PATH = this.requestGlobalParameters().get("dx_temp_path");
        final String DX_HOME = this.requestGlobalParameters().get("dx_home");
        final String DX_TEMPLATE_PATH = this.requestGlobalParameters().get("dx_template_hdfs_to_db");
        if (CommonUtils.isBlankAny(DX_TEMP_PATH, DX_HOME, DX_TEMPLATE_PATH)) {
            throw new IOException(String.format("Empty value found: dx_temp_path=%s, "
                    + "dx_home=%s,dx_template_path=%s", DX_TEMP_PATH, DX_HOME, DX_TEMPLATE_PATH));
        }

        /* External table and its delimiter. */
        String externalTableName =
                "ext_" + task.getId() + "_" + SECOND_FORMAT.format(runDate) + "_" + System.currentTimeMillis();
        String externalTableDataPath =
                tmpPathPrefix + File.separator + task.getId() + File.separator + SECOND_FORMAT.format(runDate)
                        + File.separator + System.currentTimeMillis();
        String destFileDelimiter = "\\001";

        boolean isSuccess = false;
        boolean isCommitted = false;
        try {
            /* Check to see whether the database exists or not. */
            if (!checkDbExistence(plcProgram, hiveServer, tdwDbName)) {
                isCommitted = true;
                return;
            }

            // /* Create the external table data path. */
            // HadoopResult createResult =
            // RunnerUtils.createHDFSPath(hadoopCommand, nameNodeServer,
            // externalTableDataPath, this);
            // if (createResult.getExitVal() != 0) {
            // keyValues.put("exit_code", createResult.getHadoopReturnCode());
            // keyValues.put("task_desc",
            // "Task failed because creating temporary path for "
            // + "external table fails with error code: " +
            // createResult.getHadoopReturnCode());
            // keyValues.put("run_date", HOUR_FORMAT.format(runDate));
            // commitJsonResult(keyValues, false, "");
            // committed = true;
            // return;
            // }

            /* Create the external table data path. */
            if (!createTmpPath(hadoopCommand, nameNodeServer, externalTableDataPath)) {
                isCommitted = true;
                return;
            }

            /* Create external table using the filter SQL. */
            if (!createExternalTableWithSQL(plcProgram, hiveServer, nameNodeServer, externalTableDataPath, tdwDbName,
                externalTableDb, externalTableName, filterSQL, destFileDelimiter)) {
                isCommitted = true;
                return;
            }

            /*
             * Load data to the external table, whose data directory on the name
             * node.
             */
            LoadDataResult loadDataResult =
                    loadDataWithSQL(plcProgram, hiveServer, tdwDbName, externalTableDb, externalTableName, filterSQL,
                        specialParam);
            if (loadDataResult.getExitVal() != 0) {
                isCommitted = true;
                return;
            }

            /* Check the max row limit */
            long rows = loadDataResult.getSuccessWrite();
            if (rows >= MAX_ROWS_LIMIT) {
                keyValues.put("exit_code", RunnerUtils.MAX_ROWS_LIMIT_REACHED);
                keyValues.put("task_desc", "Task reached the max rows limit: " + rows + "/" + MAX_ROWS_LIMIT);
                commitJsonResult(keyValues, false, "");
                isCommitted = true;
                return;
            }

            /* Now we need to export the data to db. */
            String nameNodeIP = nameNodeServer.getHost();
            if (!nameNodeIP.toLowerCase().startsWith("hdfs://")) {
                nameNodeIP = "hdfs://" + nameNodeIP;
            }

            String fsDefaultName = nameNodeIP + ":" + nameNodeServer.getPort();
            String hadoopJobUgi = nameNodeServer.getUserName() + "," + nameNodeServer.getUserGroup();
            String user = postgreUser;
            String password = DxDESCipher.EncryptDES(postgrePasswd);

            StringBuffer sb = new StringBuffer();
            mergekeyValue(sb, "readConcurrency", readConcurrency);
            mergekeyValue(sb, "writeConcurrency", writeConcurrency);
            mergekeyValue(sb, "errorThreshold", errorThreshold);
            mergekeyValue(sb, "truncate.sql", truncateSQL);
            mergekeyValue(sb, "target.tablename", dbTableName);
            mergekeyValue(sb, "source.path", externalTableDataPath);
            mergekeyValue(sb, "disable.recursive", disableRecursive);
            mergekeyValue(sb, "validator.regex", validatorRegex);
            mergekeyValue(sb, "fs.default.name", fsDefaultName);
            mergekeyValue(sb, "hadoop.job.ugi", hadoopJobUgi);
            mergekeyValue(sb, "charset.encoding", charsetEncoding);
            mergekeyValue(sb, "separator", destFileDelimiter);
            mergekeyValue(sb, "target.path", postgreSQL);
            mergekeyValue(sb, "connection.string", connectionString);
            mergekeyValue(sb, "user", user);
            mergekeyValue(sb, "password", password);
            mergekeyValue(sb, "column.limit", columnLimit);
            mergekeyValue(sb, "ignored.columns", ignoredColumns);
            mergekeyValue(sb, "ignore.empty.datasource", ignoreEmptyDatasource);
            mergekeyValue(sb, "validate.partition.key", validatePartitionKey);
            mergekeyValue(sb, "partition.value", partitionValue);
            mergekeyValue(sb, "driver.string", "org.postgresql.Driver");
            mergekeyValue(sb, "total.rows", loadDataResult.getSuccessWrite());
            mergekeyValue(sb, "engine.message.buffer.size",
                this.getExtPropValueWithDefault("engine.message.buffer.size", "500"));

            StringBuffer ignoreErrorsSql = new StringBuffer();
            if (APPEND_FLAG.equalsIgnoreCase(loadMode)) {
                ignoreErrorsSql.append("ALTER TABLE ").append(dbTableName)
                    .append(" ADD COLUMN etl_stamp character varying(200);");
                String[] dbTableNameTmps = dbTableName.split("\\.");
                String dbTableNameWithoutSchema = dbTableName;
                if (dbTableNameTmps.length == 2) {
                    dbTableNameWithoutSchema = dbTableNameTmps[1];
                }
                ignoreErrorsSql.append("CREATE INDEX ").append(dbTableNameWithoutSchema).append("_etl_stamp_idx ON ")
                    .append(dbTableName).append(" (etl_stamp)");
            }
            this.writeLocalLog(Level.INFO, "ignore.errors.sql=" + ignoreErrorsSql.toString());
            mergekeyValue(sb, "ignore.errors.sql", ignoreErrorsSql.toString());

            /* Create the DX control file directory. */
            File f = new File(DX_TEMP_PATH + "/dxctl/" + task.getId() + '/');
            if (!f.exists()) {
                f.mkdirs();
            }

            String dxCtrlFileName =
                    task.getId() + "_" + new SimpleDateFormat("yyyyMMddHHmmss").format(task.getCurRunDate()) + ".dxctl";
            String dxCtrlFileFullPath = f.getAbsolutePath() + "/" + dxCtrlFileName;
            dxCtrlFileFullPath = new File(dxCtrlFileFullPath).getAbsolutePath();

            CommonUtils.string2File(sb.toString(), dxCtrlFileFullPath, "UTF-8");
            this.writeLocalLog(Level.INFO, "DX ctrl file generated: " + dxCtrlFileFullPath);

            /**
             * Exec command
             */
            String[] cmds = { DX_HOME + "/run.sh", DX_TEMPLATE_PATH, dxCtrlFileFullPath };
            this.writeLocalLog(Level.INFO, "DX Command:" + Arrays.deepToString(cmds));
            RunTaskProcess dxProcess = new RunTaskProcess(cmds, this);
            dxProcess.startProcess(false);

            String dxPid = String.valueOf(dxProcess.getProcessId());
            this.writeLocalLog(Level.INFO, "DX process is running, pid=" + dxPid);
            // commitTaskAndLog(LState.RUNNING, dxPid,
            // "DX process is running, pid=" + dxPid);

            dxProcess.waitAndDestroyProcess();
            int exitValue = dxProcess.getExitVal();
            this.writeLocalLog(Level.INFO, "All done, exit value=" + exitValue);

            String output = new String();
            output = dxProcess.getNormalResult() + dxProcess.getErrResult();
            if (StringUtils.isBlank(output))
                output = "{}";
            this.writeLocalLog(Level.INFO, "Output:" + output);

            String taskDesc = null;
            try {
                int failRead = 0, successRead = 0, successWrite = 0, failWrite = 0;
                /* Parse the DX output. */
                JSONObject dbObject = new JSONObject(output);
                failRead = dbObject.getInt("failed_read");
                successRead = dbObject.getInt("success_read");
                successWrite = dbObject.getInt("success_writed");
                failWrite = dbObject.getInt("failed_writed");
                taskDesc = dbObject.getString("task_desc");

                /* Generate the TDWToPostgre output. */
                keyValues.put("exit_code", "0");
                keyValues.put("success_writed", String.valueOf(successWrite));
                keyValues.put("failed_writed", String.valueOf(failRead + failWrite));
                keyValues.put("run_date", SECOND_FORMAT.format(runDate));
                keyValues.put("task_desc", taskDesc);

                float failWritePercentage =
                        (failRead + successRead - successWrite) / ((float) (failRead + successRead));
                failWritePercentage = failWritePercentage * 100;

                /*
                 * The whole process succeeds if the fail write percentage is
                 * below the error percentage, otherwise we think it fails.
                 */
                if (failWritePercentage > new Float(errorPercentage)) {
                    String failMessage = "The failure percentage is beyond: " + errorPercentage + ", the task fails";
                    keyValues.put("task_desc", failMessage);
                    commitJsonResult(keyValues, false, "");
                    isCommitted = true;
                    return;
                }

            }
            catch (Exception e) {
                this.writeLocalLog(Level.SEVERE, "Failed to parse DX output.");
            }

            if (exitValue != 0) {
                writeLocalLog(Level.SEVERE, "Load data to PG using DX failed: " + taskDesc);
                keyValues.put("exit_code", String.valueOf(dxProcess.getExitVal()));
                keyValues.put("run_date", SECOND_FORMAT.format(runDate));
                commitJsonResult(keyValues, false, dxPid);
                isCommitted = true;
                return;
            }

            // commitTaskAndLog(LState.RUNNING, "FORMATTED_LOG", output);
            // this.writeLocalLog(Level.INFO, "=====================\n");

            keyValues.put("task_desc", "Load data from TDW to PG succeeds!");
            isSuccess = true;
        }
        catch (Exception e) {
            keyValues.put("exit_code", RunnerUtils.UNKOWN_ERROR_CODE);
            keyValues.put("task_desc", "TDW exported: " + CommonUtils.stackTraceToString(e));
            keyValues.put("run_date", SECOND_FORMAT.format(runDate));
        }
        finally {
            try {
                doCleanWork(plcProgram, hiveServer, externalTableDb, externalTableName, hadoopCommand, nameNodeServer,
                    externalTableDataPath);
            }
            catch (Exception e) {
                this.writeLocalLog(Level.SEVERE, CommonUtils.stackTraceToString(e));
            }

            try {
                if (!isCommitted) {
                    commitJsonResult(keyValues, isSuccess, plcParameter);
                }
            }
            catch (Exception e) {
                this.writeLocalLog(Level.SEVERE, CommonUtils.stackTraceToString(e));
            }
        }
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
