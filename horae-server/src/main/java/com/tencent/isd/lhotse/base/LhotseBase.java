/**
 * @author cpwang
 * @since 2009-2-24
 */
package com.tencent.isd.lhotse.base;

import com.tencent.isd.lhotse.base.logger.SysLogger;
import com.tencent.isd.lhotse.base.logger.TaskLogger;
import com.tencent.isd.lhotse.base.socket.TaskServer;
import com.tencent.isd.lhotse.util.LHostUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author cpwang
 * @since 2012-3-23
 */
public class LhotseBase {
    Log                        logger           = LogFactory.getLog(LhotseBase.class);
    public static final String BASE_CFG_PATH    = "./cfg/lhotse.properties";

    public static int          TASK_SERVER_PORT = 8516;

    public static boolean      DEBUG_MODE       = false;

    public static String       baseIP           = "localhost";


    public LhotseBase() {
        try {
            baseIP = LHostUtil.getHostIP();
        } catch (SocketException e) {
            logger.error(e.getMessage(), e);
        } catch (UnknownHostException e) {
            logger.error(e.getMessage(), e);
        }

       // FileLogger.init("lhotse_base");
        // initialize file logger
        LDBBroker broker = null;
        try {
            configure();
            broker = new LDBBroker();
            BaseCache.init(broker);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        } catch (Exception e) {
            e.printStackTrace();
            return;
        } finally {
            if (broker != null) {
                broker.close();
            }
        }

        // start sys logger
        SysLogger.init();
        SysLogger.info("LhotseBase", "Starting Lhotse Base...", baseIP);
        if (DEBUG_MODE)
            SysLogger.info("LhotseBase", "[NOTE] Debug mode enabled!", baseIP);

        // start task logger
        TaskLogger.init();
        SysLogger.info("LhotseBase", "Starting Task Logger...", baseIP);

        // clear and initialize
        SysLogger.info("LhotseBase", "Starting Task Initializer...", baseIP);
        TaskInitializer ti = new TaskInitializer();
        ti.start();

        // start server
        SysLogger.info("LhotseBase", "Starting Task Server..", baseIP);
        TaskServer ts = new TaskServer();
        ts.start();

        // start command executor
        SysLogger.info("LhotseBase", "Starting Command Executor...", baseIP);
        CommandExecutor ce = new CommandExecutor();
        ce.start();

        // start maintainer thread
        SysLogger.info("LhotseBase", "Starting Maintainer...", baseIP);
        MaintainerThread maintainer = new MaintainerThread();
        maintainer.start();

        // start monitor thread
        SysLogger.info("LhotseBase", "Starting Monitor...", baseIP);
        MonitorThread monitor = new MonitorThread();
        monitor.start();

        SysLogger.info("LhotseBase", "Successfully started Lhotse Base!", baseIP);
    }


    private void configure() throws IOException, SQLException {
        FileInputStream fis1 = new FileInputStream(BASE_CFG_PATH);
        Properties props1 = new Properties();
        props1.load(fis1);
        // LDBBroker.setConf(props1.getProperty("ldb_ip"), props1.getProperty("ldb_port"),
        // props1.getProperty("ldb_database"), props1.getProperty("ldb_username"),
        // props1.getProperty("ldb_password"));
        TASK_SERVER_PORT = Integer.parseInt(props1.getProperty("task_server_port"));
        DEBUG_MODE = Boolean.parseBoolean(props1.getProperty("debug_mode"));
        fis1.close();
    }


    public static void main(String[] args) {
        new LhotseBase();
    }


    public static String getBaseIP() {
        return baseIP;
    }

}
