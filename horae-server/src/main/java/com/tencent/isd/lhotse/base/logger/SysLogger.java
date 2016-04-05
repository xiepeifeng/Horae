package com.tencent.isd.lhotse.base.logger;

import java.sql.SQLException;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.MDC;

import com.tencent.isd.lhotse.base.LDBBroker;
import com.tencent.isd.lhotse.dao.SysLog;

/**
 * @author: cpwang
 * @date: 2012-4-17
 */
public class SysLogger extends DBLogger<SysLog> {
    private static SysLogger instance = null;
    static Log               logger   = LogFactory.getLog(SysLogger.class);


    public static void init() {
        if (instance == null) {
            instance = new SysLogger();
            instance.start("SysLogger");
        }
    }


    private static synchronized SysLog appendLog(String src, String type, String desc, String trg,
                                                 String brokerIP, long time, int taskType) {
        SysLog sl = new SysLog(src, type, desc, trg, brokerIP, time, taskType);
        instance.appendLog(sl);
        return sl;
    }


    public static void info(String src, String desc, String trg, String brokerIP, long time,
                            int taskType) {
        SysLog sl = appendLog(src, "info", desc, trg, brokerIP, time, taskType);
        logger.info(sl.toString());
    }


    public static void info(String src, String desc, String brokerIP, long time, int taskType) {
        info(src, desc, "", brokerIP, time, taskType);
    }


    public static void info(String src, String desc, String brokerIP, long time) {
        info(src, desc, "", brokerIP, time, -1);
    }


    public static void info(String src, String desc, String brokerIP, int taskType) {
        info(src, desc, "", brokerIP, 0, taskType);
    }


    public static void info(String src, String desc, String brokerIP) {
        info(src, desc, "", brokerIP, 0, -1);
    }


    public static void warn(String src, String desc, String trg, String brokerIP, long time,
                            int taskType) {
        SysLog sl = appendLog(src, "warn", desc, trg, brokerIP, time, taskType);
        logger.warn(sl.toString());
    }


    public static void warn(String src, String desc, String brokerIP, long time) {
        warn(src, desc, "", brokerIP, time, -1);
    }


    public static void warn(String src, String desc, String brokerIP) {
        warn(src, desc, "", brokerIP, 0, -1);
    }


    public static void error(String src, String desc, String trg, String brokerIP, long time,
                             int taskType) {
        SysLog sl = appendLog(src, "error", desc, trg, brokerIP, time, taskType);
        logger.error(sl.toString());
    }


    public static void error(String src, String desc, String brokerIP, long time) {
        error(src, desc, "", brokerIP, time, -1);
    }


    public static void error(String src, String desc, String brokerIP) {
        error(src, desc, "", brokerIP, 0, -1);
    }


    public static void error(String src, Exception e) {
        appendLog(src, "info", e.getMessage(), "", "", 0, -1);
        logger.error(e);
    }


    @Override
    protected void insertLogs(LDBBroker broker, ConcurrentLinkedQueue<SysLog> logs)
            throws SQLException {
        broker.insertSysLog(logs);
    }
}
