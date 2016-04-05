package com.tencent.isd.lhotse.util;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * @author: cpwang
 * @date: 2012-8-23
 */
public class LFileLogger {

	private Logger logger;

	public LFileLogger(String logDir, String logName, boolean dailyRolling) {
		File dir = new File(logDir);
		if (!dir.exists()) {
			dir.mkdirs();
		}

		String fileName = logName;
		if (!logName.toLowerCase().endsWith(".log")) {
			fileName = logName + ".log";
		}

		String logFile = logDir + File.separator + fileName;
		logger = Logger.getLogger(logFile);
		logger.setLevel(Level.INFO);

		// daily rolling pattern
		if (dailyRolling) {
			PatternLayout infoLayout = new PatternLayout("[%-d{yyyy-MM-dd HH:mm:ss}]-[%p] %m %n");
			try {
				DailyRollingFileAppender infoAppender = new DailyRollingFileAppender(infoLayout, logFile,
						"'.'yyyy-MM-dd");
				infoAppender.setAppend(true);
				infoAppender.setThreshold(Level.INFO);
				infoAppender.activateOptions();
				logger.addAppender(infoAppender);
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void info(String log) {
		logger.info(log);
	}

	public void warn(String log) {
		logger.warn(log);
	}

	public void error(String log) {
		logger.error(log);
	}

	public void error(Throwable t) {
		logger.error(t);
	}

	public void log(Level level, String log) {
		logger.log(level, log);
	}
}
