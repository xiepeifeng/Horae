package com.tencent.isd.lhotse.runner.api;

import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.runner.AbstractTaskRunner;
import com.tencent.isd.lhotse.runner.api.util.RunnerUtils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.logging.Level;

public abstract class DDCTaskRunner extends AbstractTaskRunner {
	protected final SimpleDateFormat RUN_DATE_FORMAT = 
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	protected final SimpleDateFormat HOUR_FORMAT =
		new SimpleDateFormat("yyyyMMddHH");
	protected static final String HDFS_SYSTEM = "hdfs";
	protected static final String LINUX_SYSTEM = "linux";
	protected static final String HOUR = "H";
	protected static final String DAY = "D";
	protected static final String MONTH = "M";
	protected static final String WEEK = "W";
	
	protected Date getRealDataDate(LTask task) 
	    throws IOException {
		
		Date runDate = new Date(task.getCurRunDate());
		
		/* Start running, return a status to base. */
		String logString = 
			"Start running Runner:" + task.getId() + ", run date:" +
			RUN_DATE_FORMAT.format(runDate);
		writeLocalLog(Level.INFO, logString);
		
		/* Calculate the real data date. */
		String offRangeExpr = this.getExtPropValue("offRange");
		String offRangeType = this.getExtPropValue("offRangeType");
		String periodDayExpr = null;
		if (WEEK.equals(task.getCycleUnit())) {
			periodDayExpr = this.getExtPropValue("dayOfWeek");
		} else if (MONTH.equals(task.getCycleUnit())) {
			periodDayExpr = this.getExtPropValue("dayOfMonth");
		}
		
		if ((offRangeExpr == null) || (offRangeType == null)) {
			logString = 
				"The data date for Runner: " + task.getId() + " is: " +
				RUN_DATE_FORMAT.format(runDate);
			writeLocalLog(Level.INFO, logString);			
			return runDate;
		}
		
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(runDate);
		int offRange = new Integer(offRangeExpr);
		int periodDay = 0;
		if (periodDayExpr != null) {
		    periodDay = new Integer(periodDayExpr);
		}
		
		if (HOUR.equals(task.getCycleUnit())) {
			if (offRange == 0) {
				calendar.add(Calendar.HOUR_OF_DAY, 1);
			}
		} else if (DAY.equals(task.getCycleUnit())) {
			if (offRange == 0) {
				calendar.add(Calendar.DAY_OF_YEAR, 1);
			}
		} else if (WEEK.equals(task.getCycleUnit())) {
			calendar.add(Calendar.WEEK_OF_YEAR, 1);
			calendar.add(Calendar.DAY_OF_YEAR, periodDay - 1);
			if (offRange != 0) {
				calendar.add(Calendar.DAY_OF_YEAR, -offRange);
			}
		} else if (MONTH.equals(task.getCycleUnit())) {
			if (offRange == 0) {
				calendar.add(Calendar.MONTH, 1);
			    calendar.add(Calendar.DAY_OF_YEAR, periodDay - 1);
			} else {
			    if (RunnerUtils.MONTH_EXPR.equals(offRangeType)) {
					calendar.add(Calendar.DAY_OF_YEAR, periodDay - 1);
				} else if (RunnerUtils.DAY_EXPR.equals(offRangeType)) {
					calendar.add(Calendar.MONTH, 1);
					calendar.add(Calendar.DAY_OF_YEAR, periodDay - 1);
					calendar.add(Calendar.DAY_OF_YEAR, -offRange);
				}
			}
		}
		
		logString = 
			"The data date for Runner: " + task.getId() + " is: " +
			RUN_DATE_FORMAT.format(calendar.getTime());
		writeLocalLog(Level.INFO, logString);
		
		return calendar.getTime();
	}
	
	protected boolean skipWeeklyTaskExecution(LTask task, Date dataDate) 
	    throws IOException {
		
		if (WEEK.equals(task.getCycleUnit()) && 
			this.getExtPropValue("dayOfMonth") != null) {
			
			int dayOfMonth = new Integer(this.getExtPropValue("dayOfMonth"));
			int offRange = new Integer(this.getExtPropValue("offRange"));
			
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(dataDate);
			if (offRange != 0) {
				calendar.add(Calendar.DAY_OF_YEAR, offRange);
			}			
			int monthDay = calendar.get(Calendar.DAY_OF_MONTH);
			
			if (dayOfMonth == monthDay) {
				String successMessage =
					"This weekly task conflicts with a monthly task " +
				    "running date, which is an old weekly and monthly " +
					"task in USP, we shouldn't run it";
				writeLogAndCommitTask(successMessage, Level.INFO, LState.SUCCESSFUL);
					
				return true;
			}
		}
		
		return false;
	}
	
	protected void writeLogAndCommitTask(String message, 
			                             Level messageLevel, 
			                             LState state) 
	    throws IOException {
		
		this.writeLocalLog(messageLevel, message);
		commitTask(state, "", message);
	}
	
	protected SimpleDateFormat getDateFormat(String offRange, String offRangeType, String cycleUnit) {
		SimpleDateFormat formatter = null;
		
		writeLocalLog(Level.INFO, "offRangeType: " + offRangeType + ", offRange: " + offRange);
		writeLocalLog(Level.INFO, "equals day: " + RunnerUtils.DAY_EXPR.equalsIgnoreCase(offRangeType));
		if ((offRangeType != null) && (offRange != null)) {
			writeLocalLog(Level.INFO, "It's a converted task");
			if (RunnerUtils.HOUR_EXPR.equalsIgnoreCase(offRangeType)) {
				formatter = new SimpleDateFormat("yyyyMMddHH");
			} else if (RunnerUtils.DAY_EXPR.equalsIgnoreCase(offRangeType)) {
				writeLocalLog(Level.INFO, "We find the formatter");
				formatter = new SimpleDateFormat("yyyyMMdd");
			} else if (RunnerUtils.MONTH_EXPR.equalsIgnoreCase(offRangeType)) {
				formatter = new SimpleDateFormat("yyyyMM");
			}
		}
		
		if ((offRangeType == null) && (offRange == null)) {
			if (HOUR.equalsIgnoreCase(cycleUnit)) {
				formatter = new SimpleDateFormat("yyyyMMddHH");
			} else if (DAY.equalsIgnoreCase(cycleUnit)) {
				formatter = new SimpleDateFormat("yyyyMMdd");
			} else if (MONTH.equalsIgnoreCase("yyyyMM")) {
				formatter = new SimpleDateFormat("yyyyMM");
			}
		}		
		
		return formatter;
	}
	
	protected String getPLCParameter(String taskId, Date runDate) {
		return taskId + "_" + HOUR_FORMAT.format(runDate);
	}
}
