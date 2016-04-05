package com.tencent.isd.lhotse.base;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Vector;

import com.tencent.isd.lhotse.base.alert.AbstractAlerter;
import com.tencent.isd.lhotse.base.alert.BOSSAlerter;
import com.tencent.isd.lhotse.base.alert.TOFAlerter;
import com.tencent.isd.lhotse.base.logger.SysLogger;
import com.tencent.isd.lhotse.dao.Runner;
import com.tencent.isd.lhotse.dao.Status;
import com.tencent.isd.lhotse.dao.Task;
import com.tencent.isd.lhotse.dao.TaskRun;
import com.tencent.isd.lhotse.dao.TaskType;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.util.LDateUtil;

/**
 * @author cpwang
 * @since 2012-3-23
 */
public class MonitorThread extends CommonThread {
	private String warnTag;
	private AbstractAlerter alerter;
	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private final int MONITOR_INTERVAL = 60;
	private boolean sendOver = false;
	private final int MAX_WARN_RECORDS_ONCE = 1000;

	@Override
	public void run() {

		while (true) {
			// sleep some time
			try {
				Thread.sleep(MONITOR_INTERVAL * 60000);
			}
			catch (InterruptedException e) {
			}

			long startTime = System.currentTimeMillis();
			SysLogger.info(this.getThreadName(), "Monitor starting...", LhotseBase.getBaseIP());
			LDBBroker dbbMain = null;
			try {
				dbbMain = new LDBBroker();
				int taskThreshold = BaseCache.getIntegerParameter(dbbMain, "task_threshold");
				int triesThreshold = BaseCache.getIntegerParameter(dbbMain, "task_tries_threshold");
				int instanceThreshold = BaseCache.getIntegerParameter(dbbMain, "task_instances_threshold");
				String warnReceivers = BaseCache.getStringParameter(dbbMain, "warn_receivers");
				String deployDistrict = BaseCache.getStringParameter(dbbMain, "deploy_district");
				String enableSMSStr = BaseCache.getStringParameter(dbbMain, "task_enable_sms");
				String enableRTXStr = BaseCache.getStringParameter(dbbMain, "task_enable_rtx");
				boolean enableSMS = false;
				boolean enableRTX = false;

				if (enableSMSStr != null && enableSMSStr.equals("Y"))
					enableSMS = true;

				if (enableRTXStr != null && enableRTXStr.equals("Y"))
					enableRTX = true;

				if ((deployDistrict == null) || (deployDistrict.trim().equals(""))) {
					SysLogger.info(this.getThreadName(),
							"Deploy district [deploy_district] not configured, monitor disabled!",
							LhotseBase.getBaseIP());
					continue;
				}

				if (deployDistrict.toUpperCase().startsWith("SHENZHEN")) {
					// shenzhen is by TOF
					alerter = new TOFAlerter();
				}
				else if (deployDistrict.toUpperCase().startsWith("GUANGZHOU")) {
					// separator district is by BOSS
					alerter = new BOSSAlerter(BOSSAlerter.GUANGZHOU);
				}
				else if (deployDistrict.toUpperCase().startsWith("SHANTOU")) {
					alerter = new BOSSAlerter(BOSSAlerter.SHANTOU);
				}
				else {
					SysLogger.warn(this.getThreadName(), "Invalid deploy district:" + deployDistrict
							+ ", monitor disabled!", LhotseBase.getBaseIP());
					continue;
				}

				warnTag = "Lhotse-" + deployDistrict + ":";
				// warn disregarded tasks
				warnUncompletedTasks(dbbMain, triesThreshold, taskThreshold, instanceThreshold, warnReceivers,
						enableSMS, enableRTX);
				// cpwang,20120820,warn hanged runners
				//warnHangedRunners(dbbMain);
			}
			catch (UnknownHostException e) {
				SysLogger.error(this.getThreadName(), e);
			}
			catch (SQLException e) {
				SysLogger.error(this.getThreadName(), e);
			}
			catch (Exception e) {
				e.printStackTrace();
				SysLogger.error(this.getThreadName(), e);
			}
			finally {
				if (dbbMain != null) {
					dbbMain.close();
				}
			}
			long endTime = System.currentTimeMillis();
			SysLogger.info(this.getThreadName(), "Monitor completed.", LhotseBase.getBaseIP(), endTime - startTime);
		}
	}

	public void warnUncompletedTasks(LDBBroker broker, int triesThreshold, int taskThreshold, int instanceThreshold,
			String warnReceivers, boolean enableSMS, boolean enableRTX) throws SQLException, IOException {
		HashMap<String, Integer> tasksByInCharge = new HashMap<String, Integer>();
		HashMap<String, Integer> tasksByIdAndInCharge = new HashMap<String, Integer>();

		Vector<TaskType> taskTypes = broker.getTaskTypes();
		int totalCnt = 0;
		for (TaskType tt : taskTypes) {
			ArrayList<Task> tasks = broker.getTasks(tt.getId());
			LinkedList<TaskRun> taskRuns = broker.getUnsuccessfulRuns(tt.getId());
			Iterator<TaskRun> iter = taskRuns.iterator();

			while (iter.hasNext()) {
				TaskRun tr = iter.next();
				Task task = null;
				for (Task t : tasks) {
					if (t.getId().equals(tr.getId())) {
						task = t;
						break;
					}
				}

				if (task == null) {
					continue;
				}

				if (!task.getStatus().equalsIgnoreCase(Status.NORMAL)) {
					continue;
				}

				if ((tr.getState() == LState.FAILED_VALUE) && (tr.getTryLimit() == 0)) {
					continue;
				}

				++totalCnt;

				// collect uncompleted tasks by in charge
				Integer iBIC = tasksByInCharge.get(task.getInCharge());
				int i1 = 0;
				if (iBIC != null) {
					i1 = iBIC.intValue() + 1;
				}
				else {
					i1 = 0;
				}
				tasksByInCharge.put(task.getInCharge(), i1);

				String strIdInCharge = tr.getId() + "," + task.getName() + "|" + task.getInCharge();
				Integer iI = tasksByIdAndInCharge.get(strIdInCharge);
				int i2 = 0;
				if (iI != null) {
					i2 = iI.intValue() + 1;
				}
				else {
					i2 = 0;
				}
				tasksByIdAndInCharge.put(strIdInCharge, i2);

				if (tr.getTries() >= triesThreshold) {
					String strDate = LDateUtil.getFullDateString(tr.getCurRunDate());
					String str = warnTag + "{任务实例" + tr.getId() + "," + strDate + "," + task.getName() + "}已经失败"
							+ tr.getTries() + "次";
					// stop disregarded tasks, cpwang,20111024
					SysLogger.warn(this.getThreadName(), str + "." + task.getInCharge(), LhotseBase.getBaseIP());
					// send RTX
					if (enableRTX)
						alerter.sendRTX(task.getInCharge(), LhotseBase.baseIP, warnTag, str);
					if (!isMidnight() && enableSMS)
						alerter.sendSMS(task.getInCharge(), LhotseBase.baseIP, warnTag, str);
				}
			}
		}

		// system alert must be sent to warn receivers
		if ((warnReceivers != null) && (!warnReceivers.trim().equals(""))) {
			// send all uncompleted task to administrator. just at 11:x am and
			// 4:x pm
			Calendar c = Calendar.getInstance();
			int hour = c.get(Calendar.HOUR_OF_DAY);
			if (((hour >= 11) && (hour < 12)) || ((hour >= 16) && (hour < 17))) {
				// send between 11 clock and 12 clock,or between 16 and 17
				if (sendOver == false) {
					String log = warnTag + "系统总共有 " + totalCnt + "个任务实例未完成";
					SysLogger.warn(this.getThreadName(), log, LhotseBase.getBaseIP());
					// to avoid duplicated send
					// always send system info to administrator
					alerter.sendRTX(warnReceivers, LhotseBase.baseIP, warnTag, log);
					alerter.sendSMS(warnReceivers, LhotseBase.baseIP, warnTag, log);
					sendOver = true;
				}
			}
			else {
				sendOver = false;
			}
		}

		// warn by in charge
		Iterator<Entry<String, Integer>> iter2 = tasksByInCharge.entrySet().iterator();
		Entry<String, Integer> ent2 = null;
		while (iter2.hasNext()) {
			ent2 = iter2.next();
			String inCharge = ent2.getKey();
			int cnt = ent2.getValue();
			if (cnt > taskThreshold) {
				String log = warnTag + "你有" + cnt + "个任务实例未完成";
				SysLogger.warn(this.getThreadName(), log + "." + inCharge, LhotseBase.getBaseIP());
				if (enableRTX)
					alerter.sendRTX(inCharge, LhotseBase.baseIP, warnTag, log);
			}
		}

		// warn by in charge
		Iterator<Entry<String, Integer>> iter3 = tasksByIdAndInCharge.entrySet().iterator();
		Entry<String, Integer> ent3 = null;
		int warnings = 0;
		while (iter3.hasNext()) {
			ent3 = iter3.next();
			String IdInCharge = ent3.getKey();
			String[] strs = IdInCharge.split("\\|");
			if (strs.length < 2) {
				continue;
			}
			String task = strs[0];
			String inCharge = strs[1];
			int cnt = ent3.getValue();
			if (cnt >= instanceThreshold) {
				String log = warnTag + "{任务" + task + "}有 " + cnt + "个未完成的实例";
				SysLogger.warn(this.getThreadName(), log + "." + inCharge, LhotseBase.getBaseIP());
				if (enableRTX)
					alerter.sendRTX(inCharge, LhotseBase.baseIP, warnTag, log);
				if (!isMidnight() && enableSMS)
					alerter.sendSMS(inCharge, LhotseBase.baseIP, warnTag, log);

				// cpwang,20120911.
				// if there are too many warnings, TOF will throw
				// "Too many open files" error
				// so after sending some warnings, sleep some time
				if (warnings++ >= MAX_WARN_RECORDS_ONCE) {
					break;
					/*
					 * try { Thread.sleep(10000); } catch (InterruptedException e) { } warnings = 0;
					 */
				}
			}
		}
	}

	private static boolean isMidnight() {
		Calendar c = Calendar.getInstance();
		int hour = c.get(Calendar.HOUR_OF_DAY);
		if (hour >= 9 && hour < 22)
			return false;
		return true;
	}

	@SuppressWarnings("unused")
	private void warnHangedRunners(LDBBroker broker) throws SQLException, IOException {
		java.sql.Timestamp sysTimestamp = broker.getSysTimestamp();
		ArrayList<Runner> runners = broker.getRunners();
		for (Runner r : runners) {
			if (!r.getStatus().equalsIgnoreCase(Status.NORMAL)) {
				continue;
			}
			ArrayList<TaskType> taskTypes = BaseCache.getTaskTypes();
			int pollingSeconds = 0;
			for (TaskType tt : taskTypes) {
				if (r.getTaskType() == tt.getId()) {
					pollingSeconds = tt.getPollingSeconds();
					break;
				}
			}
			if ((r.getLastHeartbeat() == null)
					|| (sysTimestamp.getTime() - r.getLastHeartbeat().getTime() >= pollingSeconds * 5000)) {
				String strLH = "null";
				if (r.getLastHeartbeat() != null) {
					strLH = sdf.format(r.getLastHeartbeat());
				}
				String log = warnTag + "Heartbeat of type " + r.getTaskType() + " at " + r.getBrokerIP()
						+ " lost since " + strLH + ":" + r.getExecutablePath();
				SysLogger.warn(this.getThreadName(), log + "." + r.getInCharge(), LhotseBase.getBaseIP());
				alerter.sendRTX(r.getInCharge(), LhotseBase.baseIP, warnTag, log);
				alerter.sendSMS(r.getInCharge(), LhotseBase.baseIP, warnTag, log);
			}
		}
	}
}
