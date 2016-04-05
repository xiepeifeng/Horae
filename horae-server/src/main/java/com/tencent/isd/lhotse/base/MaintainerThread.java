package com.tencent.isd.lhotse.base;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.TreeSet;

import com.tencent.isd.lhotse.base.logger.SysLogger;
import com.tencent.isd.lhotse.base.logger.TaskLogger;
import com.tencent.isd.lhotse.dao.Status;
import com.tencent.isd.lhotse.dao.Task;
import com.tencent.isd.lhotse.dao.TaskRun;
import com.tencent.isd.lhotse.dao.TaskType;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;

/**
 * @author cpwang
 * @since 2012-3-23
 */
public class MaintainerThread extends CommonThread {
	private final int MAINTAIN_INTERVAL = 30;
	private final int LOG_RESERVED_DAYS = 62;
	private final int PART_PRESERVED = 3;
	private final int MAX_STAT_INSTANCES = 30;
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

	@Override
	public void run() {
		while (true) {
			// sleep some time
			try {
				Thread.sleep(MAINTAIN_INTERVAL * 60000);
			}
			catch (InterruptedException e) {
			}
			long startTime = System.currentTimeMillis();
			SysLogger.info(this.getThreadName(), "Maintainer starting...", LhotseBase.getBaseIP());
			LDBBroker broker = null;
			try {
				broker = new LDBBroker(false);// standby database
				int stopCnt = 0;
				// get obsolete tasks
				final int oneoffReservedDays = BaseCache.getIntegerParameter(broker, "oneoff_reserved_days");
				HashSet<String> oneoffTaskIds = new HashSet<String>();
				int hangedRuns = 0;
				java.sql.Timestamp sysTimestamp = broker.getSysTimestamp();
				ArrayList<TaskType> types = BaseCache.getTaskTypes();
				// clear statistics
				broker.truncateRunStatistics();
				ArrayList<TaskRun> deleteRuns = new ArrayList<TaskRun>();
				ArrayList<String> deleteTaskIds = new ArrayList<String>();

				for (TaskType tt : types) {
					int taskType = tt.getId();
					// delete obsolete task instances
					ArrayList<Task> tasks = broker.getTasks(taskType);
					// find obsolete tasks and stop
					for (Task t : tasks) {
						if (t.getStatus().equalsIgnoreCase(Task.ONEOFF_CYCLE)) {
							oneoffTaskIds.add(t.getId());
							if (sysTimestamp.getTime() - t.getEndDate().getTime() >= oneoffReservedDays * 86400000) {
								// one-off task is obsolete, clear
								deleteTaskIds.add(t.getId());
							}
						}
						else {
							if ((t.getEndDate() != null) && (t.getEndDate().before(sysTimestamp))
									&& (t.getStatus() == Status.NORMAL)) {
								broker.updateTaskStatus(t.getId(), Status.NORMAL, Status.OBSOLETE);
								++stopCnt;
							}
						}
					}

					LinkedList<TaskRun> runs = broker.getTaskRuns(taskType);
					HashMap<String, TreeSet<TaskRun>> sortedTaskRuns = new HashMap<String, TreeSet<TaskRun>>();
					Iterator<TaskRun> iter2 = runs.iterator();

					while (iter2.hasNext()) {
						TaskRun tr = iter2.next();

						if ((tr.getCurRunDate() == null) || (tr.getNextRunDate() == null)
								|| (tr.getCurRunDate().compareTo(tr.getNextRunDate())) >= 0) {
							deleteRuns.add(tr);
							iter2.remove();
							continue;
						}

						long timeDiff = sysTimestamp.getTime() - tr.getCurRunDate().getTime();
						long runDateDiff = tr.getNextRunDate().getTime() - tr.getCurRunDate().getTime();
						// clear instances
						if (!oneoffTaskIds.contains(tr.getId())) {
							// one-off tasks handled separately
							int elapsedDays = (int) (timeDiff / 86400000);
							int reservedDays = (int) (Math.sqrt(runDateDiff) / 20);
							if (elapsedDays >= reservedDays) {
								// reserve about 100 cycles
								deleteRuns.add(tr);
								iter2.remove();
								continue;
							}
						}

						int state = tr.getState();
						if ((state == LState.RUNNING_VALUE) || (state == LState.KILLING_VALUE)) {
							int aliveWait = 0;
							for (Task t : tasks) {
								if (t.getId().equalsIgnoreCase(tr.getId())) {
									aliveWait = t.getAliveWait();
									break;
								}
							}
							// update hanged task instances
							java.sql.Timestamp lu = tr.getLastUpdate();
							if ((lu == null) || (lu.getTime() + aliveWait * 60000 < sysTimestamp.getTime())) {
								if (state == LState.RUNNING_VALUE) {
									tr.setState(LState.FAILED_VALUE);
								}
								else {
									tr.setState(LState.HANGED_VALUE);
								}
								broker.updateTaskRun(tr.getState(), 1, -1, true,
										"Exceuting overtime,set to failure compulsively!", tr.getId(),
										tr.getCurRunDate(), tr.getCurRunDate(), true);
								TaskLogger.warn(tr, "Exceuting overtime,set to failure compulsively!");
								++hangedRuns;
							}
						}
						else if (state == LState.SUCCESSFUL_VALUE) {
							int cycles = (int) (timeDiff / runDateDiff);
							if (cycles > MAX_STAT_INSTANCES + 2) {
								iter2.remove();
								continue;
							}

							String taskId = tr.getId();
							TreeSet<TaskRun> rsById = sortedTaskRuns.get(taskId);
							if (rsById == null) {
								rsById = new TreeSet<TaskRun>(new StatComparator());
								sortedTaskRuns.put(taskId, rsById);
							}
							rsById.add(tr);
							// contains MAX_STAT_INSTANCES+1 mostly
							while (rsById.size() > MAX_STAT_INSTANCES + 1) {
								rsById.remove(rsById.first());
							}
						}
					}

					// write back to database
					Iterator<TreeSet<TaskRun>> iter3 = sortedTaskRuns.values().iterator();
					TreeSet<TaskRun> trs = null;
					while (iter3.hasNext()) {
						trs = iter3.next();
						calculateRuns(trs, tasks);
						broker.insertRunStatistics(trs, MAX_STAT_INSTANCES);
					}
				}

				SysLogger.info(this.getThreadName(), stopCnt + " obsolete task(s) stopped.", LhotseBase.getBaseIP());

				SysLogger.info(this.getThreadName(), "Recovered " + hangedRuns + " hanged task instance(s).",
						LhotseBase.getBaseIP());

				// delete obsolete tasks and instances
				deleteTasksAndRuns(broker, deleteTaskIds, deleteRuns);

				// roll partition of task_log
				rollLogTablePartition(broker, LDBBroker.TASK_LOG_TABLE);
				// roll partition of sys_log
				rollLogTablePartition(broker, LDBBroker.SYS_LOG_TABLE);
			}
			catch (SQLException e) {
				SysLogger.error(this.getThreadName(), e);
			}
			catch (Exception e) {
				SysLogger.error(this.getThreadName(), e);
			}
			finally {
				if (SysOperLock.getLock() == SysOperLock.MAINTAINING_LOCK) {
					// release lock
					SysOperLock.setLock(SysOperLock.NULL_LOCK);
				}

				if (broker != null) {
					broker.close();
				}
			}
			long endTime = System.currentTimeMillis();
			SysLogger.info(this.getThreadName(), "Maintainer completed.", LhotseBase.getBaseIP(), endTime - startTime);
		}
	}

	private void rollLogTablePartition(LDBBroker broker, String tabName) throws SQLException {
		// get current day
		Calendar curCal = Calendar.getInstance();
		curCal.setTime(new java.util.Date());
		// truncate to day
		curCal.set(Calendar.HOUR_OF_DAY, 0);
		curCal.set(Calendar.MINUTE, 0);
		curCal.set(Calendar.SECOND, 0);

		// destination date is PART_PRESERVED days after current day
		Calendar destCal = Calendar.getInstance();
		destCal.add(Calendar.DAY_OF_YEAR, PART_PRESERVED);

		Calendar earliestCal = Calendar.getInstance();
		earliestCal.add(Calendar.DAY_OF_YEAR, -LOG_RESERVED_DAYS);

		// get maximum partition date exists
		HashMap<String, java.util.Date> parts = broker.getLogPartitions(tabName);
		java.util.Date maxDate = null;

		// get the latest date
		Iterator<Entry<String, java.util.Date>> iter = parts.entrySet().iterator();
		Entry<String, java.util.Date> ent = null;
		while (iter.hasNext()) {
			ent = iter.next();
			String pn = ent.getKey();
			java.util.Date dt = ent.getValue();
			if (dt.before(earliestCal.getTime())) {
				broker.dropLogPartition(tabName, pn);
				SysLogger.info(this.getThreadName(), "Partition " + pn + " of table " + tabName + " dropped.",
						LhotseBase.getBaseIP());
			}
			else {
				if ((maxDate == null) || (maxDate.before(dt))) {
					maxDate = dt;
				}
			}
		}

		if (maxDate == null) {
			maxDate = curCal.getTime();
		}

		Calendar maxCal = Calendar.getInstance();
		maxCal.setTime(maxDate);
		while (maxCal.compareTo(destCal) <= 0) {
			String partName = sdf.format(maxCal.getTime());
			maxCal.add(Calendar.DAY_OF_YEAR, 1);
			String partDate = sdf.format(maxCal.getTime());
			broker.splitMaxPartition(tabName, partDate, partName);
			SysLogger.info(this.getThreadName(), "Partition " + partName + " of table " + tabName + " created.",
					LhotseBase.getBaseIP());
		}
	}

	private void calculateRuns(TreeSet<TaskRun> trs, ArrayList<Task> tasks) {
		TaskRun tr = null;
		TaskRun trPrev = null;
		Iterator<TaskRun> iter = trs.iterator();
		while (iter.hasNext()) {
			tr = iter.next();

			if ((tr.getStartTime() != null) && (tr.getEndTime() != null)) {
				// calculate run duration
				long d = tr.getEndTime().getTime() - tr.getStartTime().getTime();
				if (d <= 0) {
					tr.setDuration(0);
				}
				else {
					tr.setDuration((float) d / 60000);
				}
			}

			if (trPrev != null) {
				float lf = trPrev.getDuration();
				if (lf == 0) {
					tr.setFlutter(0);
				}
				else {
					float cf = tr.getDuration();
					tr.setFlutter((cf - lf) / lf);
				}
			}
			else {
				tr.setFlutter(0);
			}

			boolean bFound = false;
			for (Task t : tasks) {
				if (t.getId().equalsIgnoreCase(tr.getId())) {
					tr.setCycleUnit(t.getCycleUnit());
					tr.setName(t.getName());
					tr.setInCharge(t.getInCharge());
					bFound = true;
				}
			}
			if (!bFound) {
				iter.remove();
				continue;
			}

			trPrev = tr;
		}
	}

	private void deleteTasksAndRuns(LDBBroker broker, ArrayList<String> taskIds, ArrayList<TaskRun> taskRuns)
			throws SQLException {
		// cpwang,20120911.lock for exclusive writing operating lb_task_run
		// mostly wait 5 minutes

		int tries = 0;
		boolean gotLock = true;
		while (SysOperLock.getLock() != SysOperLock.NULL_LOCK) {
			// wait for 1 minute to get lock
			try {
				Thread.sleep(60000);
			}
			catch (InterruptedException e) {
			}
			if (++tries >= 5) {
				gotLock = false;
				break;
			}
		}

		if (gotLock) {
			// get lock
			SysOperLock.setLock(SysOperLock.MAINTAINING_LOCK);
			// delete obsolete runs
			broker.deleteTaskRuns(taskRuns);
			for (String id : taskIds) {
				broker.deleteTasksCascade(id);
			}

			SysLogger.info(this.getThreadName(), taskRuns.size() + " instance(s) cleared," + taskIds.size()
					+ " obsolete task(s) cleared.", LhotseBase.getBaseIP());
		}
		else {
			SysLogger.info(this.getThreadName(), "Failed to get lock and can not make clear.", LhotseBase.getBaseIP());
		}
	}

	private class StatComparator implements Comparator<TaskRun> {

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		@Override
		public int compare(TaskRun tr1, TaskRun tr2) {
			if (!tr1.getId().equals(tr2.getId())) {
				return 1;
			}

			if (tr1.getCurRunDate() == null) {
				return 1;
			}

			if (tr2.getCurRunDate() == null) {
				return -1;
			}
			return tr1.getCurRunDate().compareTo(tr2.getCurRunDate());
		}

	}
}
