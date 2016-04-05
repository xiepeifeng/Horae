package com.tencent.isd.lhotse.base;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Vector;

import com.tencent.isd.lhotse.base.logger.SysLogger;
import com.tencent.isd.lhotse.dao.Task;
import com.tencent.isd.lhotse.dao.TaskRun;
import com.tencent.isd.lhotse.dao.TaskType;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.util.LDateUtil;

/**
 * @author cpwang
 * @since 2012-3-23
 */
public class TaskInitializer extends CommonThread {
	private final int TASK_CHECK_INTERVAL = 60000;

	@Override
	public void run() {
		LDBBroker broker = null;
		// run normally
		while (true) {
			if (SysOperLock.getLock() == SysOperLock.NULL_LOCK) {
				// cpwang,20120911.lock for exclusive writing task run
				SysOperLock.setLock(SysOperLock.INITIALIZING_LOCK);

				try {
					long startTime = System.currentTimeMillis();
					broker = new LDBBroker();
					SysLogger
							.info(this.getThreadName(), "Start initializing task instances...", LhotseBase.getBaseIP());
					// initialize all tasks' run dates
					java.sql.Timestamp sysTimestamp = broker.getSysTimestamp();
					int cnt = 0;
					Vector<TaskType> types = broker.getTaskTypes();
					ArrayList<String> partitions = broker.getRunPartitions();
				    // add partition for lb_task_run if this type do not partitions
					for (TaskType tt : types) {
						boolean bPart = false;
						for (String p : partitions) {
							if (p.equals(String.valueOf(tt.getId()))) {
								bPart = true;
								break;
							}
						}
						if (bPart == false) {
							SysLogger.info(this.getThreadName(), "Partition for type " + tt.getId()
									+ " does not exist, adding...", LhotseBase.getBaseIP());
							// add new partition for this type
							broker.addRunPartition(tt.getId());
							SysLogger.info(this.getThreadName(), "Partition for type " + tt.getId() + " added.",
									LhotseBase.getBaseIP());
						}

						// cnt += broker.insertTaskRun(tt.getId());
						ArrayList<Task> tasks = broker.getTasksToInitialize(tt.getId());
						ArrayList<TaskRun> taskRuns = new ArrayList<TaskRun>();
						for (Task t : tasks) {
							TaskRun tr = getNextInstance(t, t.getLastRunDate(), -1, sysTimestamp);
							if (tr != null) {
								taskRuns.add(tr);
							}
						}
						cnt += broker.insertTaskRuns(taskRuns);
					}
					long endTime = System.currentTimeMillis();
					SysLogger.info(this.getThreadName(), cnt + " task instance(s) initialized.",
							LhotseBase.getBaseIP(), (endTime - startTime));
				}
				catch (SQLException e) {
					SysLogger.error(this.getThreadName(), e);
				}
				catch (Exception e) {
					e.printStackTrace();
					SysLogger.error(this.getThreadName(), e);
				}
				finally {
					// release lock
					SysOperLock.setLock(SysOperLock.NULL_LOCK);

					if (broker != null) {
						broker.close();
					}
				}
			}

			// sleep some time
			try {
				Thread.sleep(TASK_CHECK_INTERVAL);
			}
			catch (InterruptedException e) {
			}
		}
	}

	public TaskRun getNextInstance(Task task, java.util.Date lastRundate, int state, java.sql.Timestamp sysTimestamp) {
		if (task.getCycleNumber() <= 0) {
			return null;
		}

		java.util.Date curRunDate = null;
		if (lastRundate == null) {
			curRunDate = task.getStartDate();
		}
		else {
			if (task.getCycleUnit().equalsIgnoreCase(Task.ONEOFF_CYCLE)) {
				// one-off task have instance already
				return null;
			}
			curRunDate = LDateUtil.getDateAfter(lastRundate, task.getCycleUnit(), task.getCycleNumber());
		}

		if ((task.getEndDate() != null) && (curRunDate.after(task.getEndDate()))) {
			return null;
		}

		return createTaskRun(task, curRunDate, state, sysTimestamp);
	}

	public TaskRun createTaskRun(Task task, java.util.Date curRunDate, int state, java.sql.Timestamp sysTimestamp) {
		java.util.Date nextRunDate = LDateUtil.getDateAfter(curRunDate, task.getCycleUnit(), task.getCycleNumber());

		// delay time not arrived
		if (nextRunDate.getTime() + task.getDelayTime() * 60000 > sysTimestamp.getTime()) {
			return null;
		}

		TaskRun tr = new TaskRun();
		tr.setId(task.getId());
		tr.setType(task.getType());
		tr.setCurRunDate(new java.sql.Timestamp(curRunDate.getTime()));
		tr.setNextRunDate(new java.sql.Timestamp(nextRunDate.getTime()));
		if (state >= 0) {
			tr.setState(state);
		}
		else {
			tr.setState(LState.READY_VALUE);
		}
		tr.setRedoFlag(0);
		tr.setTries(0);
		tr.setTryLimit(task.getTryLimit());
		tr.setRunPriority(task.getPriority());
		return tr;
	}
}
