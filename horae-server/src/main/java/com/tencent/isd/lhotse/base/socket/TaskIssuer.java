package com.tencent.isd.lhotse.base.socket;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;

import com.tencent.isd.lhotse.base.BaseCache;
import com.tencent.isd.lhotse.base.CommonThread;
import com.tencent.isd.lhotse.base.LDBBroker;
import com.tencent.isd.lhotse.base.logger.SysLogger;
import com.tencent.isd.lhotse.base.logger.TaskLogger;
import com.tencent.isd.lhotse.dao.Runner;
import com.tencent.isd.lhotse.dao.Server;
import com.tencent.isd.lhotse.dao.Status;
import com.tencent.isd.lhotse.dao.Task;
import com.tencent.isd.lhotse.dao.TaskLink;
import com.tencent.isd.lhotse.dao.TaskRun;
import com.tencent.isd.lhotse.dao.TaskRunSimple;
import com.tencent.isd.lhotse.dao.TaskType;
import com.tencent.isd.lhotse.message.MessageBroker;
import com.tencent.isd.lhotse.proto.LhotseObject.LKV;
import com.tencent.isd.lhotse.proto.LhotseObject.LServer;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.proto.LhotseObject.MessageType;

/**
 * @author: cpwang
 * @date: 2012-5-23
 */
public class TaskIssuer extends CommonThread {
	private String REQUEST_TAG = "TaskIssuer-Request";
	private String ISSUED_TAG = "TaskIssuer-Issued";
	private static final byte messageType = (byte) MessageType.REQUEST_TASK_VALUE;
	private static final long SLEEP_TIME = 1000L;
	private int taskType;
	private long lastUpdate;
	private int pollingSeconds = 60;
	private final int MAX_REQUESTS = 1024;
	private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private HashMap<Integer, TaskIssuerSorter> sorters = new HashMap<Integer, TaskIssuerSorter>();
	private ConcurrentLinkedQueue<Socket> requests = new ConcurrentLinkedQueue<Socket>();
	private ConcurrentHashMap<String, Integer> runningServers = new ConcurrentHashMap<String, Integer>();
	private ConcurrentHashMap<String, Integer> runningBrokers = new ConcurrentHashMap<String, Integer>();
	private ConcurrentHashMap<String, ArrayList<TaskRunSimple>> checkedTaskRunsCache = new ConcurrentHashMap<String, ArrayList<TaskRunSimple>>();
	private ConcurrentSkipListSet<TaskRun> candidateRuns = new ConcurrentSkipListSet<TaskRun>();
	private int brokerParallelism;
	private int taskParallelism;
	private int runnerPriority;
	private long sysTimestamp;
	private long sysDate;
	private long retryWaitMS;

	private IssueProbe probe;

	public TaskIssuer(int type) {
		super("TaskIssuer-" + type);
		this.taskType = type;
		lastUpdate = System.currentTimeMillis();
	}

	public TaskIssuer(int type, IssueProbe probe) {
		this(type);
		this.probe = probe;
	}

	public synchronized void addRequest(Socket socket) {
		lastUpdate = System.currentTimeMillis();
		String brokerIP = socket.getInetAddress().getHostAddress();
		SysLogger.info(REQUEST_TAG, "Request task of type " + taskType, brokerIP, taskType);

		if (requests.size() >= MAX_REQUESTS) {
			try {
				MessageBroker.sendNull(socket, messageType, (byte) 1);
				socket.close();
			}
			catch (IOException e) {
				SysLogger.error(this.getThreadName(), e);
			}
			catch (Exception e) {
				e.printStackTrace();
				SysLogger.error(this.getThreadName(), e);
			}
		}
		else {
			requests.offer(socket);
		}
	}

	public TaskRun getTaskToIssue(LDBBroker dbBroker, String brokerIP) throws SQLException {
		candidateRuns.clear();
		sorters.clear();
		runningServers.clear();
		runningBrokers.clear();
		checkedTaskRunsCache.clear();

		// get all tasks NEED run
		final java.sql.Timestamp sts = dbBroker.getSysTimestamp();

		if (sts == null) {
			return null;
		}

		this.sysTimestamp = sts.getTime();
		Calendar c = Calendar.getInstance();
		c.setTime(sts);
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		this.sysDate = c.getTime().getTime();

		TaskType tt = BaseCache.getTaskType(this.taskType);
		if (tt == null) {
			if (probe != null) {
				probe.setFeedback(IssueProbe.TYPE_NOT_REGISTERED, String.valueOf(this.taskType));
			}
			return null;
		}
		this.pollingSeconds = tt.getPollingSeconds();
		this.brokerParallelism = tt.getBrokerParallelism();
		this.taskParallelism = tt.getTaskParallelism();
		this.retryWaitMS = tt.getRetryWait() * 60000L;

		Runner runner = BaseCache.getRunner(taskType, brokerIP);
		if (runner == null) {
			if (probe != null) {
				probe.setFeedback(IssueProbe.RUNNER_NOT_AVAILABLE, brokerIP);
			}
			return null;
		}
		this.runnerPriority = runner.getPriorityLimit();

		// record heart beat
		dbBroker.updateRunner(runner, false);
		if (!runner.getStatus().equalsIgnoreCase(Status.NORMAL)) {
			// no matched runner found, or runner is not normal
			if (probe != null) {
				probe.setFeedback(IssueProbe.INVALID_RUNNER_STATUS, runner.getStatus());
			}
			return null;
		}

		ArrayList<Task> tasks = dbBroker.getTasks(this.taskType);
		if (tasks.size() <= 0) {
			// no task founds
			if (probe != null) {
				probe.setFeedback(IssueProbe.NO_TASK_FOUND, String.valueOf(this.taskType));
			}
			return null;
		}

		// get not successful task instances
		LinkedList<TaskRun> unsuccTaskRuns = dbBroker.getUnsuccessfulRuns(taskType);
		// find running and sentenced tasks
		Iterator<TaskRun> iter = unsuccTaskRuns.iterator();
		TreeSet<TaskRun> sentencedRuns = new TreeSet<TaskRun>();

		TaskRun tr = null;
		boolean bGotSentenced = false;
		while (iter.hasNext()) {
			tr = iter.next();

			if ((tr.getCurRunDate() == null) || (tr.getNextRunDate() == null)
					|| (tr.getCurRunDate().compareTo(tr.getNextRunDate()) >= 0)) {
				if (probe == null) {
					iter.remove();
					continue;
				}
				else {
					// probe task instance to diagnose
					if ((probe.getTaskId().equals(tr.getId()))
							&& (probe.getRunDate().compareTo(tr.getCurRunDate()) == 0)) {
						probe.setFeedback(IssueProbe.INVALID_RUN_DATE, tr.getCurRunDate() + "," + tr.getNextRunDate());
						return tr;
					}
				}
			}

			int state = tr.getState();
			Task task = null;
			for (Task t : tasks) {
				if (t.getId().equals(tr.getId())) {
					task = t;
					// set task's properties
					tr.setAction(task.getAction());
					tr.setSourceServer(task.getSourceServer());
					tr.setTargetServer(task.getTargetServer());
					tr.setCycleUnit(task.getCycleUnit());
					tr.setCycleNumber(task.getCycleNumber());
					tr.setSelfDepend(task.getSelfDepend());
					break;
				}
			}

			if (task == null) {
				iter.remove();
				continue;
			}
			// relate instance and task
			tr.setTask(task);

			if (state == LState.ROUGH_VALUE) {
				// issue anyway, ignore all restrictions
				return tr;
			}

			if ((state == LState.SENTENCED_VALUE) && brokerIP.equals(tr.getRuntimeBroker())) {
				// instance can be killed just when request broker and runtime
				// broker are identical
				bGotSentenced = true;
				sentencedRuns.add(tr);
				iter.remove();
				continue;
			}

			if (bGotSentenced) {
				continue;
			}

			// sorting
			TaskIssuerSorter sorter = sorters.get(task.getPriority());
			if (sorter == null) {
				sorter = new TaskIssuerSorter(task.getPriority(), dbBroker, brokerIP);
				sorters.put(task.getPriority(), sorter);
			}
			sorter.addTaskRun(tr);
		}

		if (sentencedRuns.size() > 0) {
			// issue the first one
			return sentencedRuns.first();
		}

		// start sorter
		Iterator<TaskIssuerSorter> sorterIter = sorters.values().iterator();
		TaskIssuerSorter sorter = null;
		while (sorterIter.hasNext()) {
			sorter = sorterIter.next();
			sorter.start();
		}
		// wait and check
		sorterIter = sorters.values().iterator();
		while (sorterIter.hasNext()) {
			sorter = sorterIter.next();
			while (!sorter.isCompleted()) {
				try {
					Thread.sleep(1);
				}
				catch (InterruptedException e) {
				}
			}
		}
		if (candidateRuns.isEmpty()) {
			return null;
		}
		return candidateRuns.first();
	}

	/* Assign the task properties to the object. */
	private LTask buildTask(LDBBroker broker, TaskRun tr) throws SQLException {
		LTask.Builder ltb = LTask.newBuilder();
		ltb.setPollingSeconds(this.pollingSeconds);
		ltb.setId(tr.getId());
		ltb.setAction(tr.getAction());
		ltb.setCurRunDate(tr.getCurRunDate().getTime());
		ltb.setNextRunDate(tr.getNextRunDate().getTime());
		ltb.setCycleNumber(tr.getCycleNumber());
		ltb.setCycleUnit(tr.getCycleUnit());
		ltb.setRedoFlag(tr.getRedoFlag() == 1);
		ltb.setState(LState.valueOf(tr.getState()));
		ltb.setType(tr.getType());
		int tries = tr.getTries();
		if (tries >= 9999) {
			// reset
			tries = 0;
		}
		else {
			tries += 1;
		}
		ltb.setTries(tries);
		ltb.setRuntimeId(tr.getRuntimeId() == null ? "" : tr.getRuntimeId());
		ltb.setLogDesc("Issued.");

		// translate servers by tags
		String ss = tr.getSourceServer();
		if (ss != null) {
			for (Server serv : BaseCache.getServers()) {
				if (ss.trim().equalsIgnoreCase(serv.getTag())) {
					for (LServer ls : serv.getServers()) {
						ltb.addSourceServers(ls);
					}
					break;
				}
			}
		}
		String ts = tr.getTargetServer();
		if (ts != null) {
			for (Server serv : BaseCache.getServers()) {
				if (ts.trim().equalsIgnoreCase(serv.getTag())) {
					for (LServer ls : serv.getServers()) {
						ltb.addTargetServers(ls);
					}
					break;
				}
			}
		}

		// extended properties
		HashMap<String, String> extProps = broker.getTaskExtProps(tr.getId());
		if ((extProps != null) && (extProps.size() > 0)) {
			Iterator<Entry<String, String>> iterExt = extProps.entrySet().iterator();
			Entry<String, String> ent = null;
			while (iterExt.hasNext()) {
				ent = iterExt.next();
				LKV.Builder epb = LKV.newBuilder();
				epb.setKey(ent.getKey());
				// epb.setValue(ent.getValue());
				epb.setValue(ent.getValue());
				ltb.addExtProps(epb.build());
			}
		}
		return ltb.build();
	}

	private void issueTask(LDBBroker dbBroker, Socket socket) throws SQLException, IOException {
		String brokerIP = socket.getInetAddress().getHostAddress();

		long startTime = System.currentTimeMillis();
		TaskRun taskToIssue = getTaskToIssue(dbBroker, brokerIP);

		if (taskToIssue == null) {
			MessageBroker.sendNull(socket, messageType, IssueProbe.NO_INSTANCE_FOUND);
			return;
		}

		LTask task = buildTask(dbBroker, taskToIssue);
		byte[] bytes = task.toByteArray();
		int updateState = LState.RUNNING_VALUE;
		if (taskToIssue.getState() == LState.SENTENCED_VALUE) {
			updateState = LState.KILLING_VALUE;
		}

		/* Mark the instance as initialized successfully without broker's response, which is not accurate. */
		dbBroker.updateTaskRunStart(task.getType(), task.getId(), task.getCurRunDate(), updateState, brokerIP,
				task.getTries(), task.getLogDesc());
		OutputStream output = socket.getOutputStream();
		MessageBroker messageBroker = new MessageBroker();
		messageBroker.sendMessage(output, messageType, bytes);
		output.close();
		TaskLogger.info(task, brokerIP);
		StringBuilder sb = new StringBuilder("Issued task of type ");
		sb.append(taskType);
		sb.append(":");
		sb.append(taskToIssue.getId());
		sb.append(",");
		sb.append(sdf.format(taskToIssue.getCurRunDate()));
		sb.append(",bytes:");
		sb.append(bytes.length);
		SysLogger.info(ISSUED_TAG, sb.toString(), brokerIP, System.currentTimeMillis() - startTime, taskType);

		// log maximum parallelism of a server
		Iterator<Entry<String, Integer>> iterServers = this.runningServers.entrySet().iterator();
		Entry<String, Integer> entServer = null;
		while (iterServers.hasNext()) {
			entServer = iterServers.next();
			String tag = entServer.getKey();
			Server serv = null;
			for (Server s : BaseCache.getServers()) {
				if (s.getTag().equalsIgnoreCase(tag)) {
					serv = s;
					break;
				}
			}

			if (serv == null) {
				continue;
			}
			Integer p = entServer.getValue();
			if ((p != null) && (p.intValue() >= serv.getParallelism())) {
				SysLogger.warn(this.getThreadName(), p.intValue() + " tasks of type " + this.taskType
						+ " are running on server [" + tag + "], no task can be issued any more!", brokerIP);
			}
		}

		// log maximum parallelism of a broker
		Iterator<Entry<String, Integer>> iterBrokers = this.runningBrokers.entrySet().iterator();
		Entry<String, Integer> entBroker = null;
		while (iterBrokers.hasNext()) {
			entBroker = iterBrokers.next();
			Integer p = entBroker.getValue();
			if ((p != null) && (p.intValue() >= this.brokerParallelism)) {
				SysLogger.warn(this.getThreadName(), p.intValue() + " tasks of type " + this.taskType
						+ " are running on broker [" + entBroker.getKey() + "], no task can be issued any more!",
						brokerIP);
			}
		}
	}

	@Override
	public void run() {
		LDBBroker dbBroker = null;

		while (true) {
			Socket socket = null;
			// handler one by one
			while (!requests.isEmpty()) {
				try {
					if ((dbBroker == null) || (dbBroker.isClosed())) {
						dbBroker = new LDBBroker();
					}
					// dbBroker.refreshConnection();
					// get first request
					// issue task, then remove from list
					socket = requests.poll();
					issueTask(dbBroker, socket);
				}
				catch (SQLException e) {
					SysLogger.error(this.getThreadName(), e);
				}
				catch (ClassNotFoundException e) {
					SysLogger.error(this.getThreadName(), e);
				}
				catch (IOException e) {
					SysLogger.error(this.getThreadName(), e);
				}
				catch (Exception e) {
					e.printStackTrace();
					SysLogger.error(this.getThreadName(), e);
				}
				finally {
					if ((socket != null) && (!socket.isClosed())) {
						try {
							socket.close();
						}
						catch (IOException e) {
							SysLogger.error(this.getThreadName(), e);
						}
					}
				}
			}
			// no request, close connection and wait some time
			if (dbBroker != null) {
				dbBroker.close();
			}

			try {
				Thread.sleep(SLEEP_TIME);
			}
			catch (InterruptedException e) {
			}
		}
	}

	public long getLastUpdate() {
		return lastUpdate;
	}

	/**
	 * sorter
	 * 
	 * @author: cpwang
	 * @date: 2013-1-17
	 */
	private class TaskIssuerSorter extends CommonThread {
		private LinkedList<TaskRun> taskRuns = new LinkedList<TaskRun>();
		private HashMap<String, Integer> runningTasks = new HashMap<String, Integer>();
		private HashSet<String> killingTasks = new HashSet<String>();
		private HashMap<String, java.sql.Timestamp> earliestTasks = new HashMap<String, java.sql.Timestamp>();
		private HashMap<String, ArrayList<TaskLink>> taskLinks = new HashMap<String, ArrayList<TaskLink>>();
		private LDBBroker broker;
		private boolean completed;
		private String brokerIP;

		public TaskIssuerSorter(int priority, LDBBroker dbb, String brokerIP) {
			super("TaskIssuerSorter-" + priority);
			this.broker = dbb;
			this.brokerIP = brokerIP;
		}

		public void getTaskToIssue() throws SQLException {
			Iterator<TaskRun> iter = taskRuns.iterator();
			TaskRun tr = null;
			while (iter.hasNext()) {
				tr = iter.next();
				int state = tr.getState();

				Task task = tr.getTask();

				if (state == LState.RUNNING_VALUE) {
					Integer tp = runningTasks.get(tr.getId());
					if ((tp != null) && (tp.intValue() < taskParallelism)) {
						// increase running count
						runningTasks.put(tr.getId(), tp.intValue() + 1);
					}
					else {
						runningTasks.put(tr.getId(), 1);
					}

					// store running tasks
					String ss = task.getSourceServer();
					if ((ss != null) && (!ss.equals(""))) {
						Integer ssp = runningServers.get(ss);
						if (ssp != null) {
							// increase running count
							runningServers.put(ss, ssp.intValue() + 1);
						}
						else {
							runningServers.put(ss, 1);
						}
					}

					String ts = task.getTargetServer();
					if ((ts != null) && (!ts.equals(""))) {
						Integer tsp = runningServers.get(ts);
						if (tsp != null) {
							// increase running count
							runningServers.put(ts, tsp.intValue() + 1);
						}
						else {
							runningServers.put(ts, 1);
						}
					}

					// store broker parallelism
					Integer bp = runningBrokers.get(tr.getRuntimeBroker());
					if ((bp != null) && (bp.intValue() < brokerParallelism)) {
						// increase running count
						runningBrokers.put(tr.getRuntimeBroker(), bp.intValue() + 1);
					}
					else {
						runningBrokers.put(tr.getRuntimeBroker(), 1);
					}
					// remove running instance
					iter.remove();
					continue;
				}
				else if (state == LState.KILLING_VALUE) {
					killingTasks.add(tr.getId());
					iter.remove();
					continue;
				}
				else {
					if (task.getSelfDepend() == 1) {
						java.sql.Timestamp mrd = earliestTasks.get(tr.getId());
						if ((mrd == null) || (tr.getCurRunDate().before(mrd))) {
							earliestTasks.put(tr.getId(), tr.getCurRunDate());
						}
						else {
							if (probe == null) {
								iter.remove();
								continue;
							}
							else {
								if ((probe.getTaskId().equals(tr.getId()))
										&& (probe.getRunDate().compareTo(tr.getCurRunDate()) == 0)) {
									probe.setFeedback(IssueProbe.NOT_EARLIEST_INSTANCE, sdf.format(mrd));
								}
							}
						}
					}

					boolean b = checkTask(task, tr, brokerIP);
					if (!b) {
						if (probe == null) {
							iter.remove();
							continue;
						}
						else {
							// probe task instance to diagnose
							if ((probe.getTaskId().equals(tr.getId()))
									&& (probe.getRunDate().compareTo(tr.getCurRunDate()) == 0)) {
								return;
							}
						}
					}
				}
			}

			TreeSet<TaskRun> sortedTaskRuns = new TreeSet<TaskRun>();
			iter = taskRuns.iterator();
			while (iter.hasNext()) {
				tr = iter.next();

				boolean b = checkTaskRun(tr);
				if (!b) {
					if (probe == null) {
						iter.remove();
						continue;
					}
					else {
						// probe task instance to diagnose
						if (probe.getTaskId().equals(tr.getId())
								&& (probe.getRunDate().compareTo(tr.getCurRunDate()) == 0)) {
							return;
						}
					}
				}
				// insert by order
				sortedTaskRuns.add(tr);
			}

			// sort by tries,priority,cycle,run_ate
			Iterator<TaskRun> sortedIter = sortedTaskRuns.iterator();
			while (sortedIter.hasNext()) {
				tr = sortedIter.next();

				boolean b = checkProviousTaskDone(tr.getId(), tr.getCurRunDate(), tr.getNextRunDate(),
						tr.getCycleUnit());
				if (b) {
					if (probe == null) {
						candidateRuns.add(tr);
						return;
					}
					else {
						if (probe.getTaskId().equals(tr.getId())
								&& (probe.getRunDate().compareTo(tr.getCurRunDate()) == 0)) {
							probe.setFeedback(IssueProbe.ALL_OK, "");
							return;
						}
					}
				}
				else {
					if ((probe != null) && probe.getTaskId().equals(tr.getId())
							&& (probe.getRunDate().compareTo(tr.getCurRunDate()) == 0)) {
						return;
					}
				}
			}
		}

		public boolean checkTask(Task task, TaskRun tr, String brokerIP) {
			if (!task.getStatus().equalsIgnoreCase(Status.NORMAL)) {
				// invalid status
				if (probe != null) {
					probe.setFeedback(IssueProbe.INVALID_TASK_STATUS, task.getStatus());
				}
				return false;
			}

			if ((!task.getBrokerIP().equalsIgnoreCase("any")) && (!task.getBrokerIP().equals(brokerIP))) {
				// broker not matched
				if (probe != null) {
					probe.setFeedback(IssueProbe.BROKER_NOT_MATCHED, task.getBrokerIP());
				}
				return false;
			}

			if ((tr.getRunPriority() < 0) || (tr.getRunPriority() > 9)) {
				// invalid priority
				if (probe != null) {
					probe.setFeedback(IssueProbe.ILLEGAL_PRIORITY, String.valueOf(tr.getRunPriority()));
				}
				return false;
			}

			if (runnerPriority < tr.getRunPriority()) {
				// task priority not matched this broker
				if (probe != null) {
					probe.setFeedback(IssueProbe.PRIORITY_NOT_MATCHED, String.valueOf(tr.getRunPriority()));
				}
				return false;
			}

			// denny comment on 201201228
			// if (tr.getNextRunDate().getTime() > sysTimestamp) {
			// throw new IssueProbe(IssueProbe.EXECUTE_TIME_NOT_ARRIVED, tr.getId(), tr.getNextRunDate(),
			// tr.getState());
			// }

			if (tr.getNextRunDate().getTime() + task.getDelayTime() * 60000 > sysTimestamp) {
				// delay time not arrived
				if (probe != null) {
					probe.setFeedback(IssueProbe.DELAY_TIME_NOT_ARRIVED, String.valueOf(task.getDelayTime()));
				}
				return false;
			}

			// truncate system time stamp to day
			/**
			 * the hour task is not supported by this rule
			 */
			if (sysDate + task.getStartupTime() * 60000 > sysTimestamp) {
				// start-up time not arrived
				if (probe != null) {
					probe.setFeedback(IssueProbe.STARTUP_TIME_NOT_ARRIVED, String.valueOf(task.getStartupTime()));
				}
				return false;
			}

			// set minimum run date of task
			// self-depend property not properly, remove
			if ((task.getSelfDepend() < 1) || (task.getSelfDepend() > 3)) {
				if (probe != null) {
					probe.setFeedback(IssueProbe.ILLEGAL_SELF_DEPEND, String.valueOf(task.getSelfDepend()));
				}
				return false;
			}

			int state = tr.getState();
			int tryLimit = tr.getTryLimit();
			if ((tryLimit >= 0) && (tryLimit <= tr.getTries())) {
				// not retriable
				if (probe != null) {
					probe.setFeedback(IssueProbe.TRY_OVER, String.valueOf(tryLimit));
				}
				return false;
			}

			// retried instance like FAILED,KILLED,HANGED
			if ((state == LState.FAILED_VALUE) || (state == LState.KILLED_VALUE) || (state == LState.HANGED_VALUE)) {
				if ((tr.getEndTime() != null) && (sysTimestamp - tr.getEndTime().getTime() < retryWaitMS)) {
					// not wait enough time to retry
					if (probe != null) {
						probe.setFeedback(IssueProbe.RETRY_WAIT_NOT_ARRIVE, String.valueOf(retryWaitMS / 60000));
					}
					return false;
				}
			}
			return true;
		}

		public boolean checkTaskRun(TaskRun tr) {
			String runtimeBroker = tr.getRuntimeBroker();
			if (runtimeBroker != null) {
				Integer brokerCount = runningBrokers.get(runtimeBroker);
				if ((brokerCount != null) && (brokerCount.intValue() >= brokerParallelism)) {
					// reach maximum parallelism of broker
					if (probe != null) {
						probe.setFeedback(IssueProbe.BROKER_FULL, tr.getRuntimeBroker());
					}
					return false;
				}
			}

			String ss = tr.getSourceServer();
			if ((ss != null) && (!ss.equals(""))) {
				checkServerParallelism(ss, tr.getRunPriority());
			}

			String ts = tr.getTargetServer();
			if ((ts != null) && (!ts.equals(""))) {
				checkServerParallelism(ts, tr.getRunPriority());
			}

			if (tr.getSelfDepend() == 3) {
				Integer instanceCount = runningTasks.get(tr.getId());
				if ((instanceCount != null) && (instanceCount.intValue() >= taskParallelism)) {
					// reach maximum parallelism of task
					if (probe != null) {
						probe.setFeedback(IssueProbe.TASK_FULL, String.valueOf(taskParallelism));
					}
					return false;
				}
			}
			else {
				// not parallel enabled,but has running instances,skip
				if ((runningTasks.containsKey(tr.getId())) || (killingTasks.contains(tr.getId()))) {
					if (probe != null) {
						probe.setFeedback(IssueProbe.HAS_RUNNING_INSTANCE, "");
					}
					return false;
				}

				if (tr.getSelfDepend() == 1) {
					java.sql.Timestamp earliestDate = earliestTasks.get(tr.getId());
					if ((earliestDate != null) && (tr.getCurRunDate().compareTo(earliestDate) != 0)) {
						// date serial(self_depend==1),but not the earliest
						// instance, not be handled
						if (probe != null) {
							probe.setFeedback(IssueProbe.NOT_EARLIEST_INSTANCE, sdf.format(earliestDate));
						}
						return false;
					}
				}
			}
			return true;
		}

		private boolean checkServerParallelism(String tag, int priority) {
			Integer serverCount = runningServers.get(tag);
			if (serverCount == null) {
				if (probe != null) {
					probe.setFeedback(IssueProbe.SERVER_NOT_AVAILABLE, tag);
				}
				return false;
			}

			Server server = null;
			for (Server s : BaseCache.getServers()) {
				if (s.getTag().equalsIgnoreCase(tag)) {
					server = s;
					break;
				}
			}
			if (server == null) {
				if (probe != null) {
					probe.setFeedback(IssueProbe.SERVER_NOT_AVAILABLE, tag);
				}
				return false;
			}

			int p = server.getParallelism();
			int specialPriority = server.getSpecialPriority();
			if (priority <= specialPriority) {
				// not special reserve
				p += (int) (p * (server.getSpecialReserve() / 100.0));
			}
			if (serverCount.intValue() >= p) {
				// reach maximum parallelism of source server
				if (probe != null) {
					probe.setFeedback(IssueProbe.SERVER_FULL, tag);
				}
				return false;
			}
			return true;
		}

		private boolean checkProviousTaskDone(String taskId, java.sql.Timestamp curRunDate,
				java.sql.Timestamp nextRunDate, String cycleUnit) throws SQLException {
			ArrayList<TaskLink> tls = taskLinks.get(taskId);
			if (tls == null) {
				tls = broker.getTaskLinks(taskId);
				taskLinks.put(taskId, tls);
			}

			for (TaskLink tl : tls) {
				// dependency is ignored
				if (!tl.getStatus().equalsIgnoreCase(Status.NORMAL)) {
					continue;
				}
				String fromId = tl.getTaskFrom();
				Task task = broker.getTask(fromId);
				if (task == null) {
					if (probe != null) {
						probe.setFeedback(IssueProbe.PARENT_TASK_NOT_FOUND, fromId);
					}
					return false;
				}

				int fromType = task.getType();
				if (task.getCycleUnit().equalsIgnoreCase(Task.ONEOFF_CYCLE)) {
					// one-off task, ignore time zone
					ArrayList<TaskRunSimple> trs = getTaskRuns(fromId, fromType);
					if (trs.size() <= 0) {
						if (probe != null) {
							probe.setFeedback(IssueProbe.PARENT_INSTANCE_NOT_FOUND, fromId);
						}
						return false;
					}
					for (TaskRunSimple tr : trs) {
						if (tr.getState() != LState.SUCCESSFUL_VALUE) {
							if (probe != null) {
								probe.setFeedback(IssueProbe.PARENT_NOT_SUCCESSFUL,
										fromId + "," + sdf.format(tr.getCurRunDate()) + "," + tr.getState());
							}
							return false;
						}
					}
				}
				else {
					int dt = tl.getDependenceType();

					if (dt == 1) {
						/* Check the parent instances' state in the same cycle unit with the son instance. */
						ArrayList<TaskRunSimple> trs = getTaskRuns(fromId, fromType);
						if (trs.size() <= 0) {
							if (probe != null) {
								probe.setFeedback(IssueProbe.PARENT_INSTANCE_NOT_FOUND, fromId);
							}
							return false;
						}

						java.sql.Timestamp maxNextDate = null, minCurDate = null;
						for (TaskRunSimple tr : trs) {
							if ((tr.getCurRunDate().before(curRunDate)) || (tr.getNextRunDate().after(nextRunDate))) {
								// just keep run date between
								continue;
							}

							if (tr.getState() != LState.SUCCESSFUL_VALUE) {
								if (probe != null) {
									probe.setFeedback(IssueProbe.PARENT_NOT_SUCCESSFUL,
											fromId + "," + sdf.format(tr.getCurRunDate()) + "," + tr.getState());
								}
								return false;
							}

							if ((maxNextDate == null) || (maxNextDate.before(tr.getNextRunDate()))) {
								maxNextDate = tr.getNextRunDate();
							}

							if ((minCurDate == null) || (minCurDate.after(tr.getCurRunDate()))) {
								minCurDate = tr.getCurRunDate();
							}
						}

						if (minCurDate == null) {
							if (probe != null) {
								probe.setFeedback(IssueProbe.PARENT_CURRENT_DATE_NOT_MATCHED, fromId);
							}
							return false;
						}
						if (minCurDate.compareTo(curRunDate) != 0) {
							// time range not matched
							if (probe != null) {
								probe.setFeedback(IssueProbe.PARENT_CURRENT_DATE_NOT_MATCHED,
										fromId + "," + sdf.format(minCurDate));
							}
							return false;
						}

						if (maxNextDate == null) {
							if (probe != null) {
								probe.setFeedback(IssueProbe.PARENT_NEXT_DATE_NOT_MATCHED, fromId);
							}
							return false;
						}
						if (maxNextDate.compareTo(nextRunDate) != 0) {
							if (probe != null) {
								probe.setFeedback(IssueProbe.PARENT_NEXT_DATE_NOT_MATCHED,
										fromId + "," + sdf.format(maxNextDate));
							}
							return false;
						}
					}
					else if (dt == 2) {
						/* Check the state of the parent's last instance before the son instance's next run date. */
						ArrayList<TaskRunSimple> trs = getTaskRuns(fromId, fromType);
						if (trs.size() <= 0) {
							if (probe != null) {
								probe.setFeedback(IssueProbe.PARENT_INSTANCE_NOT_FOUND, fromId);
							}
							return false;
						}

						TaskRunSimple lastRun = null;
						for (TaskRunSimple tr : trs) {
							if ((tr.getCurRunDate().before(curRunDate)) || (tr.getNextRunDate().after(nextRunDate))) {
								// just keep run date between
								continue;
							}

							if ((lastRun == null) || lastRun.getCurRunDate().before(tr.getCurRunDate())) {
								lastRun = tr;
							}
						}

						if (lastRun == null) {
							if (probe != null) {
								probe.setFeedback(IssueProbe.PARENT_INSTANCE_NOT_FOUND, fromId);
							}
							return false;
						}

						if (lastRun.getNextRunDate().compareTo(nextRunDate) != 0) {
							if (probe != null) {
								probe.setFeedback(IssueProbe.PARENT_NEXT_DATE_NOT_MATCHED,
										fromId + "," + sdf.format(lastRun.getNextRunDate()));
							}
							return false;
						}

						if (lastRun.getState() != LState.SUCCESSFUL_VALUE) {
							if (probe != null) {
								probe.setFeedback(IssueProbe.PARENT_NOT_SUCCESSFUL,
										fromId + "," + sdf.format(lastRun.getCurRunDate()) + "," + lastRun.getState());
							}
							return false;
						}
					}
					else if (dt == 3) {
						/* Check whether any one of the parent's instance succeeds in the son's instance cycle unit. */
						ArrayList<TaskRunSimple> trs = getTaskRuns(fromId, fromType);
						if (trs.size() <= 0) {
							if (probe != null) {
								probe.setFeedback(IssueProbe.PARENT_INSTANCE_NOT_FOUND, fromId);
							}
							return false;
						}

						boolean bSucc = false;
						for (TaskRunSimple tr : trs) {
							if ((tr.getCurRunDate().before(curRunDate)) || (tr.getNextRunDate().after(nextRunDate))) {
								// just keep run date between
								continue;
							}

							if (tr.getState() == LState.SUCCESSFUL_VALUE) {
								bSucc = true;
								break;
							}
						}
						if (bSucc == false) {
							if (probe != null) {
								probe.setFeedback(IssueProbe.PARENT_NOT_SUCCESSFUL, fromId);
							}
							return false;
						}
					}
					else if (dt < 0) {
						/* Check the state of the parent's instance 1 - dt cycle units before. */
						int offset = -dt + 1;
						ArrayList<TaskRunSimple> trs = getTaskRuns(fromId, fromType);
						int size = trs.size();
						if (size <= 0) {
							if (probe != null) {
								probe.setFeedback(IssueProbe.PARENT_INSTANCE_NOT_FOUND, fromId);
							}
							return false;
						}

						if (size < offset) {
							if (probe != null) {
								probe.setFeedback(IssueProbe.INSTANCE_NOT_ENOUGH, fromId + ",count:" + size);
							}
							return false;
						}

						TreeSet<TaskRunSimple> sortedRuns = new TreeSet<TaskRunSimple>();
						for (TaskRunSimple tr : trs) {
							if (tr.getNextRunDate().after(nextRunDate)) {
								// just keep run date between
								continue;
							}
							sortedRuns.add(tr);
						}

						if (sortedRuns.size() < offset) {
							if (probe != null) {
								probe.setFeedback(IssueProbe.INSTANCE_NOT_ENOUGH,
										fromId + ",count:" + sortedRuns.size());
							}
							return false;
						}

						int idx = 0;
						Iterator<TaskRunSimple> descIter = sortedRuns.descendingIterator();
						TaskRunSimple tr = null;
						while (descIter.hasNext()) {
							tr = descIter.next();
							if (++idx >= offset) {
								break;
							}
						}

						if (tr.getState() != LState.SUCCESSFUL_VALUE) {
							// earliest instance not successful
							if (probe != null) {
								probe.setFeedback(IssueProbe.PARENT_NOT_SUCCESSFUL,
										fromId + "," + sdf.format(tr.getCurRunDate()) + "," + tr.getState());
							}
							return false;
						}
					}
					else {
						if (probe != null) {
							probe.setFeedback(IssueProbe.INVALID_DEPEND_TYPE, fromId + ",type:" + dt);
						}
						return false;
					}
				}
			}
			return true;
		}

		private ArrayList<TaskRunSimple> getTaskRuns(String taskId, int taskType) throws SQLException {
			// look for instances from cache firstly
			ArrayList<TaskRunSimple> runs = checkedTaskRunsCache.get(taskId);
			if (runs == null) {
				// not found, query database
				runs = broker.getTaskRuns(taskId, taskType);
				checkedTaskRunsCache.put(taskId, runs);
			}
			return runs;
		}

		public void addTaskRun(TaskRun tr) {
			this.taskRuns.add(tr);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			completed = false;
			try {
				getTaskToIssue();
			}
			catch (SQLException e) {
				SysLogger.error(this.getThreadName(), e);
			}
			catch (Exception e) {
				e.printStackTrace();
				SysLogger.error(this.getThreadName(), e);
			}
			completed = true;
		}

		public boolean isCompleted() {
			return completed;
		}
	}
}
