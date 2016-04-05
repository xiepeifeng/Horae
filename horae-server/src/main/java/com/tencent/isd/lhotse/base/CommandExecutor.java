package com.tencent.isd.lhotse.base;

import com.tencent.isd.lhotse.base.logger.SysLogger;
import com.tencent.isd.lhotse.base.socket.IssueProbe;
import com.tencent.isd.lhotse.base.socket.TaskIssuer;
import com.tencent.isd.lhotse.base.socket.TaskServer;
import com.tencent.isd.lhotse.dao.*;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import org.jdom2.JDOMException;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

/**
 * @author cpwang
 * @since 2012-3-23
 */
public class CommandExecutor extends CommonThread {

	public static void main(String[] args) throws SQLException, ClassNotFoundException, ParseException, IOException {
		// LDBBroker.setConf("10.129.135.179", "3362", "lhotse_test", "lhotse_test", "test1#");
		LDBBroker broker = new LDBBroker();
		// BaseCache.initServers(broker);
		CommandExecutor executor = new CommandExecutor();
		Command cmd = new Command(1);
		cmd.setName("do");
		cmd.setTaskId("3");
		cmd.setParam1("2");
		// cmd.setParam1("3");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		cmd.setDateFrom(new java.sql.Timestamp(sdf.parse("20121215").getTime()));
		cmd.setDateTo(new java.sql.Timestamp(sdf.parse("20121228").getTime()));
		executor.doDo(broker, cmd);
		System.out.println(cmd.getComment());
	}

	private final String CMD_START = "START";
	private final String CMD_STOP = "STOP";
	private final String CMD_REDO = "REDO";
	private final String CMD_DO = "DO";
	private final String CMD_KILL = "KILL";
	private final String CMD_LET = "LET";
	private final String CMD_REG = "REG";
	private final String CMD_PRIOR = "PRIOR";
	private final String CMD_DIAG = "DIAG";

	private final int COMMAND_RESERVED_DAYS = 7;
	private final int COMMAND_CHECK_INTERVAL = 2;

	private Command filterCommand(LDBBroker broker, boolean bFirst) throws SQLException {
		LinkedList<Command> cmds = broker.getCommands();
		Iterator<Command> iter = cmds.iterator();
		Command cmd = null;
		final java.sql.Timestamp sysTimestamp = broker.getSysTimestamp();
		while (iter.hasNext()) {
			cmd = iter.next();
			int state = cmd.getState();

			if ((cmd.getSetTime() == null)
					|| (sysTimestamp.getTime() - cmd.getSetTime().getTime() > COMMAND_RESERVED_DAYS * 86400000)) {
				iter.remove();
				broker.deleteCommand(cmd.getId());
				continue;
			}

			if (state == LState.RUNNING_VALUE) {
				if (bFirst) {
					iter.remove();
					failCommand(broker, cmd, "Reset by system.", sysTimestamp);
					continue;
				}

				java.sql.Timestamp startTime = cmd.getStartTime();
				// execution is over 15 minutes
				if ((startTime == null) || (sysTimestamp.getTime() - startTime.getTime() > 900000)) {
					iter.remove();
					failCommand(broker, cmd, "Executing overtime!", sysTimestamp);
					continue;
				}
				else {
					// there is running command, do not execute another any more
					return null;
				}
			}

			if (cmd.getState() != LState.READY_VALUE) {
				// just keep READY commands
				iter.remove();
				continue;
			}

			if ((cmd.getName() == null) || (cmd.getName().equals(""))) {
				iter.remove();
				failCommand(broker, cmd, "Invalid command name:" + cmd.getName(), sysTimestamp);
				continue;
			}

			// setter is invalid, clear
			if ((cmd.getSetter() == null) || (cmd.getSetter().equals(""))) {
				iter.remove();
				failCommand(broker, cmd, "Invalid setter!", sysTimestamp);
				continue;
			}

		}

		if (cmds.size() > 0) {
			Collections.sort(cmds);
			Command cmdToExecute = cmds.element();
			cmdToExecute.setStartTime(sysTimestamp);
			cmdToExecute.setState(LState.RUNNING_VALUE);
			broker.updateCommand(cmdToExecute);
			StringBuffer sb = new StringBuffer();
			sb.append("Execute command:").append(cmdToExecute.getName()).append(",");
			sb.append(cmdToExecute.getTaskId()).append(",");
			sb.append("date_from:").append(cmdToExecute.getDateFrom()).append(",");
			sb.append("date_to:").append(cmdToExecute.getDateTo()).append(",");
			sb.append(cmdToExecute.getSetter());

			SysLogger.info(this.getThreadName(), sb.toString(), cmdToExecute.getSetter());
			// SysLogger.info(this.getThreadName(), "Execute command:" + cmdToExecute.getName() + "," +
			// cmdToExecute.getTaskId()
			// + "," + cmdToExecute.getSetter(), cmdToExecute.getSetter());
			return cmdToExecute;
		}
		return null;
	}

	private void failCommand(LDBBroker broker, Command cmd, String desc, java.sql.Timestamp sysTimestamp)
			throws SQLException {
		cmd.setComment("Executing overtime!");
		cmd.setState(LState.FAILED_VALUE);
		cmd.setEndTime(sysTimestamp);
		broker.updateCommand(cmd);
	}

	@Override
	public void run() {
		LDBBroker broker = null;
		// execute commands
		boolean bFirst = true;
		while (true) {
			boolean bClosed = false;
			Command ce = null;
			boolean bExecuted = false;
			try {
				broker = new LDBBroker();
				ce = filterCommand(broker, bFirst);
				bFirst = false;

				if (ce != null) {
					bExecuted = true;
					// execute
					String cmd = ce.getName().trim().toUpperCase();

					if (cmd.equals(CMD_START)) {
						int r = broker.updateTaskStatus(ce.getTaskId(), Status.FREEZED, Status.NORMAL);
						ce.setState(LState.SUCCESSFUL_VALUE);
						ce.setComment(r + " task started.");
					}
					else if (cmd.equals(CMD_STOP)) {
						int r = broker.updateTaskStatus(ce.getTaskId(), Status.NORMAL, Status.FREEZED);
						ce.setState(LState.SUCCESSFUL_VALUE);
						ce.setComment(r + " task stopped.");
					}
					else if (cmd.equals(CMD_REDO)) {
						doRedo(broker, ce);
					}
					else if (cmd.equals(CMD_DO)) {
						doDo(broker, ce);
					}
					else if (cmd.equals(CMD_LET)) {
						doLet(broker, ce);
					}
					else if (cmd.equals(CMD_REG)) {
						doRegister(broker, ce);
					}
					else if (cmd.equals(CMD_PRIOR)) {
						// cpwang,20120723
						doPrior(broker, ce);
					}
					else if (cmd.equals(CMD_KILL)) {
						doKill(broker, ce);
					}
					else if (cmd.equals(CMD_DIAG)) {
						doDiagnose(broker, ce);
					}
					else {
						ce.setComment("Illegal command!");
					}
				}
			}
			catch (SQLException e) {
				SysLogger.error(this.getThreadName(), e);
			}
			catch (ClassNotFoundException e) {
				SysLogger.error(this.getThreadName(), e);
			}
			catch (Exception e) {
				e.printStackTrace();
				SysLogger.error(this.getThreadName(), e);
			}
			finally {
				if (broker != null) {
					broker.close();
				}
			}

			if (bExecuted) {
				// end of executing command, update state
				try {
					if (ce.getState() != LState.SUCCESSFUL_VALUE) {
						ce.setState(LState.FAILED_VALUE);
					}
					broker = new LDBBroker();
					java.sql.Timestamp sysTimestamp = broker.getSysTimestamp();
					ce.setEndTime(sysTimestamp);
					broker.updateCommand(ce);
					SysLogger.info(this.getThreadName(),
							"执行完成:" + ce.getName() + "," + ce.getTaskId() + "," + ce.getSetter(), ce.getSetter());
				}
				catch (SQLException e) {
					SysLogger.error(this.getThreadName(), e);
				}
				catch (Exception e) {
					e.printStackTrace();
					SysLogger.error(this.getThreadName(), e);
				}
				finally {
					if (broker != null) {
						broker.close();
					}
				}
			}

			// if closed, quit directly
			if (bClosed) {
				break;
			}

			try {
				// sleep some time
				Thread.sleep(COMMAND_CHECK_INTERVAL * 1000);
			}
			catch (InterruptedException e) {
			}
		}
	}

	private boolean validateDate(Command cmd) {
		if (cmd.getDateFrom() == null) {
			cmd.setComment("无效的开始时间");
			return false;
		}

		if (cmd.getDateTo() == null) {
			cmd.setComment("无效的结束时间");
			return false;
		}

		if (cmd.getDateFrom().compareTo(cmd.getDateTo()) > 0) {
			cmd.setComment("结束时间必须晚于开始时间");
			return false;
		}
		return true;
	}

	/**
	 * param=1, just redo current instance.<br>
	 * param=2(default), redo current instance and sub-instance.<br>
	 * param=3, just redo sub-instance.
	 * 
	 * @param broker
	 * @param ce
	 * @throws SQLException
	 *             cpwang,20120723,support redoing multiple tasks once
	 */
	private void doRedo(LDBBroker broker, Command ce) throws SQLException {
		if (!validateDate(ce)) {
			return;
		}

		// String strIdList = ce.getTaskId();
		String param = "2";
		if ((ce.getParam1() != null) && (!ce.getParam1().equals(""))) {
			param = ce.getParam1().trim();
		}

		String taskId = ce.getTaskId();
		HashMap<String, Integer> allTasks = new HashMap<String, Integer>();
		Task t = broker.getTask(taskId);
		if (t == null) {
			ce.setComment("任务未找到");
			ce.setState(LState.FAILED_VALUE);
			return;
		}
		int taskType = t.getType();

		if ((param.equals("1")) || (param.equals("2"))) {
			// redo self
			allTasks.put(taskId, taskType);
		}
		if (param.equals("2") || param.equals("3")) {
			broker.getTaskOffsprings(allTasks, taskId);
		}

		ArrayList<TaskRunSimple> redoRunsAll = broker.getTaskRuns(taskId, taskType);
		ArrayList<TaskRunSimple> redoRuns = new ArrayList<TaskRunSimple>();
		for (TaskRunSimple r : redoRunsAll) {
			if ((r.getCurRunDate().compareTo(ce.getDateFrom()) >= 0)
					&& (r.getCurRunDate().compareTo(ce.getDateTo()) <= 0)) {
				// between set time
				redoRuns.add(r);
			}
		}

		if (redoRuns.size() <= 0) {
			ce.setComment("实例未找到");
			return;
		}

		// stop all offsprings
		Set<String> taskIds = allTasks.keySet();
		Iterator<String> iterTaskIds = taskIds.iterator();
		while (iterTaskIds.hasNext()) {
			broker.updateTaskStatus(iterTaskIds.next(), Status.NORMAL, Status.SHOCK);
		}

		ce.setComment(taskIds.size() + " 个任务已经停止");
		// ce.setComment(taskIds.size() + " task(s) stopped.");
		broker.updateCommand(ce);

		ArrayList<TaskRunSimple> offspringRuns = new ArrayList<TaskRunSimple>();
		TaskRunSimple runningTask = null;
		offspringRuns.clear();
		runningTask = setOffspringRuns(broker, offspringRuns, redoRuns, allTasks);
		if (runningTask != null) {
			ce.setComment("该任务或有后续任务正在运行,无法停止:" + runningTask.getId());
			broker.updateCommand(ce);
			return;
		}

		// set redo flag and state
		int cnt = 0;
		for (TaskRunSimple tr : offspringRuns) {
			// set redo_flag=1, state=0, tries=0
			int i = broker.updateTaskRun(LState.READY_VALUE, 1, 0, false, null, tr.getId(), tr.getCurRunDate(),
					tr.getCurRunDate(), false);
			cnt += i;
		}
		for (String id : taskIds) {
			// start tasks
			broker.updateTaskStatus(id, Status.SHOCK, Status.NORMAL);
		}
		ce.setComment(taskId + "及其后续共" + taskIds.size() + "个任务的" + cnt + "个实例已设置为重做");
		ce.setState(LState.SUCCESSFUL_VALUE);
	}

	private TaskRunSimple setOffspringRuns(LDBBroker broker, ArrayList<TaskRunSimple> offspringTaskRuns,
			ArrayList<TaskRunSimple> redoRuns, HashMap<String, Integer> allTasks) throws SQLException {
		Iterator<Entry<String, Integer>> iterTasks = allTasks.entrySet().iterator();
		Entry<String, Integer> ent = null;
		while (iterTasks.hasNext()) {
			ent = iterTasks.next();
			ArrayList<TaskRunSimple> runs = broker.getTaskRuns(ent.getKey(), ent.getValue());
			for (TaskRunSimple child : runs) {
				for (TaskRunSimple parent : redoRuns) {
					// child date window must cover parent date window
					if (child.getCurRunDate().after(parent.getCurRunDate())
							|| child.getNextRunDate().before(parent.getNextRunDate())) {
						continue;
					}

					if (child.getState() == LState.RUNNING_VALUE) {
						return child;
					}
					offspringTaskRuns.add(child);
				}
			}
		}
		return null;
	}

	private void doDo(LDBBroker broker, Command ce) throws SQLException {
		if (!validateDate(ce)) {
			return;
		}

		Task task = broker.getTask(ce.getTaskId());
		if (task == null) {
			ce.setComment("任务未找到!");
			return;
		}
		String cycle = task.getCycleUnit().toUpperCase();

		if (cycle.equals("O")) {
			ce.setComment("不支持一次性任务!");
			return;
		}

		int state = LState.READY_VALUE;
		if ((ce.getParam1() != null) && (!ce.getParam1().equals(""))) {
			try {
				state = Integer.parseInt(ce.getParam1().trim());
				if ((state < LState.READY_VALUE) || (state > LState.HANGED_VALUE)) {
					ce.setComment("非法状态:" + ce.getParam1());
					return;
				}
			}
			catch (NumberFormatException e) {
				ce.setComment(e.getMessage());
				return;
			}
		}

		java.util.Date curRunDate = ce.getDateFrom();
		ArrayList<TaskRunSimple> taskRuns = broker.getTaskRuns(task.getId(), task.getType());
		java.sql.Timestamp earlistDate = null;
		for (TaskRunSimple tr : taskRuns) {
			if ((earlistDate == null) || (earlistDate.after(tr.getCurRunDate()))) {
				earlistDate = tr.getCurRunDate();
			}
		}

		java.util.Date floorDate = null;
		if (earlistDate != null) {
			floorDate = earlistDate.before(ce.getDateTo()) ? earlistDate : ce.getDateTo();
		}
		else {
			floorDate = ce.getDateTo();
		}

		if (curRunDate.compareTo(floorDate) >= 0) {
			ce.setComment("开始时间之后无实例需要补录:" + curRunDate);
			return;
		}

		java.sql.Timestamp sysTimestamp = broker.getSysTimestamp();
		ArrayList<TaskRun> taskToAdd = new ArrayList<TaskRun>();

		TaskInitializer intializer = new TaskInitializer();
		TaskRun tr = intializer.createTaskRun(task, curRunDate, state, sysTimestamp);
		if (tr == null) {
			ce.setComment("延迟时间未达到:" + task.getDelayTime());
			return;
		}
		do {
			taskToAdd.add(tr);
			tr = intializer.getNextInstance(task, curRunDate, state, sysTimestamp);
			if (tr != null) {
				curRunDate = tr.getCurRunDate();
			}
			else {
				break;
			}
		}
		while (curRunDate.compareTo(floorDate) < 0);

		int cnt = broker.insertTaskRuns(taskToAdd);
		ce.setState(LState.SUCCESSFUL_VALUE);
		ce.setComment("已补录" + cnt + "个实例，任务ID：" + task.getId());
	}

	public void doLet(LDBBroker broker, Command ce) throws SQLException, ParseException {
		if (!validateDate(ce)) {
			return;
		}
		int cnt = broker.updateTaskRun(LState.SUCCESSFUL_VALUE, -1, -1, true, "强制成功", ce.getTaskId(),
				new java.sql.Timestamp(ce.getDateFrom().getTime()), new java.sql.Timestamp(ce.getDateTo().getTime()),
				false);
		ce.setState(LState.SUCCESSFUL_VALUE);
		ce.setComment(cnt + "个实例已经强制成功");
		// ce.setComment(cnt + " instance(s) of task " + ce.getTaskId() + " set for success.");
	}

	public void doRegister(LDBBroker broker, Command ce) throws SQLException, ClassNotFoundException, JDOMException,
			IOException {
		String param1 = ce.getParam1();
		if ((param1 == null) || (param1.equals(""))) {
			ce.setComment("Invalid type[param1] to register!");
			return;
		}
		param1 = param1.trim();
		if (param1.equalsIgnoreCase("s")) {
			// initialize all connections
			int[] cnts = BaseCache.initServers(broker);
			ce.setState(LState.SUCCESSFUL_VALUE);
			ce.setComment(cnts[0] + " server(s) and " + cnts[1] + " group(s) registered.");
			return;
		}
		else if (param1.equalsIgnoreCase("r")) {
			int r = BaseCache.initRunners(broker);
			ce.setState(LState.SUCCESSFUL_VALUE);
			ce.setComment(r + " runner(s) registered.");
		}
		else if (param1.equalsIgnoreCase("t")) {
			int t = BaseCache.initTaskTypes(broker);
			TaskServer.refreshIssuers(broker);
			ce.setState(LState.SUCCESSFUL_VALUE);
			ce.setComment(t + " task type(s) registered and relative issure(s) refreshed.");
		}
	}

	/**
	 * drag some tasks and all ancestors
	 * 
	 * @param broker
	 * @param ce
	 */
	public void doDrag(LDBBroker broker, Command ce) {

	}

	private void doKill(LDBBroker broker, Command ce) throws SQLException {
		if (!validateDate(ce)) {
			return;
		}
		int cnt = broker.updateTaskRun(LState.SENTENCED_VALUE, -1, -1, false, null, ce.getTaskId(),
				new java.sql.Timestamp(ce.getDateFrom().getTime()), new java.sql.Timestamp(ce.getDateTo().getTime()),
				true);
		ce.setState(LState.SUCCESSFUL_VALUE);
		ce.setComment(ce.getTaskId() + "的" + cnt + "个实例将被强制终止");
	}

	/**
	 * @since 2012-07-23,cpwang
	 * @param broker
	 * @param ce
	 * @throws SQLException
	 */
	private void doPrior(LDBBroker broker, Command ce) throws SQLException {
		ArrayList<String> taskIds = new ArrayList<String>();
		taskIds.add(ce.getTaskId());
		String param1 = ce.getParam1();
		int priority = -1;
		try {
			priority = Integer.parseInt(param1);
		}
		catch (NumberFormatException e) {
			ce.setState(LState.FAILED_VALUE);
			ce.setComment(e.getMessage());
			return;
		}
		if ((priority < 0) || (priority > 10)) {
			ce.setState(LState.FAILED_VALUE);
			ce.setComment("Value of task priority must be between 0 and 10.");
			return;
		}
		broker.getTaskAncestors(taskIds, ce.getTaskId());
		broker.updateTasksPriority(taskIds, priority);
		ce.setState(LState.SUCCESSFUL_VALUE);
		ce.setComment(taskIds.size() + " tasks' priority updated to " + priority);
	}

	public void doDiagnose(LDBBroker broker, Command ce) throws SQLException {
		if (!validateDate(ce)) {
			return;
		}

		Task task = broker.getTask(ce.getTaskId());
		if (task == null) {
			ce.setState(LState.SUCCESSFUL_VALUE);
			ce.setComment("任务未找到");
			return;
		}

		ArrayList<TaskRunSimple> trs = broker.getTaskRuns(task.getId(), task.getType());
		TaskRunSimple taskRun = null;
		for (TaskRunSimple r : trs) {
			if (r.getCurRunDate().getTime() == ce.getDateFrom().getTime()) {
				taskRun = r;
				break;
			}
		}

		if (taskRun == null) {
			ce.setState(LState.SUCCESSFUL_VALUE);
			ce.setComment("实例未找到");
			return;
		}

		int state = taskRun.getState();
		if ((state != LState.READY_VALUE) && (state != LState.FAILED_VALUE) && (state != LState.SENTENCED_VALUE)) {
			ce.setState(LState.SUCCESSFUL_VALUE);
			ce.setComment("实例已经运行或者已完成");
			return;
		}

		try {
			BaseCache.init(broker);
		}
		catch (IOException e1) {
			e1.printStackTrace();
		}

		String clientIP = task.getBrokerIP();
		if (clientIP.equalsIgnoreCase("any")) {
			// find a valid broker
			for (Runner r : BaseCache.getRunners()) {
				if ((r.getTaskType() == task.getType()) && (r.getStatus().equalsIgnoreCase(Status.NORMAL))) {
					clientIP = r.getBrokerIP();
					break;
				}
			}
		}

		IssueProbe probe = new IssueProbe(taskRun.getId(), taskRun.getCurRunDate());
		TaskIssuer ti = new TaskIssuer((byte) task.getType(), probe);
		ti.getTaskToIssue(broker, clientIP);
		ce.setComment(probe.toString());

		ce.setState(LState.SUCCESSFUL_VALUE);
	}
}
