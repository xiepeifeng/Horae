package com.tencent.isd.lhotse.base.socket;

import java.io.IOException;
import java.net.Socket;
import java.sql.SQLException;

import com.tencent.isd.lhotse.base.BaseCache;
import com.tencent.isd.lhotse.base.logger.SysLogger;
import com.tencent.isd.lhotse.dao.Runner;
import com.tencent.isd.lhotse.dao.TaskType;
import com.tencent.isd.lhotse.message.MessageBroker;
import com.tencent.isd.lhotse.proto.LhotseObject.MessageType;

/**
 * @author: cpwang
 * @date: 2012-5-23
 */
public class RunnerRecoverer extends AbstractSocketHandler {

	/**
	 * @param socket
	 * @param data
	 */
	public RunnerRecoverer(Socket socket, int type) {
		super(socket, type);
	}

	@Override
	protected void handle() throws IOException, SQLException {
		MessageBroker messageBroker = new MessageBroker();
		// cpwang,20120808,update startup time runner
		String log = "Runner of type " + taskType + " started up from " + clientIP + ".";
		TaskType tt = BaseCache.getTaskType(taskType);
		byte retVal = 0;
		if (tt == null) {
			log += "Type is not registered and can not be started up!";
		}
		else {
			Runner runner = BaseCache.getRunner(taskType, clientIP);
			if (runner == null) {
				log += "Runner is not registered and can not be started up!";
			}
			else {
				dbBroker.updateRunner(runner, true);
				int k = dbBroker.updateTasksRunByBrokerAndType(clientIP, taskType);
				retVal = 1;
				log += "reset " + k + " instance(s).";
			}
		}

		messageBroker.sendMessage(output, (byte) MessageType.START_RUNNER_VALUE, new byte[] { retVal });
		SysLogger.info("RunnerRecoverer", log, clientIP, taskType);
	}
}
