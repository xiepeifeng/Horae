package com.tencent.isd.lhotse.base.socket;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tencent.isd.lhotse.base.CommonThread;
import com.tencent.isd.lhotse.base.LDBBroker;
import com.tencent.isd.lhotse.base.logger.SysLogger;
import com.tencent.isd.lhotse.base.logger.TaskLogger;
import com.tencent.isd.lhotse.message.MessageBroker;
import com.tencent.isd.lhotse.proto.LhotseObject.LState;
import com.tencent.isd.lhotse.proto.LhotseObject.LTask;
import com.tencent.isd.lhotse.proto.LhotseObject.MessageType;

/**
 * @author: cpwang
 * @date: 2012-5-23
 */
public class TaskCommitter extends CommonThread {

	private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	protected Socket socket;
	protected String clientIP;
	protected byte[] messageData;

	/**
	 * @param socket
	 * @param data
	 */
	public TaskCommitter(Socket socket, byte[] data) {
		this.socket = socket;
		this.clientIP = socket.getInetAddress().getHostAddress();
		this.messageData = data;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		long startTime = System.currentTimeMillis();
		LTask t = null;
		try {
			t = LTask.parseFrom(this.messageData);
		}
		catch (InvalidProtocolBufferException e) {
			SysLogger.error(this.getThreadName(), e);
			return;
		}
		String curRunDateStr = sdf.format(new Date(t.getCurRunDate()));
		SysLogger.info("TaskCommitter", "Commit task " + t.getId() + "," + curRunDateStr, this.clientIP);

		LState state = t.getState();
		byte retVal = 1;

		LDBBroker dbBroker = null;
		try {
			dbBroker = new LDBBroker();
			// log successful result
			if ((state == LState.RUNNING) || (state == LState.KILLING)) {
				dbBroker.updateTaskRunRunning(t.getType(), t.getId(), t.getCurRunDate(), t.getState().getNumber(),
						t.getRuntimeId(), t.getLogDesc());
				TaskLogger.info(t, clientIP);
			}
			else if ((state == LState.SUCCESSFUL) || (state == LState.KILLED)) {
				dbBroker.updateTaskRunEnd(t.getType(), t.getId(), t.getCurRunDate(), t.getState().getNumber(),
						t.getRuntimeId(), t.getLogDesc());
				TaskLogger.info(t, clientIP);
			}
			else if ((state == LState.FAILED) || (state == LState.HANGED)) {
				dbBroker.updateTaskRunEnd(t.getType(), t.getId(), t.getCurRunDate(), t.getState().getNumber(),
						t.getRuntimeId(), t.getLogDesc());
				TaskLogger.error(t, clientIP);
			}
			else {
				retVal = 0;
				TaskLogger.error(t, "Illegal state:" + state.getNumber());
			}
			MessageBroker messageBroker = new MessageBroker();
			OutputStream output = socket.getOutputStream();
			messageBroker.sendMessage(output, (byte) MessageType.COMMIT_TASK_VALUE, new byte[] { retVal });
			long endTime = System.currentTimeMillis();
			SysLogger.info("TaskCommitter", "Task recorded:" + t.getId() + "," + sdf.format(t.getCurRunDate()),
					this.clientIP, endTime - startTime, t.getType());
		}
		catch (SQLException e) {
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
			if (dbBroker != null) {
				dbBroker.close();
			}

			try {
				if (socket != null) {
					socket.close();
				}
			}
			catch (IOException e) {
				SysLogger.error(this.getThreadName(), e);
			}
		}
	}
}
