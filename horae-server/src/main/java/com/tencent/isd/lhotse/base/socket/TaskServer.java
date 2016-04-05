package com.tencent.isd.lhotse.base.socket;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import com.tencent.isd.lhotse.base.CommonThread;
import com.tencent.isd.lhotse.base.LDBBroker;
import com.tencent.isd.lhotse.base.LhotseBase;
import com.tencent.isd.lhotse.base.logger.SysLogger;
import com.tencent.isd.lhotse.message.MessageBroker;
import com.tencent.isd.lhotse.proto.LhotseObject.MessageType;

/**
 * @since 2011-11-1
 * @author cpwang
 * 
 */
public class TaskServer extends CommonThread {
	private final int LISTENER_SLEEP_INTERVAL = 1;
	private final int MAX_CONNECTION_NUM = 5120;
	private static HashMap<Integer, TaskIssuer> issuers = new HashMap<Integer, TaskIssuer>();
	// maximum alive time of current issuer thread, dispose when time is out
	private static final long MAX_ALIVE_TIME = 7200000;// 2 hour

	@Override
	public void run() {
		ServerSocket serverSocket = null;

		try {
			serverSocket = new ServerSocket(LhotseBase.TASK_SERVER_PORT, MAX_CONNECTION_NUM);
			SysLogger.info(this.getThreadName(), "Server started and listening port [" + LhotseBase.TASK_SERVER_PORT
					+ "]", LhotseBase.getBaseIP());
		}
		catch (IOException e) {
			SysLogger.error(this.getThreadName(), e);
			System.exit(0);
		}
		catch (Exception e) {
			e.printStackTrace();
			SysLogger.error(this.getThreadName(), e);
			System.exit(0);
		}

		final byte b_REQUEST_TASK_VALUE = (byte) MessageType.REQUEST_TASK_VALUE;
		final byte b_COMMIT_TASK_VALUE = (byte) MessageType.COMMIT_TASK_VALUE;
		final byte b_START_RUNNER_VALUE = (byte) MessageType.START_RUNNER_VALUE;
		final byte b_REQUEST_PARAMETER = (byte) MessageType.REQUEST_PARAMETER_VALUE;

		while (true) {
			try {
				Socket socket = serverSocket.accept();
				if (socket != null) {
					InputStream input = socket.getInputStream();
					MessageBroker messageBroker = new MessageBroker();
					byte[] bytes = messageBroker.receiveMessage(input);
					if ((bytes == null) || (bytes.length < 2)) {
						SysLogger.error(this.getThreadName(), "Illeagal message!", socket.getInetAddress()
								.getHostAddress());
						socket.close();
						continue;
					}

					int len = bytes.length;
					byte messageType = bytes[0];

					byte[] messageData = new byte[len - 1];
					System.arraycopy(bytes, 1, messageData, 0, len - 1);
					if (messageType == b_COMMIT_TASK_VALUE) {
						// commit task
						TaskCommitter committer = new TaskCommitter(socket, messageData);
						committer.start();
					}
					else {
						int taskType = messageBroker.bytes2Int(messageData);
						AbstractSocketHandler handler = null;
						if (messageType == b_REQUEST_TASK_VALUE) {
							// issue task
							// find an exist issuer
							TaskIssuer ti = issuers.get(taskType);
							if (ti == null) {
								ti = new TaskIssuer(taskType);
								ti.start();
								issuers.put(taskType, ti);
							}
							ti.addRequest(socket);
						}
						else if (messageType == b_REQUEST_PARAMETER) {
							// response parameter
							handler = new ParameterIssuer(socket, taskType);
							handler.start();
						}
						else if (messageType == b_START_RUNNER_VALUE) {
							// start runner, record startup time and recover hanged
							// tasks
							handler = new RunnerRecoverer(socket, taskType);
							handler.start();
						}
						else {
							String clientIP = socket.getInetAddress().getHostAddress();
							SysLogger.info(this.getThreadName(), "Illeagal message type:" + messageType + ",length:"
									+ len, clientIP);
							// close current connection
							MessageBroker.sendNull(socket, messageType, (byte) 0);
							socket.close();
						}
					}
				}
			}
			catch (IOException e) {
				SysLogger.error(this.getThreadName(), e);
			}
			catch (Exception e) {
				e.printStackTrace();
				SysLogger.error(this.getThreadName(), e);
			}

			try {
				Thread.sleep(LISTENER_SLEEP_INTERVAL);
			}
			catch (InterruptedException e) {
			}
		}

	}

	public static synchronized void refreshIssuers(LDBBroker broker) throws SQLException {
		long curTime = System.currentTimeMillis();
		Iterator<Entry<Integer, TaskIssuer>> iter = issuers.entrySet().iterator();
		Entry<Integer, TaskIssuer> ent = null;
		while (iter.hasNext()) {
			ent = iter.next();
			TaskIssuer ti = ent.getValue();
			// not exists
			/*
			 * if (ti == null) { ti = new TaskIssuer(typeId); ti.start(); issuers[typeId] = ti; ++cnt; } else {
			 */
			// long time not to receive requests
			if (ti.getLastUpdate() < curTime - MAX_ALIVE_TIME) {
				ti.stop();
				iter.remove();
			}
			// }
		}
	}
}
