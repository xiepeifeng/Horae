package com.tencent.isd.lhotse.base.socket;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.sql.SQLException;

import com.tencent.isd.lhotse.base.CommonThread;
import com.tencent.isd.lhotse.base.LDBBroker;
import com.tencent.isd.lhotse.base.logger.SysLogger;

/**
 * @author: cpwang
 * @date: 2012-5-23
 */
public abstract class AbstractSocketHandler extends CommonThread {
	protected Socket socket;
	protected String clientIP;
	protected OutputStream output;
	protected LDBBroker dbBroker;
	protected int taskType;

	public AbstractSocketHandler(Socket socket, int type) {
		this.socket = socket;
		this.clientIP = socket.getInetAddress().getHostAddress();
		this.taskType = type;
	}

	/**
	 * handle message
	 */
	protected abstract void handle() throws IOException, SQLException;

	@Override
	public void run() {
		try {
			dbBroker = new LDBBroker();
			output = socket.getOutputStream();
			handle();
			output.close();
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
