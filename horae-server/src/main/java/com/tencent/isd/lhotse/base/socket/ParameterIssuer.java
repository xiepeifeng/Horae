package com.tencent.isd.lhotse.base.socket;

import java.io.IOException;
import java.net.Socket;
import java.sql.SQLException;

import com.tencent.isd.lhotse.base.BaseCache;
import com.tencent.isd.lhotse.base.logger.SysLogger;
import com.tencent.isd.lhotse.message.MessageBroker;
import com.tencent.isd.lhotse.proto.LhotseObject.LParameters;
import com.tencent.isd.lhotse.proto.LhotseObject.MessageType;

/**
 * @author: cpwang
 * @date: 2012-7-19
 */
public class ParameterIssuer extends AbstractSocketHandler {

	/**
	 * @param socket
	 * @param data
	 */
	public ParameterIssuer(Socket socket, int type) {
		super(socket, type);
	}

	@Override
	protected void handle() throws IOException, SQLException {
		long startTime = System.currentTimeMillis();
		SysLogger.info("ParameterIssuer", "Request parameters of type " + taskType, this.clientIP);

		LParameters lp = BaseCache.getTypeParameters(taskType);
		byte[] bytes = null;
		if (lp != null) {
			bytes = lp.toByteArray();
		}
		else {
			bytes = new byte[0];
		}
		MessageBroker messageBroker = new MessageBroker();
		messageBroker.sendMessage(output, (byte) MessageType.START_RUNNER_VALUE, bytes);
		long endTime = System.currentTimeMillis();
		SysLogger.info("ParameterIssuer", "Parameters of type " + taskType + " responded,bytes:" + bytes.length,
				this.clientIP, endTime - startTime, taskType);
	}
}
