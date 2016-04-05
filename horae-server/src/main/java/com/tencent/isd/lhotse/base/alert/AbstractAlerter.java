package com.tencent.isd.lhotse.base.alert;

import java.io.IOException;

/**
 * @author: cpwang
 * @date: 2012-7-27
 */
public abstract class AbstractAlerter {
	public abstract void sendMail(String receiver, String srcIP, String title, String msgInfo) throws IOException;

	public abstract void sendRTX(String receiver, String srcIP, String title, String msgInfo) throws IOException;

	public abstract void sendSMS(String receiver, String srcIP, String title, String msgInfo) throws IOException;

	public abstract void sendNOC(String receiver, String srcIP, String title, String msgInfo) throws IOException;
}
