package com.tencent.isd.lhotse.base.alert;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author cpwang
 * @since 2011-5-10
 */
public class TOFAlerter extends AbstractAlerter {
	private static final String TOF_SYS_KEY = "2d9c8b7cb3de421b9647004693993a3f";
	// private static final int MAIL_MESSAGE = 1;
	private static final int RTX_MESSAGE = 2;
	private static final int SMS_MESSAGE = 3;
	// private static final int NOC_MESSAGE = 4;

	private static final String TOF_WS_URL_OA = "http://ws.tof.oa.com/MessageService.svc";// SHENZHEN
	// private static final String TOF_WS_URL_OSS =
	// "http://ws.oss.com/MessageService.svc";//separated district

	private static final String SOAP_XML_HEAD = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <SOAP-ENV:Envelope "
			+ "SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\" "
			+ "xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\" "
			+ "xmlns:xsi=\"http://www.w3.org/1999/XMLSchema-instance\" "
			+ "xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" "
			+ "xmlns:xsd=\"http://www.w3.org/1999/XMLSchema\"> <SOAP-ENV:Header> "
			+ "<Application_Context xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\"> "
			+ "<AppKey xmlns=\"http://schemas.datacontract.org/2004/07/Tencent.OA.Framework.Context\">" + TOF_SYS_KEY
			+ "</AppKey> </Application_Context> </SOAP-ENV:Header> ";

	private static final String RTX_SOAP_XML = "<SOAP-ENV:Body> <n2:SendRTX xmlns:n2=\"http://tempuri.org/\"> "
			+ "<n2:message xmlns:n3="
			+ "\"http://schemas.datacontract.org/2004/07/Tencent.OA.Framework.Messages.DataContract\"> "
			+ "<n3:MsgInfo>%MsgInfo%</n3:MsgInfo> <n3:Priority>%Priority%</n3:Priority> "
			+ "<n3:Receiver>%Receiver%</n3:Receiver> <n3:Sender>%Sender%</n3:Sender> "
			+ "<n3:Title>%Title%</n3:Title> </n2:message> </n2:SendRTX> </SOAP-ENV:Body> </SOAP-ENV:Envelope>";

	private static final String SMS_SOAP_XML = "<SOAP-ENV:Body> <n2:SendSMS xmlns:n2=\"http://tempuri.org/\"> "
			+ "<n2:message xmlns:n3="
			+ "\"http://schemas.datacontract.org/2004/07/Tencent.OA.Framework.Messages.DataContract\"> "
			+ "<n3:MsgInfo>%MsgInfo%</n3:MsgInfo> <n3:Priority>%Priority%</n3:Priority> "
			+ "<n3:Receiver>%Receiver%</n3:Receiver> <n3:Sender>%Sender%</n3:Sender> </n2:message> "
			+ "</n2:SendSMS> </SOAP-ENV:Body> </SOAP-ENV:Envelope>";

	/**
	 * 
	 * @param soapRtxXml
	 * @param type
	 * @return
	 * @throws IOException
	 */
	private String sendSOAPToWebService(String soapRtxXml, int type) throws IOException {
		if (soapRtxXml == null) {
			return "Message is null";
		}
		String sendType = "";
		if (type == RTX_MESSAGE) {
			sendType = "SendRTX";
		}
		else if (type == SMS_MESSAGE) {
			sendType = "SendSMS";
		}

		String ws = TOF_WS_URL_OA;
		// if (!isOA) {
		// ws = TOF_WS_URL_OSS;
		// }
		URL url = new URL(ws);
		HttpURLConnection httpUrlConn = (HttpURLConnection) url.openConnection();
		httpUrlConn.setRequestMethod("POST");
		httpUrlConn.setDoInput(true);
		httpUrlConn.setDoOutput(true);
		httpUrlConn.setRequestProperty("Content-Type", "text/xml; charset=utf-8");
		httpUrlConn.setRequestProperty("Content-Length", Integer.toString(soapRtxXml.length()));
		httpUrlConn.setRequestProperty("SOAPAction", "\"http://tempuri.org/IMessageService/" + sendType + "\"");

		OutputStream os = httpUrlConn.getOutputStream();
		OutputStreamWriter osw = new OutputStreamWriter(os, "utf-8");
		osw.write(soapRtxXml);
		osw.flush();
		osw.close();
		String msg = httpUrlConn.getResponseMessage();
		httpUrlConn.disconnect();
		return msg;
	}

	/**
	 * @param Sender
	 *            :900(system),RTX,CellPhoneNumber
	 * @param Receiver
	 *            ;RTX,CellPhoneNumber
	 * @param Message
	 * @param Priority
	 *            :Low/Normal/High
	 * @throws IOException
	 */
	public void sendSMS(String sender, String receiver, String title, String msgInfo, String priority)
			throws IOException {
		String soapxml = SOAP_XML_HEAD
				+ SMS_SOAP_XML.replace("%Sender%", sender).replace("%Receiver%", receiver)
						.replace("%MsgInfo%", msgInfo).replace("%Priority%", priority);
		sendSOAPToWebService(soapxml, SMS_MESSAGE);
	}

	/**
	 * send SYSTEM SMS
	 * 
	 * @param receiver
	 * @param title
	 * @param msgInfo
	 * @param priority
	 * @return
	 * @throws IOException
	 */
	public void sendSMS(String receiver, String srcIP, String title, String msgInfo) throws IOException {
		sendSMS("900", receiver, title, msgInfo, "Normal");
	}

	/**
	 * send SYSTEM RTX
	 * 
	 * @param receiver
	 * @param title
	 * @param msgInfo
	 * @param priority
	 * @return
	 * @throws IOException
	 */
	public void sendRTX(String receiver, String srcIP, String title, String msgInfo) throws IOException {
		sendRTX("900", receiver, title, msgInfo, "Normal");
	}

	/**
	 * @param Sender
	 *            :900(system),RTX,CellPhoneNumber
	 * @param Receiver
	 *            (delimited by ;);RTX,CellPhoneNumber
	 * @param Message
	 * @param Priority
	 *            :Low/Normal/High
	 * @throws IOException
	 */
	private void sendRTX(String sender, String receiver, String title, String msgInfo, String priority)
			throws IOException {
		String soapxml = SOAP_XML_HEAD
				+ RTX_SOAP_XML.replace("%Sender%", sender).replace("%Receiver%", receiver).replace("%Title%", title)
						.replace("%MsgInfo%", msgInfo).replace("%Priority%", priority);

		// not separated district, SHENZHEN
		sendSOAPToWebService(soapxml, RTX_MESSAGE);
	}

	@Override
	public void sendMail(String receiver, String srcIP, String title, String msgInfo) {
		// TODO Auto-generated method stub

	}

	@Override
	public void sendNOC(String receiver, String srcIP, String title, String msgInfo) {
		// TODO Auto-generated method stub

	}

}
