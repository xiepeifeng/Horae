package com.tencent.isd.lhotse.base.alert;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.tencent.isd.lhotse.base.logger.SysLogger;

/**
 * @author: cpwang
 * @date: 2012-7-27
 */
public class BOSSAlerter extends AbstractAlerter {
	Log logger = LogFactory.getLog(BOSSAlerter.class);
	private static final String SYS_LOG_SRC = "BossAlerter";
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	// private static final String BOSS_SHENZHEN_IP = "119.147.21.172";
	// private static final int BOSS_SHENZHEN_PORT = 60158;
	private static final String BOSS_GUANGZHOU_IP = "10.142.25.95";
	private static final int BOSS_GUANGZHOU_PORT = 60159;
	private static final String BOSS_SHANTOU_IP = "10.141.18.42";
	private static final int BOSS_SHANTOU_PORT = 60158;

	// public static final int SHENZHEN = 1;
	public static final int GUANGZHOU = 2;
	public static final int SHANTOU = 3;

	private String servIP;
	private int servPort;

	public BOSSAlerter(int district) {
		if (district == GUANGZHOU) {
			servIP = BOSS_GUANGZHOU_IP;
			servPort = BOSS_GUANGZHOU_PORT;
		}
		else if (district == SHANTOU) {
			servIP = BOSS_SHANTOU_IP;
			servPort = BOSS_SHANTOU_PORT;
		}
	}

	@Override
	public void sendMail(String receiver, String srcIP, String title, String msgInfo) {
		sendMsgByBoss("MAIL#" + receiver, srcIP, msgInfo);
	}

	@Override
	public void sendRTX(String receiver, String srcIP, String title, String msgInfo) {
	}

	@Override
	public void sendSMS(String receiver, String srcIP, String title, String msgInfo) {
		sendMsgByBoss(receiver, srcIP, msgInfo);
	}

	@Override
	public void sendNOC(String receiver, String srcIP, String title, String msgInfo) {
		sendMsgByBoss("NOC#" + receiver, srcIP, msgInfo);
	}

	private void sendMsgByBoss(String receiver, String ip, String msgInfo) {
		String strNow = sdf.format(new Date());
		Socket socket = new Socket();
		try {
			InetSocketAddress isa = new InetSocketAddress(servIP, servPort);
			socket.setSoTimeout(5);
			socket.connect(isa);
			OutputStream os = socket.getOutputStream();
			StringBuilder sb = new StringBuilder();
			sb.append(strNow);
			sb.append("&");
			sb.append(receiver);
			sb.append("&");
			sb.append(ip);
			sb.append("&");
			sb.append(msgInfo);
			sb.append("\r\n");
			os.write(sb.toString().getBytes("UTF-8"));
			os.flush();
			os.close();

			/*
			 * InputStream is = socket.getInputStream(); BufferedReader br = new BufferedReader(new
			 * InputStreamReader(is)); String line = br.readLine(); if (line != null) { String[] strs = line.split("&");
			 * if (strs.length > 0) { if (strs[0].equals("0")) { System.out.println("Send successfully!"); } } }
			 * br.close();
			 */
		}
		catch (UnknownHostException e) {
			SysLogger.error(SYS_LOG_SRC, e);
		}
		catch (IOException e) {
			SysLogger.error(SYS_LOG_SRC, e);
		}
		finally {
			try {
				socket.close();
			}
			catch (IOException e) {
				SysLogger.error(SYS_LOG_SRC, e);
			}
		}
	}
}
