package com.tencent.isd.lhotse.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

/**
 * @author: cpwang
 * @date: 2012-5-21
 */
public class LHostUtil {
	public synchronized static String getHostIP() throws UnknownHostException, SocketException {
		String os = System.getProperty("os.name").toLowerCase();
		if (os.indexOf("nix") >= 0 || os.indexOf("nux") >= 0) {
			// linux or unix
			Enumeration<NetworkInterface> nis = NetworkInterface.getNetworkInterfaces();
			while (nis.hasMoreElements()) {
				NetworkInterface ni = nis.nextElement();
				Enumeration<InetAddress> ias = ni.getInetAddresses();
				while (ias.hasMoreElements()) {
					InetAddress ia = ias.nextElement();
					if ((!ia.isSiteLocalAddress()) || ia.isLoopbackAddress() || ia.getHostAddress().indexOf(":") >= 0) {
						continue;
					}
					else {
						return ia.getHostAddress();
					}
				}
			}
		}
		else {
			// windows
			return InetAddress.getLocalHost().getHostAddress();
		}
		return null;
	}
}
