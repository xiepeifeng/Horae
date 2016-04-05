package com.tencent.isd.lhotse.message;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * @author: cpwang
 * @date: 2012-5-8
 */
public class MessageBroker {
	private static final int TIME_OUT = 30000;
	private static final int SO_TIME_OUT = 300000;//read 10 minute mostly

	private byte[] int2Bytes(int i) {
		byte[] bs = new byte[4];
		bs[0] = (byte) (i >> 24);
		bs[1] = (byte) (i >> 16);
		bs[2] = (byte) (i >> 8);
		bs[3] = (byte) i;
		return bs;
	}

	public int bytes2Int(byte[] bs) {
		return (bs[0] & 0xFF) << 24 | (bs[1] & 0xFF) << 8 | (bs[2] & 0xFF) << 8 | bs[3] & 0xFF;
	}

	private int bytes2Int(byte b0, byte b1, byte b2, byte b3) {
		return (b0 & 0xFF) << 24 | (b1 & 0xFF) << 8 | (b2 & 0xFF) << 8 | b3 & 0xFF;
	}

	/**
	 * receive and decode message
	 * 
	 * @param is
	 * @return
	 * @throws IOException
	 */
	public byte[] receiveMessage(InputStream is) throws IOException {
		byte[] bHead = new byte[5];
		int r = is.read(bHead);
		if (r < 5) {
			return null;
		}

		int len = bytes2Int(bHead[1], bHead[2], bHead[3], bHead[4]);
		byte[] contents = new byte[len + 1];
		//is.read(contents, 1, len);
		contents[0] = bHead[0];
		int offset = 1;
		int left = len;
		do {
			int k = is.read(contents, offset, left);
			if (k == -1) {
				break;
			}
			offset += k;
			left -= k;
		}
		while (offset < len);
		return contents;
	}

	/**
	 * encode and send message
	 * 
	 * @param os
	 * @param type
	 * @param contents
	 * @throws IOException
	 */
	public void sendMessage(OutputStream os, byte type, byte[] contents) throws IOException {
		int len = contents.length;
		byte[] bAll = new byte[5 + len];
		bAll[0] = type;
		byte[] bLen = int2Bytes(len);
		System.arraycopy(bLen, 0, bAll, 1, 4);
		System.arraycopy(contents, 0, bAll, 5, len);
		os.write(bAll);
		os.flush();
	}

	/**
	 * set message and parse response
	 * 
	 * @param sendType
	 * @param sendData
	 * @return
	 * @throws IOException
	 */
	public byte[] sendAndReceive(String server, int port, byte sendType, byte[] sendData) throws IOException {
		// InetAddress ia = InetAddress.getByName(this.serverIP);
		InetSocketAddress isa = new InetSocketAddress(server, port);
		Socket socket = new Socket();
		socket.setTcpNoDelay(true);// avoid packet delay
		socket.setSoTimeout(SO_TIME_OUT);//0:infinite timeout
		socket.connect(isa, TIME_OUT);
		OutputStream output = socket.getOutputStream();
		sendMessage(output, sendType, sendData);
		// parse received message
		InputStream input = socket.getInputStream();
		byte[] bytes = receiveMessage(input);
		// close
		//output.close();
		//input.close();
		socket.close();
		if ((bytes == null) || (bytes.length < 1)) {
			return null;
		}

		int len = bytes.length;
		// byte[0] is message type
		byte[] receiveData = new byte[len - 1];
		System.arraycopy(bytes, 1, receiveData, 0, len - 1);
		return receiveData;
	}

	public byte[] sendAndReceive(String server, int port, byte sendType, int sendData) throws IOException {
		byte[] bs = int2Bytes(sendData);
		return sendAndReceive(server, port, sendType, bs);
	}

	public byte[] sendAndReceive(String server, int port, byte sendType, String sendData) throws IOException {
		byte[] bs = sendData.getBytes("UTF-8");
		return sendAndReceive(server, port, sendType, bs);
	}

	public static synchronized void sendNull(Socket socket, byte messageType, byte code) throws IOException {
		MessageBroker messageBroker = new MessageBroker();
		byte[] bytes = new byte[] { code };
		messageBroker.sendMessage(socket.getOutputStream(), messageType, bytes);
	}
}
