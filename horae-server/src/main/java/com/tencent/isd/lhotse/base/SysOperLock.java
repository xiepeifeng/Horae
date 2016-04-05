package com.tencent.isd.lhotse.base;

/**
 * @author: cpwang
 * @date: 2012-9-11
 */
public class SysOperLock {
	public static final byte NULL_LOCK = 0;
	public static final byte INITIALIZING_LOCK = 1;
	public static final byte MAINTAINING_LOCK = 2;
	private static volatile byte operLock = NULL_LOCK;

	public static synchronized void setLock(byte lock) {
		operLock = lock;
	}

	public static synchronized byte getLock() {
		return operLock;
	}
}
