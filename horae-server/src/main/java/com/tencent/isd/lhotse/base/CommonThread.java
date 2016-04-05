package com.tencent.isd.lhotse.base;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author: cpwang
 * @date: 2013-1-6
 */
public abstract class CommonThread implements Runnable {
    private String threadName;
    private Thread thread;


    public CommonThread() {
        this.threadName = this.getClass().getSimpleName();
    }


    public CommonThread(String name) {
        this.threadName = name;
    }


    public void start() {
        if (thread == null) {
            thread = new Thread(this, threadName);
            thread.start();
        }
    }


    public void stop() {
        if (thread != null) {
            thread.interrupt();
            thread = null;
        }
    }


    public String getThreadName() {
        return threadName;
    }
}
