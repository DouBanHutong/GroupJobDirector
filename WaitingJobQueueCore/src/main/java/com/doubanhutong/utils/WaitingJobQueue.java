package com.doubanhutong.utils;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * @author YanFeng
 * @date 2021/7/13
 */
public class WaitingJobQueue {

    private final Logger logger = LoggerFactory.getLogger(DefaultLogClient.class);
    private volatile boolean running = true;
    private volatile long lastTime;
    private LogSender sender;
    private LogClientConfig config;
    private Deque<LogBean> logData = new ConcurrentLinkedDeque();
    private Deque<LogBean> reservedLogData = new ConcurrentLinkedDeque();

}
