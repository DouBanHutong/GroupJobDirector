package com.invooker.groupjob;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Deque;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.List;
/**
 * @author YanFeng
 * @date 2021/7/14
 */
public class GroupJobDirector<T> {

    private volatile boolean running;
    private volatile long lastTime;
    private long totalWaitingLimit;
    private int queueLimit;
    private long timeInterval;
    private Consumer<List<Pair<T,Long>>> jobExecutor;
    private Consumer<String> logConsumer;

    private volatile AtomicLong jobListLength;

    private Deque<Pair<T,Long>> jobList;

    private ScheduledExecutorService scheduledLoopThreadPool;

    private ExecutorService jobExecutorPool;


    public GroupJobDirector(){}


    public boolean push(T jobUnit){
        if(null != jobUnit){
            if(jobListLength.incrementAndGet() <= totalWaitingLimit){
                jobList.addLast(Pair.of(jobUnit,System.currentTimeMillis()));
            }else {
                jobListLength.decrementAndGet();
            }
        }
        return false;
    }


    private void run(){
        if(null != scheduledLoopThreadPool){
            scheduledLoopThreadPool.scheduleWithFixedDelay(() -> {
                long needExecuteSize = getNeedExecuteSize();
                if(needExecuteSize > 0){
                    List<Pair<T,Long>> currentToBeExecutedJobs = queueLimit > 1 ? new ArrayList<>(queueLimit) : new ArrayList<>(1);
                    while (needExecuteSize > 0){
                        Pair<T,Long> currentPair = jobList.pollFirst();
                        if(null != currentPair){
                            jobListLength.decrementAndGet();
                            currentToBeExecutedJobs.add(currentPair);
                            needExecuteSize--;
                        }else {
                            break;
                        }
                    }

                    logConsumer.accept("Prepare to execute new jobs:" + currentToBeExecutedJobs.size());
                    if(null != jobExecutor){
                        try {
                            jobExecutorPool.submit(() -> {
                                logConsumer.accept("Executing job");
                                try {
                                    jobExecutor.accept(currentToBeExecutedJobs);
                                }catch (Exception ignore){}
                            });
                            lastTime = System.currentTimeMillis();
                        }catch (Exception e){
                            logConsumer.accept("Execute jobs Exception Catch =>" + e);
                        }
                    }
                }
            },1000,100, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 判定当前需要执行job的数量
     * 当且仅当  当前存在排队任务 且 (当前排队任务数大于 队列长度  或  距离上次执行时间间隔超过了设定阈值)   需要执行
     * @return job数量
     */
    private long getNeedExecuteSize(){
        if(jobListLength.get() > 0){
            if((lastTime + timeInterval) <= System.currentTimeMillis() || jobListLength.get() >= queueLimit){
                return Math.min(jobListLength.get(),queueLimit);
            }
        }
        return 0L;
    }

    public GroupJobDirector<T> Build(Long jobExecuteTimeInterval, Consumer<List<Pair<T,Long>>> jobExecutor, Consumer<String> logConsumer, Integer queueLimit, Long totalWaitingLimit, Integer concurrency){

        this.timeInterval = (null != jobExecuteTimeInterval && jobExecuteTimeInterval > 0) ? jobExecuteTimeInterval : 0L;
        this.queueLimit = (null != queueLimit && queueLimit > 0) ? queueLimit : 1;
        this.totalWaitingLimit = (null != totalWaitingLimit && totalWaitingLimit > 0) ? totalWaitingLimit : Long.MAX_VALUE;

        this.jobExecutor = null != jobExecutor ? jobExecutor : job -> {};
        this.logConsumer = null != logConsumer ? logConsumer : log -> {};

        this.jobList = new ConcurrentLinkedDeque();
        this.jobListLength = new AtomicLong(0L);
        this.scheduledLoopThreadPool = Executors.newSingleThreadScheduledExecutor();
        int trueConcurrency = (null != concurrency && concurrency > 0) ? concurrency : 1;
        this.jobExecutorPool = Executors.newFixedThreadPool(trueConcurrency);
        this.run();
        this.running = true;
        return this;
    }

}
