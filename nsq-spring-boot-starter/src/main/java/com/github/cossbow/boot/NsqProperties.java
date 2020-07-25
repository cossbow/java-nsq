package com.github.cossbow.boot;

import org.springframework.boot.context.properties.ConfigurationProperties;


@ConfigurationProperties(prefix = "mq.nsq")
public class NsqProperties {

    public static final String QUALIFIER_PUB_SNAPPY = "Snappy";
    public static final String QUALIFIER_PUB_DEFLATE = "Deflate";


    //
    //
    //


    private String lookupAddress = "127.0.0.1:4161";

    private long lookupPeriodMillis = 60_000;

    private int defaultAttemptLimit = 8;

    private int defaultAttemptDelay = 60_000;

    private int schedulerPoolSize = 10;

    private long defaultBatchInterval = 100;

    private String userAgent;

    private long monitorLookupNodesPeriod = 15;  // minutes

    private long monitorQueryStatsPeriod = 1;   // minutes


    public String getLookupAddress() {
        return lookupAddress;
    }

    public void setLookupAddress(String lookupAddress) {
        this.lookupAddress = lookupAddress;
    }

    public long getLookupPeriodMillis() {
        return lookupPeriodMillis;
    }

    public void setLookupPeriodMillis(long lookupPeriodMillis) {
        this.lookupPeriodMillis = lookupPeriodMillis;
    }

    public int getDefaultAttemptLimit() {
        return defaultAttemptLimit;
    }

    public void setDefaultAttemptLimit(int defaultAttemptLimit) {
        this.defaultAttemptLimit = defaultAttemptLimit;
    }

    public int getDefaultAttemptDelay() {
        return defaultAttemptDelay;
    }

    public void setDefaultAttemptDelay(int defaultAttemptDelay) {
        this.defaultAttemptDelay = defaultAttemptDelay;
    }

    public int getSchedulerPoolSize() {
        return schedulerPoolSize;
    }

    public void setSchedulerPoolSize(int schedulerPoolSize) {
        this.schedulerPoolSize = schedulerPoolSize;
    }

    public long getDefaultBatchInterval() {
        return defaultBatchInterval;
    }

    public void setDefaultBatchInterval(long defaultBatchInterval) {
        this.defaultBatchInterval = defaultBatchInterval;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public long getMonitorLookupNodesPeriod() {
        return monitorLookupNodesPeriod;
    }

    public void setMonitorLookupNodesPeriod(long monitorLookupNodesPeriod) {
        this.monitorLookupNodesPeriod = monitorLookupNodesPeriod;
    }

    public long getMonitorQueryStatsPeriod() {
        return monitorQueryStatsPeriod;
    }

    public void setMonitorQueryStatsPeriod(long monitorQueryStatsPeriod) {
        this.monitorQueryStatsPeriod = monitorQueryStatsPeriod;
    }
}
