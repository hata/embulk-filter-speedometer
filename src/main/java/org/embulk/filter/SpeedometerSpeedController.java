package org.embulk.filter;

import org.embulk.filter.SpeedometerFilterPlugin.PluginTask;


class SpeedometerSpeedController {
    private final SpeedometerSpeedAggregator aggregator;

    private final long limitBytesPerSec;
    private final int maxSleepMillisec;
    private final int logIntervalMillisec;

    private long startTime;
    private volatile long periodStartTime;
    private volatile long periodTotalBytes;
    private volatile long threadTotalBytes;
    private volatile long periodTotalRecords;
    private volatile long threadTotalRecords;
    private volatile boolean renewFlag = true;

    SpeedometerSpeedController(PluginTask task, SpeedometerSpeedAggregator aggregator) {
        this.limitBytesPerSec = task.getSpeedLimit();
        this.maxSleepMillisec = task.getMaxSleepMillisec();
        this.logIntervalMillisec = task.getLogIntervalSeconds() * 1000;
        this.aggregator = aggregator;
    }

    public void stop() {
        startNewPeriod(0);
        aggregator.stopController(this);
    }

    public long getSpeedLimit() {
        return limitBytesPerSec;
    }

    public int getMaxSleepMillisec() {
        return maxSleepMillisec;
    }

    public int getLogIntervalMillisec() {
        return logIntervalMillisec;
    }

    public long getTotalBytes() {
        return threadTotalBytes + periodTotalBytes;
    }
    
    public long getTotalRecords() {
        return threadTotalRecords + periodTotalRecords;
    }

    public long getPeriodBytesPerSec(long nowTime) {
        return (periodTotalBytes * 1000) / getTimeDeltaMillisec(nowTime);
    }

    public long getPeriodRecordsPerSec(long nowTime) {
        return (periodTotalRecords * 1000) / getTimeDeltaMillisec(nowTime);
    }

    public void checkSpeedLimit(long nowTime, long newDataSize) {
        checkSpeedLimit(nowTime, newDataSize, false);
    }

    public void checkSpeedLimit(long nowTime, long newDataSize, boolean endRecord) {
        if (startTime == 0) {
            startTime = nowTime;
            aggregator.startController(this, startTime);
        }

        if (renewFlag) {
            renewFlag = false;
            startNewPeriod(nowTime);
        }

        periodTotalBytes += newDataSize;
        if (endRecord) {
            periodTotalRecords++;
        }
        aggregator.checkProgress(nowTime, logIntervalMillisec);

        if (limitBytesPerSec <= 0) {
            return;
        }

        long speedLimitForThread = aggregator.getSpeedLimitForController(this);
        long timeDeltaMillisec = nowTime > periodStartTime ? nowTime - periodStartTime : 1;
        long bytesPerSec = (periodTotalBytes * 1000) / timeDeltaMillisec;
        long overBytes = bytesPerSec - speedLimitForThread;

        if (overBytes > 0) {
            try {
                long sleepTime = (periodTotalBytes * 1000) / speedLimitForThread - timeDeltaMillisec;
                sleepTime = sleepTime > maxSleepMillisec ? maxSleepMillisec : sleepTime > 0 ? sleepTime : 0;
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                // TODO: Do I need to throw an exception ?
            }
        }
    }

    void renewPeriod() {
        renewFlag = true;
    }

    boolean isRenewPeriodSet() {
        return renewFlag;
    }

    SpeedometerSpeedAggregator getAggregator() {
        return aggregator;
    }

    private void startNewPeriod(long newPeriodTime) {
        threadTotalBytes += periodTotalBytes;
        periodTotalBytes = 0;
        threadTotalRecords += periodTotalRecords;
        periodTotalRecords = 0;
        periodStartTime = newPeriodTime;
    }

    private long getTimeDeltaMillisec(long nowTime) {
        long timeDeltaMillisec = nowTime - periodStartTime;
        return timeDeltaMillisec <= 0 ? 1 : timeDeltaMillisec;
    }
}

