package org.embulk.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.embulk.spi.Exec;
import org.slf4j.Logger;

class SpeedometerSpeedAggregator {
    private static class SpeedometerSpeedAggregatorHolder {
        private static final SpeedometerSpeedAggregator INSTANCE = new SpeedometerSpeedAggregator();
    }

    private final long INITAL_START_TIME = 0;

    private final AtomicInteger activeControllerCount = new AtomicInteger(0);
    private final AtomicLong globalStartTime = new AtomicLong(INITAL_START_TIME);
    private final AtomicLong globalTotalBytes = new AtomicLong(0);
    private final AtomicLong globalTotalRecords = new AtomicLong(0);
    private final AtomicLong previousLogReportTimeMillisec = new AtomicLong(INITAL_START_TIME);

    // TODO: We can use google's library.
    private final List<SpeedometerSpeedController> controllerList = new ArrayList<>();

    public static SpeedometerSpeedAggregator getInstance() {
        return SpeedometerSpeedAggregatorHolder.INSTANCE;
    }

    public SpeedometerSpeedAggregator() {
        showLogMessage(activeControllerCount.get(), 0, 0, 0, 0, 0);
    }

    public void startController(SpeedometerSpeedController controller, long nowTime) {
        globalStartTime.compareAndSet(INITAL_START_TIME, nowTime);
        activeControllerCount.incrementAndGet();
        synchronized (controllerList) {
            controllerList.add(controller);
        }
    }

    public void stopController(SpeedometerSpeedController controller) {
        synchronized (controllerList) {
            controllerList.remove(controller);
        }
        long runningCount = activeControllerCount.decrementAndGet();
        globalTotalBytes.addAndGet(controller.getTotalBytes());
        globalTotalRecords.addAndGet(controller.getTotalRecords());

        // NOTE: Sometimes, there is no running thread nevertheless there are remaining tasks.
        // So, this message may be output while running tasks.
        if (runningCount == 0) {
            showOverallMessage();
        }
    }

    public long getSpeedLimitForController(SpeedometerSpeedController controller) {
        return controller.getSpeedLimit() / activeControllerCount.get();
    }

    public void checkProgress(long nowTime, int logIntervalMillisec) {
        if (logIntervalMillisec <= 0) {
            return;
        }

        long previousTime = previousLogReportTimeMillisec.get();
        if (previousTime == INITAL_START_TIME) {
            previousLogReportTimeMillisec.compareAndSet(INITAL_START_TIME, nowTime);
        } else {
            long nowInterval = previousTime + logIntervalMillisec;
            if (nowInterval < nowTime) {
                if (previousLogReportTimeMillisec.compareAndSet(previousTime, nowTime)) {
                    showProgressMessage(nowTime);
                    renewPeriods();
                }
            }
        }
    }

    Logger getLogger() {
        return Exec.getLogger(SpeedometerFilterPlugin.class);
    }

    long getGlobalStartTime() {
        return globalStartTime.get();
    }

    int getActiveControllerCount() {
        return activeControllerCount.get();
    }

    long getGlobalTotalBytes() {
        return globalTotalBytes.get();
    }

    long getGlobalTotalRecords() {
        return globalTotalRecords.get();
    }

    List<SpeedometerSpeedController> getControllerList() {
        List<SpeedometerSpeedController> copyList;
        synchronized (controllerList) {
            copyList = new ArrayList<SpeedometerSpeedController>(controllerList);
        }
        return copyList;
    }

    private void renewPeriods() {
        for (SpeedometerSpeedController controller : getControllerList()) {
            controller.renewPeriod();
        }
    }

    private void showProgressMessage(long nowTime) {
        long currentTotalSize = globalTotalBytes.get();
        long currentBytesPerSec = 0;
        long currentTotalRecords = globalTotalRecords.get();
        long currentRecordsPerSec = 0;
        for (SpeedometerSpeedController controller : getControllerList()) {
            currentTotalSize += controller.getTotalBytes();
            currentTotalRecords += controller.getTotalRecords();
            currentBytesPerSec += controller.getPeriodBytesPerSec(nowTime);
            currentRecordsPerSec += controller.getPeriodRecordsPerSec(nowTime);
        }

        long timeDelta = nowTime - globalStartTime.get();
        timeDelta = timeDelta > 0 ? timeDelta : 1;

        showLogMessage(activeControllerCount.get(), currentTotalSize, timeDelta, currentBytesPerSec, currentTotalRecords, currentRecordsPerSec);
    }

    private void showOverallMessage() {
        long timeDelta = System.currentTimeMillis() - globalStartTime.get();
        timeDelta = timeDelta > 0 ? timeDelta : 1;
        long bytesPerSec = (globalTotalBytes.get() * 1000) / timeDelta;
        long recordsPerSec = (globalTotalRecords.get() * 1000) / timeDelta;

        showLogMessage(activeControllerCount.get(), globalTotalBytes.get(), timeDelta, bytesPerSec, globalTotalRecords.get(), recordsPerSec);
    }

    private void showLogMessage(int activeThreads, long totalBytes, long timeMilliSec, long bytesPerSec, long totalRecords, long recordsPerSec) {
        Logger logger = getLogger();
        if (logger != null) {
            logger.info(String.format("{speedometer: {active: %d, total: %s, sec: %s, speed: %s/s, records: %s, record-speed: %s/s}}",
                    activeThreads,
                    SpeedometerUtil.toByteText(totalBytes),
                    SpeedometerUtil.toTimeText(timeMilliSec),
                    SpeedometerUtil.toByteText(bytesPerSec),
                    SpeedometerUtil.toDecimalText(totalRecords),
                    SpeedometerUtil.toDecimalText(recordsPerSec)));
        }
    }
}
