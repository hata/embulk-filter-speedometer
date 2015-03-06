package org.embulk.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.embulk.filter.SpeedometerFilterPlugin.PluginTask;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

interface SpeedometerSpeedController {
    void start();
    void stop();

    public long getSpeedLimit();
    void checkSpeedLimit(long newDataSize);
    public long getTotalBytes();
    public void renewPeriod();
    public long getPeriodBytesPerSec(long nowTime);
}

class SpeedometerSingleSpeedController implements SpeedometerSpeedController {
    private final long limitBytesPerSec;
    private final int maxSleepMillisec;
    private final long startTime;
    private long totalBytes;

    SpeedometerSingleSpeedController(PluginTask task) {
        this.limitBytesPerSec = task.getSpeedLimit();
        this.maxSleepMillisec = task.getMaxSleepMillisec();
        startTime = System.currentTimeMillis();
    }

    public long getSpeedLimit() {
        return limitBytesPerSec;
    }
    
    public int getMaxSleepMillisec() {
        return maxSleepMillisec;
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    public void checkSpeedLimit(long newDataSize) {
        totalBytes += newDataSize;

        if (limitBytesPerSec <= 0) {
            return;
        }

        long nowTime = System.currentTimeMillis(); // Do I need to use nanoTime ?
        if (startTime == nowTime) {
            nowTime += 1;
        }

        long timeDelta = nowTime - startTime;

        long bytesPerSec = (totalBytes * 1000) / timeDelta;
        long overBytes = bytesPerSec - limitBytesPerSec;
        if (overBytes > 0) {
            try {
                long sleepTime = (totalBytes *1000) / limitBytesPerSec - timeDelta;
                sleepTime = sleepTime > maxSleepMillisec ? maxSleepMillisec : sleepTime > 0 ? sleepTime : 0;
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                // TODO: Do I need to throw an exception ?
            }
        }
    }
    
    public String getStatsReport() {
        long timeDelta = (System.currentTimeMillis() - startTime);
        timeDelta = timeDelta > 0 ? timeDelta : 1;
        long bytesPerSec = ((totalBytes * 1000) / timeDelta);
        return String.format("filter-speedometer %d bytes / %d seconds (%d bytes/sec)", totalBytes, (long)timeDelta/1000, bytesPerSec);
    }

    public void start() {
        
    }

    public void stop() {
    }
    
    public void renewPeriod() {
        
    }

    public long getPeriodBytesPerSec(long nowTime) {
        return 0;
    }
}



class GlobalSpeedMonitor {
    private final long INITAL_START_TIME = 0;

    private final AtomicInteger controllerIDGenerator = new AtomicInteger(0);
    private final AtomicInteger masterControllerID = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger activeControllerCount = new AtomicInteger(0);
    private final AtomicLong globalStartTime = new AtomicLong(INITAL_START_TIME);
    private final AtomicLong globalTotalBytes = new AtomicLong(0);
    private final AtomicInteger monitorVersion = new AtomicInteger(0);
    private final long logIntervalSeconds = 10;
    private final AtomicLong previousLogReportTimeMillisec = new AtomicLong(INITAL_START_TIME);
    private final AtomicLong renewPeriodTimeMillisec = new AtomicLong(INITAL_START_TIME);

    // TODO: We can use google's library.
    private final List<SpeedometerSpeedController> controllerList = new ArrayList<>();

    public GlobalSpeedMonitor() {
    }

    public int getNewControllerID() {
        return  controllerIDGenerator.getAndIncrement();        
    }

    public void startController(SpeedometerSpeedController controller, long nowTime) {
        globalStartTime.compareAndSet(INITAL_START_TIME, nowTime);
        activeControllerCount.incrementAndGet();

        synchronized (controllerList) {
            controllerList.add(controller);
        }

        monitorVersion.incrementAndGet();
    }

    public void stopController(SpeedometerSpeedController controller) {
        synchronized (controllerList) {
            controllerList.remove(controller);
        }
        
        long runningCount = activeControllerCount.decrementAndGet();
        monitorVersion.incrementAndGet();
        globalTotalBytes.addAndGet(controller.getTotalBytes());
        
        // TODO: report total messages.
        if (runningCount == 0) {
            showOverallMessage();
        }
    }

    int getMonitorVersion() {
        return monitorVersion.get();
    }
    
    boolean isMasterController(int controllerID) {
        return controllerID == masterControllerID.get();
    }
    
    public void askControllers() {
        List<SpeedometerSpeedController> copyList;
        synchronized (controllerList) {
            copyList = new ArrayList<SpeedometerSpeedController>(controllerList);
        }

        for (SpeedometerSpeedController controller : copyList) {
            // do something to each controller.
            // controller.getTotalBytes().
        }
    }

    public long getSpeedLimitForController(SpeedometerSpeedController controller) {
        return controller.getSpeedLimit() / activeControllerCount.get();
    }
    
    public void checkProgress(long nowTime) {
        long previousTime = previousLogReportTimeMillisec.get();
        if (previousTime == 0) {
            previousLogReportTimeMillisec.compareAndSet(0, nowTime);
        } else {
            long nowInterval = previousTime + logIntervalSeconds * 1000;
            if (nowInterval < nowTime) {
                if (previousLogReportTimeMillisec.compareAndSet(previousTime, nowTime)) {
                    showProgressMessage();
                    renewPeriods();
                }
            }
        }
    }
    
    
    private void renewPeriods() {
        List<SpeedometerSpeedController> copyList;
        synchronized (controllerList) {
            copyList = new ArrayList<SpeedometerSpeedController>(controllerList);
        }

        for (SpeedometerSpeedController controller : copyList) {
            controller.renewPeriod();
        }
        
    }

    // TODO: This should check speed for current progress threads.
    private void showProgressMessage() {
        long nowTime = System.currentTimeMillis();
        
        List<SpeedometerSpeedController> copyList;
        synchronized (controllerList) {
            copyList = new ArrayList<SpeedometerSpeedController>(controllerList);
        }

        long currentTotalSize = 0;
        long currentBytesPerSec = 0;
        for (SpeedometerSpeedController controller : copyList) {
            currentTotalSize += controller.getTotalBytes();
            currentBytesPerSec += controller.getPeriodBytesPerSec(nowTime);
        }
        
        long timeDelta = nowTime - globalStartTime.get();
        timeDelta = timeDelta > 0 ? timeDelta : 1;
        Logger logger = Exec.getLogger(SpeedometerFilterPlugin.class);
        logger.info(String.format("speedometer active:%d  filter: %d bytes / %d seconds ( %d bytes/sec)",
                activeControllerCount.get(), currentTotalSize, timeDelta/1000, currentBytesPerSec));
    }

    private void showOverallMessage() {
        long timeDelta = System.currentTimeMillis() - globalStartTime.get();
        timeDelta = timeDelta > 0 ? timeDelta : 1;
        long bytesPerSec = (globalTotalBytes.get() * 1000) / timeDelta;
        Logger logger = Exec.getLogger(SpeedometerFilterPlugin.class);
        logger.info(String.format("speedometer overall %d bytes / %d seconds ( %d bytes/sec)", globalTotalBytes.get(), timeDelta/1000, bytesPerSec));
    }
}

class SpeedometerMultiSpeedController implements SpeedometerSpeedController  {
    private static final GlobalSpeedMonitor monitor = new GlobalSpeedMonitor();

    private final long limitBytesPerSec;
    private final int maxSleepMillisec;
    private final int controllerID;
    private long startTime;
    private volatile long periodStartTime;
    private volatile long periodTotalBytes;
    private volatile long threadTotalBytes;
    private int previousMonitorVersion;
    private volatile boolean renewFlag = false;

    SpeedometerMultiSpeedController(PluginTask task) {
        this.limitBytesPerSec = task.getSpeedLimit();
        this.maxSleepMillisec = task.getMaxSleepMillisec();
        this.controllerID = monitor.getNewControllerID();
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        startNewPeriod(0);
        monitor.stopController(this);
    }

    public long getSpeedLimit() {
        return limitBytesPerSec;
    }
    
    public int getMaxSleepMillisec() {
        return maxSleepMillisec;
    }

    public long getTotalBytes() {
        return threadTotalBytes + periodTotalBytes;
    }
    
    public long getPeriodBytesPerSec(long nowTime) {
        long timeDeltaMillisec = nowTime - periodStartTime;
        if (timeDeltaMillisec <= 0) {
            timeDeltaMillisec = 1;
        }
        return (periodTotalBytes * 1000) / timeDeltaMillisec;        
    }
    
    public int getControllerID() {
        return controllerID;
    }

    public void checkSpeedLimit(long newDataSize) {
        long nowTime = System.currentTimeMillis();

        if (startTime == 0) {
            startTime = nowTime;
            monitor.startController(this, startTime);
        }
        
        int monitorVersion = monitor.getMonitorVersion();
        if (monitorVersion != previousMonitorVersion) {
            previousMonitorVersion = monitorVersion;
            startNewPeriod(nowTime);
        } else if (renewFlag) {
            renewFlag = false;
            startNewPeriod(nowTime);
        }

        periodTotalBytes += newDataSize;
        monitor.checkProgress(nowTime);
        
        if (limitBytesPerSec <= 0) {
            return;
        }

        long speedLimitForThread = monitor.getSpeedLimitForController(this);
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

    public void renewPeriod() {
        renewFlag = true;
    }

    private void startNewPeriod(long newPeriodTime) {
        threadTotalBytes += periodTotalBytes;
        periodTotalBytes = 0;
        periodStartTime = newPeriodTime;
    }
}
