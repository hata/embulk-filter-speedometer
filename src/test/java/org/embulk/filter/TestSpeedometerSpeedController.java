package org.embulk.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.Verifications;

import org.embulk.filter.SpeedometerFilterPlugin.PluginTask;
import org.junit.Test;

public class TestSpeedometerSpeedController {
    @Mocked
    SpeedometerSpeedAggregator aggregator;

    @Mocked
    PluginTask task;

    SpeedometerSpeedController controller;

    @Test
    public void testSpeedometerSpeedController() {
        new NonStrictExpectations() {{
            task.getSpeedLimit(); result = 1L;
            task.getMaxSleepMillisec(); result = 2;
            task.getLogIntervalSeconds(); result = 3;
        }};

        controller = new SpeedometerSpeedController(task, aggregator);
        assertEquals("Verify aggregator is set.", aggregator, controller.getAggregator());
        assertEquals("Verify sleepdLimit is set.", 1, controller.getSpeedLimit());
        assertEquals("Verify maxSleepMillisec is set.", 2, controller.getMaxSleepMillisec());
        assertEquals("Verify logIntervalSeconds is set.", 3 * 1000, controller.getLogIntervalMillisec());

        new Verifications() {{
            task.getSpeedLimit(); times = 1;
            task.getMaxSleepMillisec(); times = 1;
            task.getLogIntervalSeconds(); times = 1;
        }};
    }

    @Test
    public void testStop() {
        controller = new SpeedometerSpeedController(task, aggregator);
        controller.stop();

        new Verifications() {{
            aggregator.stopController(controller); times = 1;
        }};
    }

    @Test
    public void testGetTotalBytes() {
        new NonStrictExpectations() {{
            task.getSpeedLimit(); result = 1L;
            task.getMaxSleepMillisec(); result = 2;
            task.getLogIntervalSeconds(); result = 3;
            aggregator.getSpeedLimitForController((SpeedometerSpeedController)any); result = 10000;
        }};
        long nowTime = System.currentTimeMillis();
        int newDataSize = 3;

        controller = new SpeedometerSpeedController(task, aggregator);
        controller.checkSpeedLimit(nowTime, newDataSize);
        assertEquals("Verify total bytes", newDataSize, controller.getTotalBytes());
        controller.checkSpeedLimit(nowTime + 1, newDataSize);
        assertEquals("Verify total bytes", newDataSize * 2, controller.getTotalBytes());
    }

    @Test
    public void testGetPeriodBytesPerSec() {
        new NonStrictExpectations() {{
            task.getSpeedLimit(); result = 1L;
            task.getMaxSleepMillisec(); result = 2;
            task.getLogIntervalSeconds(); result = 3;
            aggregator.getSpeedLimitForController((SpeedometerSpeedController)any); result = 10000;
        }};
        long nowTime = System.currentTimeMillis();
        int newDataSize = 3;

        controller = new SpeedometerSpeedController(task, aggregator);
        controller.checkSpeedLimit(nowTime, newDataSize);
        assertTrue("Verify total bytes", controller.getPeriodBytesPerSec(System.currentTimeMillis()) > 0);
    }

    @Test
    public void testCheckSpeedLimit() {
        new NonStrictExpectations() {{
            task.getSpeedLimit(); result = 1L;
            task.getMaxSleepMillisec(); result = 2;
            task.getLogIntervalSeconds(); result = 3;
            aggregator.getSpeedLimitForController((SpeedometerSpeedController)any); result = 10000;
        }};
        long nowTime = System.currentTimeMillis();
        int newDataSize = 3;

        controller = new SpeedometerSpeedController(task, aggregator);
        controller.checkSpeedLimit(nowTime, newDataSize);

        new Verifications() {{
            aggregator.startController(controller, anyLong); times = 1;
            aggregator.checkProgress(anyLong, 3 * 1000); times = 1;
            aggregator.getSpeedLimitForController(controller); times = 1;
        }};
    }

    @Test
    public void testRenewPeriod() {
        controller = new SpeedometerSpeedController(task, aggregator);
        controller.renewPeriod();
        assertTrue("Verify renewPeriod flag is set.", controller.isRenewPeriodSet());
    }

}
