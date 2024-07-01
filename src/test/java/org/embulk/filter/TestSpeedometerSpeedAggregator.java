package org.embulk.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.embulk.filter.SpeedometerFilterPlugin.PluginTask;
import org.junit.Test;
import org.slf4j.Logger;

import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;

public class TestSpeedometerSpeedAggregator {
    @Mocked SpeedometerSpeedController controller;
    @Mocked PluginTask task;

    @Test
    public void testGetInstance() {
        assertNotNull("Verify there is a singleton.", SpeedometerSpeedAggregator.getInstance(task));
    }

    @Test
    public void testSpeedometerSpeedAggregator() {
        SpeedometerSpeedAggregator aggregator = new SpeedometerSpeedAggregator();
        assertEquals("Verify the default global start time is zero.", 0, aggregator.getGlobalStartTime());
        assertEquals("Verify the default active count is zero.", 0, aggregator.getActiveControllerCount());
        assertEquals("{speedometer: {active: %d, total: %s, sec: %s, speed: %s/s, records: %s, record-speed: %s/s}}", aggregator.getLogFormat());
    }

    @Test
    public void testSpeedometerSpeedAggregatorWithLabel() {
        SpeedometerSpeedAggregator aggregator = new SpeedometerSpeedAggregator("foo");
        assertEquals("Verify the default global start time is zero.", 0, aggregator.getGlobalStartTime());
        assertEquals("Verify the default active count is zero.", 0, aggregator.getActiveControllerCount());
        assertEquals("{speedometer: {label: foo, active: %d, total: %s, sec: %s, speed: %s/s, records: %s, record-speed: %s/s}}", aggregator.getLogFormat());
    }

    @Test
    public void testStartController() {
        SpeedometerSpeedAggregator aggregator = new SpeedometerSpeedAggregator();
        long nowTime = System.currentTimeMillis();
        aggregator.startController(controller, nowTime);
        assertEquals("Verify global start time is set.", nowTime, aggregator.getGlobalStartTime());
        assertEquals("Verify active controller count.", 1, aggregator.getActiveControllerCount());
        assertTrue("Verify there is a registered controller.", aggregator.getControllerList().contains(controller));
    }

    @Test
    public void testStopController() {
        new Expectations() {{
            controller.getTotalBytes(); result = 11;
            controller.getTotalRecords(); result = 5;
        }};

        SpeedometerSpeedAggregator aggregator = new SpeedometerSpeedAggregator();
        long nowTime = System.currentTimeMillis();
        aggregator.startController(controller, nowTime);
        aggregator.stopController(controller);
        assertEquals("Verify active controller count.", 0, aggregator.getActiveControllerCount());
        assertFalse("Verify there is a registered controller.", aggregator.getControllerList().contains(controller));
        assertEquals("Verify controller's total bytes are added to aggregator.", 11, aggregator.getGlobalTotalBytes());
        assertEquals("Verify controller's total records are added to aggregator.", 5, aggregator.getGlobalTotalRecords());
    }

    @Test
    public void testStopControllerShowOverallMessage(@Mocked final Logger logger) {
        SpeedometerSpeedAggregator aggregator = new SpeedometerSpeedAggregator();
        new Expectations(aggregator) {{
            aggregator.getLogger(); result = logger;
        }};

        long nowTime = System.currentTimeMillis();
        aggregator.startController(controller, nowTime);
        aggregator.startController(controller, nowTime);
        aggregator.stopController(controller);
        aggregator.stopController(controller);

        new Verifications() {{
            logger.info(withAny("Overall message.")); times = 1;
        }};
   }

    @Test
    public void testGetSpeedLimitForController() {
        new Expectations() {{
            controller.getSpeedLimit(); result = 10;
        }};

        SpeedometerSpeedAggregator aggregator = new SpeedometerSpeedAggregator();
        long nowTime = System.currentTimeMillis();
        aggregator.startController(controller, nowTime);
        assertEquals("Verify speed limit for one controller.", 10, aggregator.getSpeedLimitForController(controller));
        aggregator.startController(controller, nowTime);
        assertEquals("Verify speed limit for two controllers.", 10 / 2, aggregator.getSpeedLimitForController(controller));
    }

    @Test
    public void testCheckProgress() throws Exception {
        SpeedometerSpeedAggregator aggregator = new SpeedometerSpeedAggregator();
        long initPeriodTime = System.currentTimeMillis();
        final long startPeriodTime = initPeriodTime + 2;
        aggregator.startController(controller, initPeriodTime);
        aggregator.checkProgress(initPeriodTime, 1);
        aggregator.checkProgress(startPeriodTime, 1);
        aggregator.checkProgress(startPeriodTime, 1); // This call should not affect anything.

        new Verifications() {{
            controller.renewPeriod(); times = 1;
            controller.getTotalBytes(); times = 1;
            controller.getTotalRecords(); times = 1;
            controller.getPeriodBytesPerSec(startPeriodTime); times = 1;
            controller.getPeriodRecordsPerSec(startPeriodTime); times = 1;
        }};
    }

    @Test
    public void testCheckProgressIsDisabled() throws Exception {
        SpeedometerSpeedAggregator aggregator = new SpeedometerSpeedAggregator();
        long initPeriodTime = System.currentTimeMillis();
        final long startPeriodTime = initPeriodTime + 2;
        aggregator.startController(controller, initPeriodTime);
        aggregator.checkProgress(initPeriodTime, 0);
        aggregator.checkProgress(startPeriodTime, 0);

        new Verifications() {{
            controller.renewPeriod(); times = 0;
            controller.getTotalBytes(); times = 0;
            controller.getTotalRecords(); times = 0;
            controller.getPeriodBytesPerSec(startPeriodTime); times = 0;
            controller.getPeriodRecordsPerSec(startPeriodTime); times = 0;
        }};
    }
}
