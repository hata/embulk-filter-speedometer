package org.embulk.filter;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestSpeedometerUtil {

    @Test
    public void testToNumberText() {
        assertEquals("Verify 0 bytes",  " 0.0B", SpeedometerUtil.toNumberText(0));
        assertEquals("Verify 0 bytes", " 1.0KB", SpeedometerUtil.toNumberText(1000L));
        assertEquals("Verify 0 bytes", " 1.0MB", SpeedometerUtil.toNumberText(1000L * 1000));
        assertEquals("Verify 0 bytes", " 1.0GB", SpeedometerUtil.toNumberText(1000L * 1000 * 1000));
        assertEquals("Verify 0 bytes", " 1.0TB", SpeedometerUtil.toNumberText(1000L * 1000 * 1000 * 1000));
        assertEquals("Verify 0 bytes", " 1.0PB", SpeedometerUtil.toNumberText(1000L * 1000 * 1000 * 1000 * 1000));
        assertEquals("Verify 0 bytes", " 1.0EB", SpeedometerUtil.toNumberText(1000L * 1000 * 1000 * 1000 * 1000 * 1000));
    }

    @Test
    public void testToTimeText() {
        assertEquals("Verify 0 msec", "0.00", SpeedometerUtil.toTimeText(0));
        assertEquals("Verify 1 sec", "1.00", SpeedometerUtil.toTimeText(1000));
        assertEquals("Verify 9 secs", "9.00", SpeedometerUtil.toTimeText(9000));
        assertEquals("Verify 10 secs", "10.0", SpeedometerUtil.toTimeText(10000));
        assertEquals("Verify 1 min", "1:00", SpeedometerUtil.toTimeText(60000));
        assertEquals("Verify 10 mins 10 secs", "10:01", SpeedometerUtil.toTimeText(60000 * 10 + 1000));
        assertEquals("Verify 1 hour 10 mins 10 secs", "1:10:01", SpeedometerUtil.toTimeText(60000 * 10 + 1000 + 3600 * 1000));
        assertEquals("Verify 10 hours 10 mins 10 secs", "10:10:01", SpeedometerUtil.toTimeText(60000 * 10 + 1000 + 10 * 3600 * 1000));
        assertEquals("Verify 23 hours 10 mins 10 secs", "23:10:01", SpeedometerUtil.toTimeText(60000 * 10 + 1000 + 23 * 3600 * 1000));
        assertEquals("Verify 1.0 days", "1.0 days", SpeedometerUtil.toTimeText(60L * 60 * 24 * 1000));
        assertEquals("Verify 100 days", "100 days", SpeedometerUtil.toTimeText(60L * 60 * 24 * 1000 * 100));
    }

}
