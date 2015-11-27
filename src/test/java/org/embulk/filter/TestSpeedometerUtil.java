package org.embulk.filter;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestSpeedometerUtil {

    @Test
    public void testToByteText() {
        assertEquals("Verify 0 bytes",  "0.0b", SpeedometerUtil.toByteText(0));
        assertEquals("Verify 0 bytes",  "100b", SpeedometerUtil.toByteText(100));
        assertEquals("Verify 0 bytes", "1.0kb", SpeedometerUtil.toByteText(1000L));
        assertEquals("Verify 0 bytes", "100kb", SpeedometerUtil.toByteText(1000L * 100));
        assertEquals("Verify 0 bytes", "1.0mb", SpeedometerUtil.toByteText(1000L * 1000));
        assertEquals("Verify 0 bytes", "100mb", SpeedometerUtil.toByteText(1000L * 1000 * 100));
        assertEquals("Verify 0 bytes", "1.0gb", SpeedometerUtil.toByteText(1000L * 1000 * 1000));
        assertEquals("Verify 0 bytes", "100gb", SpeedometerUtil.toByteText(1000L * 1000 * 1000 * 100));
        assertEquals("Verify 0 bytes", "1.0tb", SpeedometerUtil.toByteText(1000L * 1000 * 1000 * 1000));
        assertEquals("Verify 0 bytes", "100tb", SpeedometerUtil.toByteText(1000L * 1000 * 1000 * 1000 * 100));
        assertEquals("Verify 0 bytes", "1.0pb", SpeedometerUtil.toByteText(1000L * 1000 * 1000 * 1000 * 1000));
        assertEquals("Verify 0 bytes", "100pb", SpeedometerUtil.toByteText(1000L * 1000 * 1000 * 1000 * 1000 * 100));
        assertEquals("Verify 0 bytes", "1.0eb", SpeedometerUtil.toByteText(1000L * 1000 * 1000 * 1000 * 1000 * 1000));
    }

    @Test
    public void testToDecimalText() {
        assertEquals("Verify 0 bytes", "0", SpeedometerUtil.toDecimalText(0));
        assertEquals("Verify 0 bytes", "100", SpeedometerUtil.toDecimalText(100));
        assertEquals("Verify 0 bytes", "1,000", SpeedometerUtil.toDecimalText(1000L));
        assertEquals("Verify 0 bytes", "100,000", SpeedometerUtil.toDecimalText(1000L * 100));
        assertEquals("Verify 0 bytes", "1,000,000", SpeedometerUtil.toDecimalText(1000L * 1000));
        assertEquals("Verify 0 bytes", "100,000,000", SpeedometerUtil.toDecimalText(1000L * 1000 * 100));
        assertEquals("Verify 0 bytes", "1,000,000,000", SpeedometerUtil.toDecimalText(1000L * 1000 * 1000));
        assertEquals("Verify 0 bytes", "100,000,000,000", SpeedometerUtil.toDecimalText(1000L * 1000 * 1000 * 100));
        assertEquals("Verify 0 bytes", "1,000,000,000,000", SpeedometerUtil.toDecimalText(1000L * 1000 * 1000 * 1000));
        assertEquals("Verify 0 bytes", "100,000,000,000,000", SpeedometerUtil.toDecimalText(1000L * 1000 * 1000 * 1000 * 100));
        assertEquals("Verify 0 bytes", "1,000,000,000,000,000", SpeedometerUtil.toDecimalText(1000L * 1000 * 1000 * 1000 * 1000));
        assertEquals("Verify 0 bytes", "100,000,000,000,000,000", SpeedometerUtil.toDecimalText(1000L * 1000 * 1000 * 1000 * 1000 * 100));
        assertEquals("Verify 0 bytes", "1,000,000,000,000,000,000", SpeedometerUtil.toDecimalText(1000L * 1000 * 1000 * 1000 * 1000 * 1000));
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

    @Test
    public void testToNumberLengthForZero() {
        assertEquals("Verify toNumberText to zero", 1, SpeedometerUtil.toDigitsTextLength(0));
    }

    @Test
    public void testToNumberLengthForMax() {
        assertEquals("Verify toNumberText to max", String.valueOf(Long.MAX_VALUE).length(), SpeedometerUtil.toDigitsTextLength(Long.MAX_VALUE));
    }

    @Test
    public void testToNumberLengthForMin() {
        assertEquals("Verify toNumberText to min", String.valueOf(Long.MIN_VALUE).length(), SpeedometerUtil.toDigitsTextLength(Long.MIN_VALUE));
    }

    @Test
    public void testToNumberLengthForSeveralValues() {
        for (int i = -1024;i < 1024;i++) {
            assertEquals("Verify toNumberText to " + i, String.valueOf(i).length(), SpeedometerUtil.toDigitsTextLength(i));
        }
    }
}
