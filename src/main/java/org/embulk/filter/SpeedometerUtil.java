package org.embulk.filter;

public class SpeedometerUtil {

    public static String toNumberText(long originalNum) {
        long baseNum = 1000;
        long remain = 0;
        float num = originalNum;

        String[] units = new String[]{ "b", "kb", "mb", "gb", "tb", "pb", "eb", "zb", "yb" };
        for (String unit : units) {
            if (num < baseNum) {
                if (num < baseNum/10) {
                    return String.format("%1.1f%s", num + ((float)remain) / baseNum, unit);
                } else {
                    return String.format("%2d%s", (long)num, unit);
                }
            } else {
                num /= baseNum;
            }
        }

        return String.valueOf(originalNum);
    }

    public static String toTimeText(long timeMillisecs) {
        long num = timeMillisecs;

        long mseconds = num % 1000;
        num /= 1000;

        if (num < 10) {
            return String.format("%1.2f", num + mseconds / 1000.0);
        }

        if (num < 60) {
            return String.format("%1.1f", num + mseconds / 1000.0);
        }

        long seconds = num % 60;
        num /= 60;

        if (num < 60) {
            return String.format("%1d:%02d", num, seconds);
        }

        long mins = num % 60;
        num /= 60;

        if (num < 24) {
            return String.format("%1d:%02d:%02d", num, mins, seconds);
        }

        long hours = num % 24;
        num /= 24;

        if (num < 10) {
            return String.format("%1.1f days", num + hours / 24.0);
        }

        return String.format("%1d days", num);
    }
}
