package org.embulk.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import java.util.zip.GZIPInputStream;

import org.junit.Test;

public class TestSingleRun {
    static final String TEST_DIR = System.getProperty("embulk.integrationtest.dir");

    // e.g. {speedometer: {active: 4, total: 13.5mb, sec: 1:51, speed: 121kb/s, records: 269,748, record-speed: 2,435/s}}
    static final Pattern logLinePattern = Pattern.compile("\\{speedometer: \\{active: [^,]+, total: [^,]+, sec: [^,]+, speed: [^,]+, records: \\S+, record-speed: [^\\}]+\\}\\}");

    private static String getTestFile(String name) {
        return TEST_DIR + File.separator + name;
    }

    @Test
    public void testValidateMinOutputFile() throws Exception {
        validateResultFile("min_01.csv.gz", "result_min_000.00.csv");
    }

    @Test
    public void testValidateBigOutputFile() throws Exception {
        validateResultFile("big_01.csv.gz", "result_big_000.00.csv");
    }

    @Test
    public void testSpeedometerMinLog() throws Exception {
        validateSpeedometerLog("config_min.yml.run.log");
    }

    @Test
    public void testSpeedometerBigLog() throws Exception {
        validateSpeedometerLog("config_big.yml.run.log");
    }

    private void validateSpeedometerLog(String logFile) throws Exception {
        boolean found = false;
        try (BufferedReader r = new BufferedReader(new FileReader(getTestFile(logFile)))) {
            String line = r.readLine();
            while (line != null) {
                if (logLinePattern.matcher(line).find()) {
                    found = true;
                    break;
                }
                line = r.readLine();
            }
        }
        assertTrue("Verify there are speedometer log lines.", found);
    }

    private void validateResultFile(String gzipSrcFile, String resultFile) throws Exception {
        long sourceCksum;
        long destCksum;

        try (InputStream in = new GZIPInputStream(new FileInputStream(getTestFile(gzipSrcFile)))) {
            sourceCksum = getChecksum(in);
        }

        try (InputStream in = new FileInputStream(getTestFile(resultFile))) {
            destCksum = getChecksum(in);
        }

        assertEquals("Verify input and output contents are identical.", sourceCksum, destCksum);
    }

    private long getChecksum(InputStream in) throws IOException {
        Checksum cksum = new CRC32();
        byte[] buf = new byte[8192];
        int len = in.read(buf);
        while (len > 0) {
            cksum.update(buf, 0, len);
            len = in.read(buf);
        }
        return cksum.getValue();
    }
}
