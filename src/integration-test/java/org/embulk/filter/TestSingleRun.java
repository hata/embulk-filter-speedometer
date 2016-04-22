package org.embulk.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.ArrayList;
import java.util.Collections;

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
        validateResultFiles("min_01.csv.gz", "result_min_000.00.csv", "result_min_001.00.csv");
    }

    @Test
    public void testValidateBigOutputFile() throws Exception {
        validateResultFiles("big_01.csv.gz", "result_big_000.00.csv", "result_big_001.00.csv");
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

    private void validateResultFiles(String gzipSrcFile, String... resultFiles) throws Exception {
        ArrayList inList = new ArrayList();
        ArrayList outList = new ArrayList();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
            new GZIPInputStream(new FileInputStream(getTestFile(gzipSrcFile)))))) {
            String line = reader.readLine(); // Discard a header line
            line = reader.readLine();
            while (line != null) {
                inList.add(line);
                line = reader.readLine();
            }
        }

        for (String resultFile : resultFiles) {
            try (BufferedReader reader = new BufferedReader(new FileReader(getTestFile(resultFile)))) {
                String line = reader.readLine(); // Discard a header line
                line = reader.readLine();
                while (line != null) {
                    outList.add(line);
                    line = reader.readLine();
                }
            }
        }

        Collections.sort(inList);
        Collections.sort(outList);

        assertEquals("Verify input and output lines are identical.", inList, outList);
    }
}
