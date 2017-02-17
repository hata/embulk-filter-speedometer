package org.embulk.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.List;
import java.util.Collections;
import java.util.Set;

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
        validateResultFiles("min_01.csv.gz", "result_min_");
    }

    @Test
    public void testValidateBigOutputFile() throws Exception {
        validateResultFiles("big_01.csv.gz", "result_big_");
    }

    @Test
    public void testValidateJsonOutputFile() throws Exception {
        validateJsonResultFiles("ref_json_result_01.csv.gz", "result_json_");
    }

    @Test
    public void testSpeedometerMinLog() throws Exception {
        validateSpeedometerLog("config_min.yml.run.log");
    }

    @Test
    public void testSpeedometerBigLog() throws Exception {
        validateSpeedometerLog("config_big.yml.run.log");
    }

    @Test
    public void testSpeedometerJsonLog() throws Exception {
        validateSpeedometerLog("config_json.yml.run.log");
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

    private void validateResultFiles(String gzipSrcFile, final String prefix) throws Exception {
        ArrayList<String> inList = new ArrayList();
        ArrayList<String> outList = new ArrayList();

        readToListFromGzipFile(gzipSrcFile, inList);
        readToListFromPrefixMatching(prefix, outList);

        Collections.sort(inList);
        Collections.sort(outList);

        assertEquals("Verify input and output lines are identical. in:" +
            inList.size() + ", out:" + outList.size(), inList.toString(), outList.toString());
    }


    private void validateJsonResultFiles(String gzipSrcFile, final String prefix) throws Exception {
        ArrayList<String> inList = new ArrayList();
        ArrayList<String> outList = new ArrayList();

        readToListFromGzipFile(gzipSrcFile, inList);
        readToListFromPrefixMatching(prefix, outList);

        assertEquals("Verify input and output lines are identical. in:" +
            inList.size() + ", out:" + outList.size(), readToSet(inList), readToSet(outList));
    }

    private void readToListFromGzipFile(String gzipSrcFile, List<String> lineList) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
            new GZIPInputStream(new FileInputStream(getTestFile(gzipSrcFile)))))) {
            String line = reader.readLine(); // Discard a header line
            line = reader.readLine();
            while (line != null) {
                lineList.add(line);
                line = reader.readLine();
            }
        }
    }

    private void readToListFromPrefixMatching(final String prefix, List<String> lineList) throws IOException {
        // In travis env, there are many cpus and it may be different from
        // my local environment. From this, list all files using File.list method.
        String[] resultFiles = new File(TEST_DIR).list(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith(prefix) && name.endsWith(".csv");
            }
        });

        for (String resultFile : resultFiles) {
            try (BufferedReader reader = new BufferedReader(new FileReader(getTestFile(resultFile)))) {
                String line = reader.readLine(); // Discard a header line
                line = reader.readLine();
                while (line != null) {
                    lineList.add(line);
                    line = reader.readLine();
                }
            }
        }
    }

    private TreeSet<String> readToSet(List<String> lineList) throws Exception {
        TreeSet<String> set = new TreeSet<>();
        for (String line : lineList) {
            line = stripQuote(line);
            if (line.startsWith("{") && line.endsWith("}")) {
                ArrayList<String> fields = new ArrayList<>();
                for (String field : line.substring(1, line.length() - 1).split(",")) {
                    fields.add(field);
                }
                Collections.sort(fields);
                set.add(fields.toString());
            } else {
                throw new Exception("Unexpected lines." + lineList);
            }
        }
        return set;
    }

    private String stripQuote(String line) {
        return line.startsWith("'") && line.endsWith("'") ? line.substring(1, line.length() -1) : line;
    }
}
