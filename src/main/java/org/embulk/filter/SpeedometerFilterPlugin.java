package org.embulk.filter;

import java.util.Map;

import javax.validation.constraints.Min;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.util.Timestamps;
import org.msgpack.value.Value;

import com.google.common.base.Optional;

public class SpeedometerFilterPlugin
        implements FilterPlugin
{
    private static final int TRUE_LENGTH = Boolean.toString(true).length();
    private static final int FALSE_LENGTH = Boolean.toString(false).length();

    public interface PluginTask
            extends Task,  TimestampFormatter.Task
    {
        @Config("speed_limit")
        @ConfigDefault("0")
        @Min(0)
        public long getSpeedLimit();

        @Config("max_sleep_millisec")
        @ConfigDefault("1000")
        @Min(0)
        public int getMaxSleepMillisec();

        @Config("delimiter")
        @ConfigDefault("\",\"")
        public String getDelimiter();

        @Config("record_padding_size")
        @ConfigDefault("1")
        public int getRecordPaddingSize();

        @Config("log_interval_seconds")
        @ConfigDefault("10")
        @Min(0)
        public int getLogIntervalSeconds();

        @Config("column_options")
        @ConfigDefault("{}")
        public Map<String, TimestampColumnOption> getColumnOptions();

        @Config("label")
        @ConfigDefault("null")
        public Optional<String> getLabel();

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    public interface TimestampColumnOption extends Task,
    TimestampFormatter.TimestampColumnOption
    { }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        Schema outputSchema = inputSchema;
        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema,
            Schema outputSchema, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        return new SpeedControlPageOutput(task, inputSchema, output);
    }

    static class SpeedControlPageOutput implements PageOutput {
        private final SpeedometerSpeedController controller;
        private final Schema schema;
        private final TimestampFormatter[] timestampFormatters;
        private final PageReader pageReader;
        private final BufferAllocator allocator;
        private final int delimiterLength;
        private final int recordPaddingSize;
        private final PageBuilder pageBuilder;

        SpeedControlPageOutput(PluginTask task, Schema schema, PageOutput pageOutput) {
            this.controller = new SpeedometerSpeedController(task, SpeedometerSpeedAggregator.getInstance(task));
            this.schema = schema;
            this.allocator = task.getBufferAllocator();
            this.delimiterLength = task.getDelimiter().length();
            this.recordPaddingSize = task.getRecordPaddingSize();
            this.pageReader = new PageReader(schema);
            this.timestampFormatters = Timestamps.newTimestampColumnFormatters(task, schema, task.getColumnOptions());
            this.pageBuilder = new PageBuilder(allocator, schema, pageOutput);
        }

        @Override
        public void add(Page page) {
            ColumnVisitorImpl visitor = new ColumnVisitorImpl(pageBuilder);
            pageReader.setPage(page);
            while (pageReader.nextRecord()) {
                visitor.speedMonitorStartRecord();
                schema.visitColumns(visitor);
                visitor.speedMonitorEndRecord();
                pageBuilder.addRecord();
            }
        }

        @Override
        public void finish() {
            controller.stop();
            pageBuilder.finish();
        }

        @Override
        public void close() {
            pageBuilder.close();
            pageReader.close();
        }

        class ColumnVisitorImpl implements ColumnVisitor {
            private final PageBuilder pageBuilder;
            private long startRecordTime;

            ColumnVisitorImpl(PageBuilder pageBuilder) {
                this.pageBuilder = pageBuilder;
            }

            @Override
            public void booleanColumn(Column column) {
                if (pageReader.isNull(column)) {
                    speedMonitor(column);
                    pageBuilder.setNull(column);
                } else {
                    pageBuilder.setBoolean(column, speedMonitor(column, pageReader.getBoolean(column)));
                }
            }

            @Override
            public void longColumn(Column column) {
                if (pageReader.isNull(column)) {
                    speedMonitor(column);
                    pageBuilder.setNull(column);
                } else {
                    pageBuilder.setLong(column, speedMonitor(column, pageReader.getLong(column)));
                }
            }

            @Override
            public void doubleColumn(Column column) {
                if (pageReader.isNull(column)) {
                    speedMonitor(column);
                    pageBuilder.setNull(column);
                } else {
                    pageBuilder.setDouble(column, speedMonitor(column, pageReader.getDouble(column)));
                }
            }

            @Override
            public void stringColumn(Column column) {
                if (pageReader.isNull(column)) {
                    speedMonitor(column);
                    pageBuilder.setNull(column);
                } else {
                    pageBuilder.setString(column, speedMonitor(column, pageReader.getString(column)));
                }
            }

            @Override
            public void timestampColumn(Column column) {
                if (pageReader.isNull(column)) {
                    speedMonitor(column);
                    pageBuilder.setNull(column);
                } else {
                    pageBuilder.setTimestamp(column, speedMonitor(column, pageReader.getTimestamp(column)));
                }
            }

            @Override
            public void jsonColumn(Column column) {
                if (pageReader.isNull(column)) {
                    speedMonitor(column);
                    pageBuilder.setNull(column);
                } else {
                    pageBuilder.setJson(column, speedMonitor(column, pageReader.getJson(column)));
                }
            }

            private void speedMonitorStartRecord() {
                startRecordTime = System.currentTimeMillis();
            }

            private void speedMonitorEndRecord() {
                controller.checkSpeedLimit(startRecordTime, recordPaddingSize, true);
            }

            // For null column
            private void speedMonitor(Column column) {
                speedMonitorForDelimiter(column);
            }

            private boolean speedMonitor(Column column, boolean b) {
                speedMonitorForDelimiter(column);
                controller.checkSpeedLimit(startRecordTime, b ? TRUE_LENGTH : FALSE_LENGTH);
                return b;
            }

            private long speedMonitor(Column column, long l) {
                speedMonitorForDelimiter(column);
                controller.checkSpeedLimit(startRecordTime, SpeedometerUtil.toDigitsTextLength(l));
                return l;
            }

            private double speedMonitor(Column column, double d) {
                speedMonitorForDelimiter(column);
                controller.checkSpeedLimit(startRecordTime, String.valueOf(d).length());
                return d;
            }

            private String speedMonitor(Column column, String s) {
                speedMonitorForDelimiter(column);
                controller.checkSpeedLimit(startRecordTime, s.length());
                return s;
            }

            private Timestamp speedMonitor(Column column, Timestamp t) {
                speedMonitorForDelimiter(column);
                TimestampFormatter formatter = timestampFormatters[column.getIndex()];
                controller.checkSpeedLimit(startRecordTime, formatter.format(t).length());
                return t;
            }

            private Value speedMonitor(Column column, Value v) {
                speedMonitorForDelimiter(column);
                // NOTE: This may not be good for performance. But, I have no other idea.
                String s = v.toJson();
                controller.checkSpeedLimit(startRecordTime, s != null ? s.length() : 0);
                return v;
            }

            private void speedMonitorForDelimiter(Column column) {
                if (column.getIndex() > 0) {
                    controller.checkSpeedLimit(startRecordTime, delimiterLength);
                }
            }
        }
    }
}
