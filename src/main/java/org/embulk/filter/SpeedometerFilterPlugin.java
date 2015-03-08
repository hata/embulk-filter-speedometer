package org.embulk.filter;

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
import org.embulk.spi.type.TimestampType;

import com.google.common.collect.ImmutableMap;

public class SpeedometerFilterPlugin
        implements FilterPlugin
{
    private static final int TRUE_LENGTH = Boolean.toString(true).length();
    private static final int FALSE_LENGTH = Boolean.toString(false).length();

    public interface PluginTask
            extends Task,  TimestampFormatter.FormatterTask
    {
        @Config("speed_limit")
        @ConfigDefault("0")
        @Min(0)
        public long getSpeedLimit();

        @Config("max_sleep_millisec")
        @ConfigDefault("1000")
        public int getMaxSleepMillisec();

        @Config("delimiter")
        @ConfigDefault("\",\"")
        public String getDelimiter();

        @Config("record_padding_size")
        @ConfigDefault("1")
        public int getRecordPaddingSize();

        @Config("log_interval_seconds")
        @ConfigDefault("10")
        public int getLogIntervalSeconds();

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

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
        private final ImmutableMap<Column, TimestampFormatter> timestampMap;
        private final PageOutput pageOutput;
        private final PageReader pageReader;
        private final BufferAllocator allocator;
        private final int delimiterLength;
        private final int recordPaddingSize;

        SpeedControlPageOutput(PluginTask task, Schema schema, PageOutput pageOutput) {
            this.controller = new SpeedometerSpeedController(task, SpeedometerSpeedAggregator.getInstance());
            this.schema = schema;
            this.pageOutput = pageOutput;
            this.allocator = task.getBufferAllocator();
            this.delimiterLength = task.getDelimiter().length();
            this.recordPaddingSize = task.getRecordPaddingSize();
            pageReader = new PageReader(schema);
            timestampMap = buildTimestampFormatterMap(task, schema);
        }

        @Override
        public void add(Page page) {
            try (final PageBuilder pageBuilder = new PageBuilder(allocator, schema, pageOutput)) {
                ColumnVisitorImpl visitor = new ColumnVisitorImpl(pageBuilder);
                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    visitor.speedMonitorStartRecord();
                    schema.visitColumns(visitor);
                    visitor.speedMonitorEndRecord();
                    pageBuilder.addRecord();
                }
                pageBuilder.finish();
            }
        }

        @Override
        public void finish() {
            pageOutput.finish();
        }

        @Override
        public void close() {
            controller.stop();
            pageReader.close();
            pageOutput.close();
        }

        private ImmutableMap<Column, TimestampFormatter> buildTimestampFormatterMap(final PluginTask task, Schema schema) {
            final ImmutableMap.Builder<Column, TimestampFormatter> builder = new ImmutableMap.Builder<>();

            schema.visitColumns(new ColumnVisitor() {
                @Override
                public void booleanColumn(Column column) { }

                @Override
                public void longColumn(Column column) { }

                @Override
                public void doubleColumn(Column column) { }

                @Override
                public void stringColumn(Column column) { }

                @Override
                public void timestampColumn(Column column) {
                    if (column.getType() instanceof TimestampType) {
                        TimestampType tt = (TimestampType) column.getType();
                        builder.put(column, new TimestampFormatter(tt.getFormat(), task));
                    } else {
                        throw new RuntimeException("Timestamp should be TimestampType.");
                    }
                }
            });

            return builder.build();
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

            private void speedMonitorStartRecord() {
                startRecordTime = System.currentTimeMillis();
            }

            private void speedMonitorEndRecord() {
                controller.checkSpeedLimit(startRecordTime, recordPaddingSize);
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
                controller.checkSpeedLimit(startRecordTime, String.valueOf(l).length());
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
                TimestampFormatter formatter = timestampMap.get(column);
                controller.checkSpeedLimit(startRecordTime, formatter.format(t).length());
                return t;
            }

            private void speedMonitorForDelimiter(Column column) {
                if (column.getIndex() > 0) {
                    controller.checkSpeedLimit(startRecordTime, delimiterLength);
                }
            }
        }
    }
}
