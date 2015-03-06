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
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.TimestampType;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableMap;

public class SpeedometerFilterPlugin
        implements FilterPlugin
{
    private static final int TRUE_LENGTH = "true".length();
    private static final int FALSE_LENGTH = "false".length();

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
        @ConfigDefault("60")
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
        private final PageReader reader;
        private final PluginTask task;
        private final Logger log;
        
        SpeedControlPageOutput(PluginTask task, Schema schema, PageOutput pageOutput) {
            this.controller = new SpeedometerMultiSpeedController(task);
            this.schema = schema;
            this.pageOutput = pageOutput;
            this.task = task;
            reader = new PageReader(schema);
            timestampMap = buildTimestampFormatterMap(schema);
            this.log = Exec.getLogger(SpeedometerFilterPlugin.class);
            
            this.controller.start();
        }

        @Override
        public void add(Page page) {
            try (final PageBuilder pageBuilder = new PageBuilder(task.getBufferAllocator(), schema, pageOutput)) {
                reader.setPage(page);
                while (reader.nextRecord()) {
                    // TODO: This should change for performance.
                    schema.visitColumns(new ColumnVisitor() {
                        @Override
                        public void booleanColumn(Column column) {
                            if (reader.isNull(column)) {
                                speedMonitor(column);
                                pageBuilder.setNull(column);
                            } else {
                                pageBuilder.setBoolean(column, speedMonitor(column, reader.getBoolean(column)));
                            }
                        }

                        @Override
                        public void longColumn(Column column) {
                            if (reader.isNull(column)) {
                                speedMonitor(column);
                                pageBuilder.setNull(column);
                            } else {
                                pageBuilder.setLong(column, speedMonitor(column, reader.getLong(column)));
                            }
                        }

                        @Override
                        public void doubleColumn(Column column) {
                            if (reader.isNull(column)) {
                                speedMonitor(column);
                                pageBuilder.setNull(column);
                            } else {
                                pageBuilder.setDouble(column, speedMonitor(column, reader.getDouble(column)));
                            }
                        }

                        @Override
                        public void stringColumn(Column column) {
                            if (reader.isNull(column)) {
                                speedMonitor(column);
                                pageBuilder.setNull(column);
                            } else {
                                pageBuilder.setString(column, speedMonitor(column, reader.getString(column)));
                            }
                        }

                        @Override
                        public void timestampColumn(Column column) {
                            if (reader.isNull(column)) {
                                speedMonitor(column);
                                pageBuilder.setNull(column);
                            } else {
                                pageBuilder.setTimestamp(column, speedMonitor(column, reader.getTimestamp(column)));
                            }
                        }
                    });
                    speedMonitorEndRecord();
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
            reader.close();
            pageOutput.close();
        }

        private ImmutableMap<Column, TimestampFormatter> buildTimestampFormatterMap(Schema schema) {
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
                        throw new RuntimeException("timestamp should be TimestampType.");
                    }
                }
            });

            return builder.build();
        }

        // For null column
        private void speedMonitor(Column column) {
            speedMonitorForDelimiter(column);
        }

        private boolean speedMonitor(Column column, boolean b) {
            speedMonitorForDelimiter(column);
            controller.checkSpeedLimit(b ? TRUE_LENGTH : FALSE_LENGTH);
            return b;
        }

        private long speedMonitor(Column column, long l) {
            speedMonitorForDelimiter(column);
            controller.checkSpeedLimit(String.valueOf(l).length());
            return l;
        }

        private double speedMonitor(Column column, double d) {
            speedMonitorForDelimiter(column);
            controller.checkSpeedLimit(String.valueOf(d).length());
            return d;
        }

        private String speedMonitor(Column column, String s) {
            speedMonitorForDelimiter(column);
            controller.checkSpeedLimit(s.length());
            return s;
        }

        private Timestamp speedMonitor(Column column, Timestamp t) {
            speedMonitorForDelimiter(column);
            TimestampFormatter formatter = timestampMap.get(column);
            controller.checkSpeedLimit(formatter.format(t).length());
            return t;
        }

        private void speedMonitorForDelimiter(Column column) {
            if (column.getIndex() > 0) {
                controller.checkSpeedLimit(task.getDelimiter().length());
            }
        }
        
        private void speedMonitorEndRecord() {
            controller.checkSpeedLimit(task.getRecordPaddingSize());
        }
    }
}
