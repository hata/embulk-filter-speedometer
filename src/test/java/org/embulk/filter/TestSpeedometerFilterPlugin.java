package org.embulk.filter;

import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.Verifications;

import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.filter.SpeedometerFilterPlugin.PluginTask;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.junit.Test;

public class TestSpeedometerFilterPlugin
{
    @Mocked
    Exec exec;

    @Mocked
    ConfigSource config;

    @Mocked
    PluginTask task;

    @Mocked
    TaskSource taskSource;

    @Mocked
    Schema schema;

    @Mocked
    PageOutput inPageOutput;

    @Mocked
    FilterPlugin.Control control;

    @Test
    public void testTransaction() {
        new NonStrictExpectations() {{
            config.loadConfig(PluginTask.class); result = task;
        }};

        SpeedometerFilterPlugin plugin = new SpeedometerFilterPlugin();
        plugin.transaction(config, schema, control);

        new Verifications() {{
            config.loadConfig(PluginTask.class); times = 1;
            control.run((TaskSource)any, schema); times = 1;
        }};
    }

    @Test
    public void testOpen(final @Mocked PageReader reader, final @Mocked PageBuilder builder, final @Mocked Page page) throws Exception {
        new NonStrictExpectations() {{
            taskSource.loadTask(PluginTask.class); result = task;
            task.getDelimiter(); result = "";
            reader.nextRecord(); result = true; result = false;
        }};

        SpeedometerFilterPlugin plugin = new SpeedometerFilterPlugin();
        PageOutput output = plugin.open(taskSource, schema, schema, inPageOutput);
        output.add(page);

        new Verifications() {{
            taskSource.loadTask(PluginTask.class); times = 1;
            builder.addRecord(); times = 1;
            builder.finish(); times = 0;
            reader.nextRecord(); times = 2;
            reader.setPage(page); times = 1;
            schema.visitColumns(withInstanceOf(ColumnVisitor.class)); times = 1;
        }};
    }

    @Test
    public void testFinish(final @Mocked PageReader reader, final @Mocked PageBuilder builder, final @Mocked Page page) throws Exception {
        new NonStrictExpectations() {{
            taskSource.loadTask(PluginTask.class); result = task;
            task.getDelimiter(); result = "";
        }};

        SpeedometerFilterPlugin plugin = new SpeedometerFilterPlugin();
        PageOutput output = plugin.open(taskSource, schema, schema, inPageOutput);
        output.finish();

        new Verifications() {{
            taskSource.loadTask(PluginTask.class); times = 1;
            builder.finish(); times = 1;
        }};
    }

    @Test
    public void testClose(final @Mocked PageReader reader, final @Mocked PageBuilder builder, final @Mocked Page page) throws Exception {
        new NonStrictExpectations() {{
            taskSource.loadTask(PluginTask.class); result = task;
            task.getDelimiter(); result = "";
        }};

        SpeedometerFilterPlugin plugin = new SpeedometerFilterPlugin();
        PageOutput output = plugin.open(taskSource, schema, schema, inPageOutput);
        output.close();

        new Verifications() {{
            taskSource.loadTask(PluginTask.class); times = 1;
            builder.close(); times = 1;
            reader.close(); times = 1;
        }};
    }
}
