package org.embulk.filter;

import mockit.Expectations;
import mockit.Mocked;
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

    @Mocked
    PageReader reader;

    @Mocked
    PageBuilder builder;

    @Mocked
    Page page;

    @Test
    public void testTransaction() {
        SpeedometerFilterPlugin plugin = new SpeedometerFilterPlugin();
        new Expectations(plugin) {{
            plugin.getTask(config); result = task;
        }};

        plugin.transaction(config, schema, control);

        new Verifications() {{
            control.run((TaskSource)any, schema); times = 1;
        }};
    }

    @Test
    public void testOpen() throws Exception {
        SpeedometerFilterPlugin plugin = new SpeedometerFilterPlugin();
        new Expectations(plugin) {{
            plugin.getTask(taskSource); result = task;
            task.getDelimiter(); result = "";
            reader.nextRecord(); result = true; result = false;
            Exec.getPageReader(schema); result = reader;
        }};

        PageOutput output = plugin.open(taskSource, schema, schema, inPageOutput);
        output.add(page);

        new Verifications() {{
            builder.addRecord(); times = 1;
            builder.finish(); times = 0;
            reader.nextRecord(); times = 2;
            reader.setPage(page); times = 1;
            schema.visitColumns(withInstanceOf(ColumnVisitor.class)); times = 1;
        }};
    }

    @Test
    public void testFinish() throws Exception {
        SpeedometerFilterPlugin plugin = new SpeedometerFilterPlugin();
        new Expectations(plugin) {{
            plugin.getTask(taskSource); result = task;
            task.getDelimiter(); result = "";
        }};

        PageOutput output = plugin.open(taskSource, schema, schema, inPageOutput);
        output.finish();

        new Verifications() {{
            builder.finish(); times = 1;
        }};
    }

    @Test
    public void testClose() throws Exception {
        SpeedometerFilterPlugin plugin = new SpeedometerFilterPlugin();
        new Expectations(plugin) {{
            plugin.getTask(taskSource); result = task;
            task.getDelimiter(); result = "";
        }};

        PageOutput output = plugin.open(taskSource, schema, schema, inPageOutput);
        output.close();

        new Verifications() {{
            builder.close(); times = 1;
        }};
    }
}
