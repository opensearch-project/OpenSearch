/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.upgrade;

import org.opensearch.cli.MockTerminal;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.mockito.Mockito;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class InstallPluginsTaskTests extends OpenSearchTestCase {

    private final MockTerminal terminal = new MockTerminal();
    private InstallPluginsTask task;
    private Environment env;

    private static final String OFFICIAL_PLUGIN = "analysis-icu";
    private static final String CUSTOM_PLUGIN = "job-scheduler";

    @Before
    public void setUpTask() throws IOException {
        task = new InstallPluginsTask();
        env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", "").build());
    }

    public void testInstallPluginsTaskWithOfficialPlugin() throws IOException {
        InstallPluginsTask spyTask = spy(task);
        TaskInput taskInput = createTaskInputWithPlugin(OFFICIAL_PLUGIN);
        spyTask.accept(new Tuple<>(taskInput, terminal));

        verify(spyTask, Mockito.atLeast(1)).executeInstallPluginCommand(OFFICIAL_PLUGIN, taskInput, terminal);
    }

    public void testInstallPluginsTaskWithCustomPlugin() throws IOException {
        TaskInput taskInput = createTaskInputWithPlugin(CUSTOM_PLUGIN);
        task.accept(new Tuple<>(taskInput, terminal));

        assertThat(terminal.getOutput(), containsString("Please install the following custom plugins manually"));
    }

    public void testGetCommandsBasedOnOS() {
        TaskInput taskInput = createTaskInputWithPlugin(OFFICIAL_PLUGIN);
        List<String> commandsList = task.getProcessBuilderBasedOnOS(OFFICIAL_PLUGIN, taskInput).command();

        final String os = System.getProperty("os.name", "");
        if (os.startsWith("Windows")) {
            assertEquals("cmd.exe", commandsList.get(0));
        } else {
            assertEquals("sh", commandsList.get(0));
        }
    }

    private TaskInput createTaskInputWithPlugin(String plugin) {
        TaskInput taskInput = new TaskInput(env);
        List<String> pluginsList = new ArrayList<>();
        pluginsList.add(plugin);
        taskInput.setPlugins(pluginsList);
        return taskInput;
    }
}
