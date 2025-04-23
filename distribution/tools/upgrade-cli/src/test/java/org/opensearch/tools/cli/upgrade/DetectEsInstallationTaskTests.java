/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.upgrade;

import org.opensearch.cli.MockTerminal;
import org.opensearch.cli.Terminal;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.File;
import java.nio.file.Path;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class DetectEsInstallationTaskTests extends OpenSearchTestCase {

    private final MockTerminal terminal = new MockTerminal();
    private DetectEsInstallationTask task;
    private Environment env;

    @Before
    public void setUpTask() {
        task = new DetectEsInstallationTask();
        env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", "").build());
    }

    @SuppressForbidden(reason = "Read config directory from test resources.")
    public void testTaskExecution() throws Exception {
        Path esConfig = new File(getClass().getResource("/config").getPath()).toPath();
        // path for es_home
        terminal.addTextInput(esConfig.getParent().toString());
        // path for es_config
        terminal.addTextInput(esConfig.toString());
        TaskInput taskInput = new TaskInput(env);
        Tuple<TaskInput, Terminal> input = new Tuple<>(taskInput, terminal);

        task.accept(input);

        assertThat(taskInput.getEsConfig(), is(esConfig));
        assertThat(taskInput.getBaseUrl(), is("http://localhost:42123"));
        assertThat(taskInput.getPlugins(), hasSize(0));
        assertThat(taskInput.getNode(), is("node-x"));
        assertThat(taskInput.getCluster(), is("my-cluster"));
    }

    public void testRetrieveUrlFromSettings() {
        Settings esSettings = Settings.builder().put("http.port", "9201").build();

        assertThat(task.retrieveUrl(esSettings), is("http://localhost:9201"));
    }

    public void testRetrieveDefaultUrlFromConfig() {
        assertThat(task.retrieveUrl(Settings.EMPTY), is("http://localhost:9200"));
    }
}
