/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.upgrade;

import org.opensearch.LegacyESVersion;
import org.opensearch.cli.MockTerminal;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class ValidateInputTaskTests extends OpenSearchTestCase {

    private ValidateInputTask task;
    private MockTerminal terminal;
    private Environment env;

    @Before
    public void setTask() {
        task = new ValidateInputTask();
        terminal = new MockTerminal();
        env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", "test_home").build());
    }

    public void testUnsupportedEsVersion() {
        TaskInput taskInput = new TaskInput(env);
        taskInput.setVersion(LegacyESVersion.fromId(7100199));

        final RuntimeException e = expectThrows(RuntimeException.class, () -> task.accept(new Tuple<>(taskInput, terminal)));

        assertTrue(e.getMessage(), e.getMessage().contains("The installed version 7.10.1 of elasticsearch is not supported."));
    }

    public void testGetSummaryFields() {
        TaskInput taskInput = new TaskInput(env);
        taskInput.setEsConfig(PathUtils.get("es_home"));
        taskInput.setCluster("some-cluster");
        taskInput.setNode("some-node");
        taskInput.setVersion(LegacyESVersion.fromId(7100299));
        taskInput.setBaseUrl("some-url");
        taskInput.setPlugins(Arrays.asList("plugin-1", "plugin-2"));

        Map<String, String> summary = task.getSummaryFieldsMap(taskInput);

        assertThat(summary.entrySet(), hasSize(7));
        assertThat(summary.get("Cluster"), is("some-cluster"));
        assertThat(summary.get("Node"), is("some-node"));
        assertThat(summary.get("Endpoint"), is("some-url"));
        assertThat(summary.get("Elasticsearch Version"), is("7.10.2"));
        assertThat(summary.get("Elasticsearch Plugins"), is("[plugin-1, plugin-2]"));
        assertThat(summary.get("Elasticsearch Config"), is("es_home"));
        assertThat(summary.get("OpenSearch Config"), is(env.configDir().toString()));
    }
}
