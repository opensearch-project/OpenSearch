/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.coordination;

import org.opensearch.OpenSearchException;
import org.opensearch.cli.MockTerminal;
import org.opensearch.cli.UserException;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.test.OpenSearchIntegTestCase;

import picocli.CommandLine;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class RemoveSettingsCommandIT extends OpenSearchIntegTestCase {

    public void testRemoveSettingsAbortedByUser() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        String node = internalCluster().startNode();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder()
                    .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
                    .build()
            )
            .get();
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build()
        );
        expectThrows(
            () -> removeSettings(
                environment,
                true,
                new String[] { DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey() }
            ),
            OpenSearchNodeCommand.ABORTED_BY_USER_MSG
        );
    }

    public void testRemoveSettingsSuccessful() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        String node = internalCluster().startNode();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder()
                    .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
                    .build()
            )
            .get();
        assertThat(
            client().admin().cluster().prepareState().get().getState().metadata().persistentSettings().keySet(),
            contains(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey())
        );
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build()
        );
        MockTerminal terminal = removeSettings(
            environment,
            false,
            randomBoolean()
                ? new String[] { DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey() }
                : new String[] { "cluster.routing.allocation.disk.*" }
        );
        assertThat(terminal.getOutput(), containsString(RemoveSettingsCommand.SETTINGS_REMOVED_MSG));
        assertThat(terminal.getOutput(), containsString("The following settings will be removed:"));
        assertThat(
            terminal.getOutput(),
            containsString(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey() + ": " + false)
        );

        internalCluster().startNode(dataPathSettings);
        assertThat(
            client().admin().cluster().prepareState().get().getState().metadata().persistentSettings().keySet(),
            not(contains(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey()))
        );
    }

    public void testSettingDoesNotMatch() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        String node = internalCluster().startNode();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder()
                    .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
                    .build()
            )
            .get();
        assertThat(
            client().admin().cluster().prepareState().get().getState().metadata().persistentSettings().keySet(),
            contains(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey())
        );
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build()
        );
        UserException ex = expectThrows(
            UserException.class,
            () -> removeSettings(environment, false, new String[] { "cluster.routing.allocation.disk.bla.*" })
        );
        assertThat(
            ex.getMessage(),
            containsString("No persistent cluster settings matching [cluster.routing.allocation.disk.bla.*] were found on this node")
        );
    }

    private MockTerminal executeCommand(OpenSearchNodeCommand command, Environment environment, boolean abort, String... args)
        throws Exception {
        final MockTerminal terminal = new MockTerminal();

        new CommandLine(command).parseArgs(args);

        final String input;
        if (abort) {
            input = randomValueOtherThanMany(c -> c.equalsIgnoreCase("y"), () -> randomAlphaOfLength(1));
        } else {
            input = randomBoolean() ? "y" : "Y";
        }
        terminal.addTextInput(input);

        try {
            command.execute(terminal, environment);
        } finally {
            assertThat(terminal.getOutput(), containsString(OpenSearchNodeCommand.STOP_WARNING_MSG));
        }

        return terminal;
    }

    private MockTerminal removeSettings(Environment environment, boolean abort, String... args) throws Exception {
        final MockTerminal terminal = executeCommand(new RemoveSettingsCommand(), environment, abort, args);
        assertThat(terminal.getOutput(), containsString(RemoveSettingsCommand.CONFIRMATION_MSG));
        assertThat(terminal.getOutput(), containsString(RemoveSettingsCommand.SETTINGS_REMOVED_MSG));
        return terminal;
    }

    private void expectThrows(ThrowingRunnable runnable, String message) {
        OpenSearchException ex = expectThrows(OpenSearchException.class, runnable);
        assertThat(ex.getMessage(), containsString(message));
    }
}
