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

import joptsimple.OptionSet;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.cli.MockTerminal;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.gateway.GatewayMetaState;
import org.opensearch.gateway.PersistedClusterStateService;
import org.opensearch.indices.IndicesService;
import org.opensearch.node.Node.DiscoverySettings;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.gateway.DanglingIndicesState.AUTO_IMPORT_DANGLING_INDICES_SETTING;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;
import static org.opensearch.test.NodeRoles.nonClusterManagerNode;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class UnsafeBootstrapAndDetachCommandIT extends OpenSearchIntegTestCase {

    private MockTerminal executeCommand(
        OpenSearchNodeCommand command,
        Environment environment,
        int nodeOrdinal,
        boolean abort,
        Boolean applyClusterReadOnlyBlock
    ) throws Exception {
        final MockTerminal terminal = new MockTerminal();
        final OptionSet options;
        if (Objects.nonNull(applyClusterReadOnlyBlock)) {
            options = command.getParser()
                .parse(
                    "-ordinal",
                    Integer.toString(nodeOrdinal),
                    "-apply-cluster-read-only-block",
                    Boolean.toString(applyClusterReadOnlyBlock)
                );
        } else {
            options = command.getParser().parse("-ordinal", Integer.toString(nodeOrdinal));
        }
        final String input;

        if (abort) {
            input = randomValueOtherThanMany(c -> c.equalsIgnoreCase("y"), () -> randomAlphaOfLength(1));
        } else {
            input = randomBoolean() ? "y" : "Y";
        }

        terminal.addTextInput(input);

        try {
            command.execute(terminal, options, environment);
        } finally {
            assertThat(terminal.getOutput(), containsString(OpenSearchNodeCommand.STOP_WARNING_MSG));
        }

        return terminal;
    }

    private MockTerminal unsafeBootstrap(Environment environment, boolean abort, Boolean applyClusterReadOnlyBlock) throws Exception {
        final MockTerminal terminal = executeCommand(
            new UnsafeBootstrapClusterManagerCommand(),
            environment,
            0,
            abort,
            applyClusterReadOnlyBlock
        );
        assertThat(terminal.getOutput(), containsString(UnsafeBootstrapClusterManagerCommand.CONFIRMATION_MSG));
        assertThat(terminal.getOutput(), containsString(UnsafeBootstrapClusterManagerCommand.CLUSTER_MANAGER_NODE_BOOTSTRAPPED_MSG));
        return terminal;
    }

    private MockTerminal detachCluster(Environment environment, boolean abort) throws Exception {
        final MockTerminal terminal = executeCommand(new DetachClusterCommand(), environment, 0, abort, null);
        assertThat(terminal.getOutput(), containsString(DetachClusterCommand.CONFIRMATION_MSG));
        assertThat(terminal.getOutput(), containsString(DetachClusterCommand.NODE_DETACHED_MSG));
        return terminal;
    }

    private MockTerminal unsafeBootstrap(Environment environment) throws Exception {
        return unsafeBootstrap(environment, false, null);
    }

    private MockTerminal detachCluster(Environment environment) throws Exception {
        return detachCluster(environment, false);
    }

    private void expectThrows(ThrowingRunnable runnable, String message) {
        OpenSearchException ex = expectThrows(OpenSearchException.class, runnable);
        assertThat(ex.getMessage(), containsString(message));
    }

    private void ensureReadOnlyBlock(boolean expected_block_state, String node) {
        ClusterState state = internalCluster().client(node)
            .admin()
            .cluster()
            .prepareState()
            .setLocal(true)
            .execute()
            .actionGet()
            .getState();
        boolean current_block_state = state.metadata()
            .persistentSettings()
            .getAsBoolean(Metadata.SETTING_READ_ONLY_SETTING.getKey(), false);
        if (current_block_state != expected_block_state) {
            fail(
                String.format(
                    Locale.ROOT,
                    "Read only setting for the node %s don't match. Expected %s. Actual %s",
                    node,
                    expected_block_state,
                    current_block_state
                )
            );
        }
    }

    private void removeBlock() {
        if (isInternalCluster()) {
            Settings settings = Settings.builder().putNull(Metadata.SETTING_READ_ONLY_SETTING.getKey()).build();
            assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get());
        }
    }

    public void testBootstrapNotClusterManagerEligible() {
        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(nonClusterManagerNode(internalCluster().getDefaultSettings())).build()
        );
        expectThrows(() -> unsafeBootstrap(environment), UnsafeBootstrapClusterManagerCommand.NOT_CLUSTER_MANAGER_NODE_MSG);
    }

    public void testBootstrapNoDataFolder() {
        final Environment environment = TestEnvironment.newEnvironment(internalCluster().getDefaultSettings());
        expectThrows(() -> unsafeBootstrap(environment), OpenSearchNodeCommand.NO_NODE_FOLDER_FOUND_MSG);
    }

    public void testDetachNoDataFolder() {
        final Environment environment = TestEnvironment.newEnvironment(internalCluster().getDefaultSettings());
        expectThrows(() -> detachCluster(environment), OpenSearchNodeCommand.NO_NODE_FOLDER_FOUND_MSG);
    }

    public void testBootstrapNodeLocked() throws IOException {
        Settings envSettings = buildEnvSettings(Settings.EMPTY);
        Environment environment = TestEnvironment.newEnvironment(envSettings);
        try (NodeEnvironment ignored = new NodeEnvironment(envSettings, environment)) {
            expectThrows(() -> unsafeBootstrap(environment), OpenSearchNodeCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG);
        }
    }

    public void testDetachNodeLocked() throws IOException {
        Settings envSettings = buildEnvSettings(Settings.EMPTY);
        Environment environment = TestEnvironment.newEnvironment(envSettings);
        try (NodeEnvironment ignored = new NodeEnvironment(envSettings, environment)) {
            expectThrows(() -> detachCluster(environment), OpenSearchNodeCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG);
        }
    }

    public void testBootstrapNoNodeMetadata() {
        Settings envSettings = buildEnvSettings(Settings.EMPTY);
        Environment environment = TestEnvironment.newEnvironment(envSettings);
        expectThrows(() -> unsafeBootstrap(environment), OpenSearchNodeCommand.NO_NODE_FOLDER_FOUND_MSG);
    }

    public void testBootstrapNotBootstrappedCluster() throws Exception {
        String node = internalCluster().startNode(
            Settings.builder()
                .put(DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s") // to ensure quick node startup
                .build()
        );
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            assertTrue(state.blocks().hasGlobalBlockWithId(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID));
        });

        Settings dataPathSettings = internalCluster().dataPathSettings(node);

        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build()
        );
        expectThrows(() -> unsafeBootstrap(environment), UnsafeBootstrapClusterManagerCommand.EMPTY_LAST_COMMITTED_VOTING_CONFIG_MSG);
    }

    public void testBootstrapNoClusterState() throws IOException {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        String node = internalCluster().startNode();
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        NodeEnvironment nodeEnvironment = internalCluster().getClusterManagerNodeInstance(NodeEnvironment.class);
        internalCluster().stopRandomDataNode();
        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build()
        );
        PersistedClusterStateService.deleteAll(nodeEnvironment.nodeDataPaths());

        expectThrows(() -> unsafeBootstrap(environment), OpenSearchNodeCommand.NO_NODE_METADATA_FOUND_MSG);
    }

    public void testDetachNoClusterState() throws IOException {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        String node = internalCluster().startNode();
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        NodeEnvironment nodeEnvironment = internalCluster().getClusterManagerNodeInstance(NodeEnvironment.class);
        internalCluster().stopRandomDataNode();
        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build()
        );
        PersistedClusterStateService.deleteAll(nodeEnvironment.nodeDataPaths());

        expectThrows(() -> detachCluster(environment), OpenSearchNodeCommand.NO_NODE_METADATA_FOUND_MSG);
    }

    public void testBootstrapAbortedByUser() throws IOException {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        String node = internalCluster().startNode();
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build()
        );
        expectThrows(() -> unsafeBootstrap(environment, true, null), OpenSearchNodeCommand.ABORTED_BY_USER_MSG);
    }

    public void testDetachAbortedByUser() throws IOException {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        String node = internalCluster().startNode();
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build()
        );
        expectThrows(() -> detachCluster(environment, true), OpenSearchNodeCommand.ABORTED_BY_USER_MSG);
    }

    public void test3ClusterManagerNodes2Failed() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(2);
        List<String> clusterManagerNodes = new ArrayList<>();

        logger.info("--> start 1st cluster-manager-eligible node");
        clusterManagerNodes.add(
            internalCluster().startClusterManagerOnlyNode(
                Settings.builder().put(DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s").build()
            )
        ); // node ordinal 0

        logger.info("--> start one data-only node");
        String dataNode = internalCluster().startDataOnlyNode(
            Settings.builder().put(DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s").build()
        ); // node ordinal 1

        logger.info("--> start 2nd and 3rd cluster-manager-eligible nodes and bootstrap");
        clusterManagerNodes.addAll(internalCluster().startClusterManagerOnlyNodes(2)); // node ordinals 2 and 3

        logger.info("--> wait for all nodes to join the cluster");
        ensureStableCluster(4);

        List<String> currentClusterNodes = new ArrayList<>(clusterManagerNodes);
        currentClusterNodes.add(dataNode);
        currentClusterNodes.forEach(node -> ensureReadOnlyBlock(false, node));

        logger.info("--> create index test");
        createIndex("test");
        ensureGreen("test");

        Settings clusterManager1DataPathSettings = internalCluster().dataPathSettings(clusterManagerNodes.get(0));
        Settings clusterManager2DataPathSettings = internalCluster().dataPathSettings(clusterManagerNodes.get(1));
        Settings clusterManager3DataPathSettings = internalCluster().dataPathSettings(clusterManagerNodes.get(2));
        Settings dataNodeDataPathSettings = internalCluster().dataPathSettings(dataNode);

        logger.info("--> stop 2nd and 3d cluster-manager eligible node");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(clusterManagerNodes.get(1)));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(clusterManagerNodes.get(2)));

        logger.info("--> ensure NO_MASTER_BLOCK on data-only node");
        assertBusy(() -> {
            ClusterState state = internalCluster().client(dataNode)
                .admin()
                .cluster()
                .prepareState()
                .setLocal(true)
                .execute()
                .actionGet()
                .getState();
            assertTrue(state.blocks().hasGlobalBlockWithId(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID));
        });

        logger.info("--> try to unsafely bootstrap 1st cluster-manager-eligible node, while node lock is held");
        Environment environmentClusterManager1 = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(clusterManager1DataPathSettings).build()
        );
        expectThrows(
            () -> unsafeBootstrap(environmentClusterManager1),
            UnsafeBootstrapClusterManagerCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG
        );

        logger.info("--> stop 1st cluster-manager-eligible node and data-only node");
        NodeEnvironment nodeEnvironment = internalCluster().getClusterManagerNodeInstance(NodeEnvironment.class);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(clusterManagerNodes.get(0)));
        assertBusy(() -> internalCluster().getInstance(GatewayMetaState.class, dataNode).allPendingAsyncStatesWritten());
        internalCluster().stopRandomDataNode();

        logger.info("--> unsafely-bootstrap 1st cluster-manager-eligible node");
        MockTerminal terminal = unsafeBootstrap(environmentClusterManager1, false, true);
        Metadata metadata = OpenSearchNodeCommand.createPersistedClusterStateService(Settings.EMPTY, nodeEnvironment.nodeDataPaths())
            .loadBestOnDiskState().metadata;
        assertThat(
            terminal.getOutput(),
            containsString(
                String.format(
                    Locale.ROOT,
                    UnsafeBootstrapClusterManagerCommand.CLUSTER_STATE_TERM_VERSION_MSG_FORMAT,
                    metadata.coordinationMetadata().term(),
                    metadata.version()
                )
            )
        );

        logger.info("--> start 1st cluster-manager-eligible node");
        String clusterManagerNode2 = internalCluster().startClusterManagerOnlyNode(clusterManager1DataPathSettings);

        logger.info("--> detach-cluster on data-only node");
        Environment environmentData = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataNodeDataPathSettings).build()
        );
        detachCluster(environmentData, false);

        logger.info("--> start data-only node");
        String dataNode2 = internalCluster().startDataOnlyNode(dataNodeDataPathSettings);

        logger.info("--> ensure there is no NO_MASTER_BLOCK and unsafe-bootstrap is reflected in cluster state");
        assertBusy(() -> {
            ClusterState state = internalCluster().client(dataNode2)
                .admin()
                .cluster()
                .prepareState()
                .setLocal(true)
                .execute()
                .actionGet()
                .getState();
            assertFalse(state.blocks().hasGlobalBlockWithId(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID));
            assertTrue(
                state.metadata().persistentSettings().getAsBoolean(UnsafeBootstrapClusterManagerCommand.UNSAFE_BOOTSTRAP.getKey(), false)
            );
        });

        List<String> bootstrappedNodes = new ArrayList<>();
        bootstrappedNodes.add(dataNode2);
        bootstrappedNodes.add(clusterManagerNode2);
        bootstrappedNodes.forEach(node -> ensureReadOnlyBlock(true, node));

        logger.info("--> ensure index test is green");
        ensureGreen("test");
        IndexMetadata indexMetadata = clusterService().state().metadata().index("test");
        assertThat(indexMetadata.getSettings().get(IndexMetadata.SETTING_HISTORY_UUID), notNullValue());

        logger.info("--> detach-cluster on 2nd and 3rd cluster-manager-eligible nodes");
        Environment environmentClusterManager2 = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(clusterManager2DataPathSettings).build()
        );
        detachCluster(environmentClusterManager2, false);
        Environment environmentClusterManager3 = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(clusterManager3DataPathSettings).build()
        );
        detachCluster(environmentClusterManager3, false);

        logger.info("--> start 2nd and 3rd cluster-manager-eligible nodes and ensure 4 nodes stable cluster");
        bootstrappedNodes.add(internalCluster().startClusterManagerOnlyNode(clusterManager2DataPathSettings));
        bootstrappedNodes.add(internalCluster().startClusterManagerOnlyNode(clusterManager3DataPathSettings));
        ensureStableCluster(4);
        bootstrappedNodes.forEach(node -> ensureReadOnlyBlock(true, node));
        removeBlock();
    }

    public void testAllClusterManagerEligibleNodesFailedDanglingIndexImport() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);

        Settings settings = Settings.builder().put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), true).build();

        logger.info("--> start mixed data and cluster-manager-eligible node and bootstrap cluster");
        String clusterManagerNode = internalCluster().startNode(settings); // node ordinal 0

        logger.info("--> start data-only node and ensure 2 nodes stable cluster");
        String dataNode = internalCluster().startDataOnlyNode(settings); // node ordinal 1
        ensureStableCluster(2);

        logger.info("--> index 1 doc and ensure index is green");
        client().prepareIndex("test").setId("1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();
        ensureGreen("test");
        assertBusy(
            () -> internalCluster().getInstances(IndicesService.class)
                .forEach(indicesService -> assertTrue(indicesService.allPendingDanglingIndicesWritten()))
        );

        logger.info("--> verify 1 doc in the index");
        assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 1L);
        assertThat(client().prepareGet("test", "1").execute().actionGet().isExists(), equalTo(true));

        logger.info("--> stop data-only node and detach it from the old cluster");
        Settings dataNodeDataPathSettings = Settings.builder()
            .put(internalCluster().dataPathSettings(dataNode), true)
            .put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), true)
            .build();
        assertBusy(() -> internalCluster().getInstance(GatewayMetaState.class, dataNode).allPendingAsyncStatesWritten());
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataNode));
        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder()
                .put(internalCluster().getDefaultSettings())
                .put(dataNodeDataPathSettings)
                .put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), true)
                .build()
        );
        detachCluster(environment, false);

        logger.info("--> stop cluster-manager-eligible node, clear its data and start it again - new cluster should form");
        internalCluster().restartNode(clusterManagerNode, new InternalTestCluster.RestartCallback() {
            @Override
            public boolean clearData(String nodeName) {
                return true;
            }
        });

        logger.info("--> start data-only only node and ensure 2 nodes stable cluster");
        internalCluster().startDataOnlyNode(dataNodeDataPathSettings);
        ensureStableCluster(2);

        logger.info("--> verify that the dangling index exists and has green status");
        assertBusy(() -> { assertThat(client().admin().indices().prepareExists("test").execute().actionGet().isExists(), equalTo(true)); });
        ensureGreen("test");

        logger.info("--> verify the doc is there");
        assertThat(client().prepareGet("test", "1").execute().actionGet().isExists(), equalTo(true));
    }

    public void testNoInitialBootstrapAfterDetach() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        Settings clusterManagerNodeDataPathSettings = internalCluster().dataPathSettings(clusterManagerNode);
        internalCluster().stopCurrentClusterManagerNode();

        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(clusterManagerNodeDataPathSettings).build()
        );
        detachCluster(environment);

        String node = internalCluster().startClusterManagerOnlyNode(
            Settings.builder()
                // give the cluster 2 seconds to elect the cluster-manager (it should not)
                .put(DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "2s")
                .put(clusterManagerNodeDataPathSettings)
                .build()
        );

        ClusterState state = internalCluster().client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertTrue(state.blocks().hasGlobalBlockWithId(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID));

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node));
    }

    public void testCanRunUnsafeBootstrapAfterErroneousDetachWithoutLoosingMetadata() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        Settings clusterManagerNodeDataPathSettings = internalCluster().dataPathSettings(clusterManagerNode);
        ClusterUpdateSettingsRequest req = new ClusterUpdateSettingsRequest().persistentSettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "1234kb")
        );
        internalCluster().client().admin().cluster().updateSettings(req).get();

        ClusterState state = internalCluster().client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.metadata().persistentSettings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey()), equalTo("1234kb"));

        ensureReadOnlyBlock(false, clusterManagerNode);

        internalCluster().stopCurrentClusterManagerNode();

        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(clusterManagerNodeDataPathSettings).build()
        );
        detachCluster(environment);
        unsafeBootstrap(environment); // read-only block will remain same as one before bootstrap, in this case it is false

        String clusterManagerNode2 = internalCluster().startClusterManagerOnlyNode(clusterManagerNodeDataPathSettings);
        ensureGreen();
        ensureReadOnlyBlock(false, clusterManagerNode2);

        state = internalCluster().client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.metadata().settings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey()), equalTo("1234kb"));
    }

    public void testUnsafeBootstrapWithApplyClusterReadOnlyBlockAsFalse() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        Settings clusterManagerNodeDataPathSettings = internalCluster().dataPathSettings(clusterManagerNode);
        ClusterUpdateSettingsRequest req = new ClusterUpdateSettingsRequest().persistentSettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "1234kb")
        );
        internalCluster().client().admin().cluster().updateSettings(req).get();

        ClusterState state = internalCluster().client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.metadata().persistentSettings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey()), equalTo("1234kb"));

        ensureReadOnlyBlock(false, clusterManagerNode);

        internalCluster().stopCurrentClusterManagerNode();

        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(clusterManagerNodeDataPathSettings).build()
        );
        unsafeBootstrap(environment, false, false);

        String clusterManagerNode2 = internalCluster().startClusterManagerOnlyNode(clusterManagerNodeDataPathSettings);
        ensureGreen();
        ensureReadOnlyBlock(false, clusterManagerNode2);

        state = internalCluster().client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.metadata().settings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey()), equalTo("1234kb"));
    }
}
