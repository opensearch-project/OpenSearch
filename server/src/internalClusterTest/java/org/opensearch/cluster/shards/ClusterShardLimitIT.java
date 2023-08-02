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

package org.opensearch.cluster.shards;

import org.opensearch.Version;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.Priority;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.MockHttpTransport;
import org.opensearch.test.NodeConfigurationSource;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.nio.MockNioTransportPlugin;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.opensearch.indices.ShardLimitValidator.SETTING_MAX_SHARDS_PER_CLUSTER_KEY;
import static org.opensearch.test.NodeRoles.dataNode;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class ClusterShardLimitIT extends OpenSearchIntegTestCase {
    private static final String shardsPerNodeKey = SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey();
    private static final String ignoreDotIndexKey = ShardLimitValidator.SETTING_CLUSTER_IGNORE_DOT_INDEXES.getKey();

    public void testSettingClusterMaxShards() {
        int shardsPerNode = between(1, 500_000);
        setMaxShardLimit(shardsPerNode, shardsPerNodeKey);
    }

    public void testSettingIgnoreDotIndexes() {
        boolean ignoreDotIndexes = randomBoolean();
        setIgnoreDotIndex(ignoreDotIndexes);
    }

    public void testMinimumPerNode() {
        int negativeShardsPerNode = between(-50_000, 0);
        try {
            if (frequently()) {
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().put(shardsPerNodeKey, negativeShardsPerNode).build())
                    .get();
            } else {
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put(shardsPerNodeKey, negativeShardsPerNode).build())
                    .get();
            }
            fail("should not be able to set negative shards per node");
        } catch (IllegalArgumentException ex) {
            assertEquals(
                "Failed to parse value [" + negativeShardsPerNode + "] for setting [cluster.max_shards_per_node] must be >= 1",
                ex.getMessage()
            );
        }
    }

    public void testIndexCreationOverLimit() {
        int dataNodes = client().admin().cluster().prepareState().get().getState().getNodes().getDataNodes().size();

        ShardCounts counts = ShardCounts.forDataNodeCount(dataNodes);

        setMaxShardLimit(counts.getShardsPerNode(), shardsPerNodeKey);
        // Create an index that will bring us up to the limit
        createIndex(
            "test",
            Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, counts.getFirstIndexShards())
                .put(SETTING_NUMBER_OF_REPLICAS, counts.getFirstIndexReplicas())
                .build()
        );

        try {
            prepareCreate(
                "should-fail",
                Settings.builder()
                    .put(indexSettings())
                    .put(SETTING_NUMBER_OF_SHARDS, counts.getFailingIndexShards())
                    .put(SETTING_NUMBER_OF_REPLICAS, counts.getFailingIndexReplicas())
            ).get();
            fail("Should not have been able to go over the limit");
        } catch (IllegalArgumentException e) {
            verifyException(dataNodes, counts, e);
        }
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        assertFalse(clusterState.getMetadata().hasIndex("should-fail"));
    }

    /**
     * The test checks if the index starting with the dot can be created if the node has
     * number of shards equivalent to the cluster.max_shards_per_node and the cluster.ignore_Dot_indexes
     * setting is set to true. If the cluster.ignore_Dot_indexes is set to true index creation of
     * indexes starting with dot would succeed.
     */
    public void testIndexCreationOverLimitForDotIndexesSucceeds() {
        int dataNodes = client().admin().cluster().prepareState().get().getState().getNodes().getDataNodes().size();

        // Setting the cluster.max_shards_per_node setting according to the data node count.
        setMaxShardLimit(dataNodes, shardsPerNodeKey);
        setIgnoreDotIndex(true);

        /*
            Create an index that will bring us up to the limit. It would create index with primary equal to the
            dataNodes * dataNodes so that cluster.max_shards_per_node setting is reached.
         */
        createIndex(
            "test",
            Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, dataNodes * dataNodes)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );

        // Getting total active shards in the cluster.
        int currentActiveShards = client().admin().cluster().prepareHealth().get().getActiveShards();

        // Getting cluster.max_shards_per_node setting
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        String maxShardsPerNode = clusterState.getMetadata().settings().get(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey());

        // Checking if the total shards created are equivalent to dataNodes * cluster.max_shards_per_node
        assertEquals(dataNodes * Integer.parseInt(maxShardsPerNode), currentActiveShards);

        createIndex(
            ".test-index",
            Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0).build()
        );

        clusterState = client().admin().cluster().prepareState().get().getState();
        assertTrue(clusterState.getMetadata().hasIndex(".test-index"));
    }

    /**
     * The test checks if the index starting with the dot should not be created if the node has
     * number of shards equivalent to the cluster.max_shards_per_node and the cluster.ignore_Dot_indexes
     * setting is set to false. If the cluster.ignore_Dot_indexes is set to false index creation of
     * indexes starting with dot would fail as well.
     */
    public void testIndexCreationOverLimitForDotIndexesFail() {
        int dataNodes = client().admin().cluster().prepareState().get().getState().getNodes().getDataNodes().size();
        int maxAllowedShards = dataNodes * dataNodes;

        // Setting the cluster.max_shards_per_node setting according to the data node count.
        setMaxShardLimit(dataNodes, shardsPerNodeKey);

        /*
            Create an index that will bring us up to the limit. It would create index with primary equal to the
            dataNodes * dataNodes so that cluster.max_shards_per_node setting is reached.
         */
        createIndex(
            "test",
            Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, maxAllowedShards)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );

        // Getting total active shards in the cluster.
        int currentActiveShards = client().admin().cluster().prepareHealth().get().getActiveShards();

        // Getting cluster.max_shards_per_node setting
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        String maxShardsPerNode = clusterState.getMetadata().settings().get(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey());

        // Checking if the total shards created are equivalent to dataNodes * cluster.max_shards_per_node
        assertEquals(dataNodes * Integer.parseInt(maxShardsPerNode), currentActiveShards);

        int extraShardCount = 1;
        try {
            createIndex(
                ".test-index",
                Settings.builder()
                    .put(indexSettings())
                    .put(SETTING_NUMBER_OF_SHARDS, extraShardCount)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .build()
            );
        } catch (IllegalArgumentException e) {
            verifyException(maxAllowedShards, currentActiveShards, extraShardCount, e);
        }
        clusterState = client().admin().cluster().prepareState().get().getState();
        assertFalse(clusterState.getMetadata().hasIndex(".test-index"));
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/6287")
    public void testCreateIndexWithMaxClusterShardSetting() {
        int dataNodes = client().admin().cluster().prepareState().get().getState().getNodes().getDataNodes().size();
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        setMaxShardLimit(dataNodes, shardsPerNodeKey);

        int maxAllowedShards = dataNodes + 1;
        int extraShardCount = maxAllowedShards + 1;
        // Getting total active shards in the cluster.
        int currentActiveShards = client().admin().cluster().prepareHealth().get().getActiveShards();
        try {
            setMaxShardLimit(maxAllowedShards, SETTING_MAX_SHARDS_PER_CLUSTER_KEY);
            prepareCreate("test_index_with_cluster_shard_limit").setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, extraShardCount).put(SETTING_NUMBER_OF_REPLICAS, 0).build()
            ).get();
        } catch (final IllegalArgumentException ex) {
            verifyException(Math.min(maxAllowedShards, dataNodes * dataNodes), currentActiveShards, extraShardCount, ex);
        } finally {
            setMaxShardLimit(-1, SETTING_MAX_SHARDS_PER_CLUSTER_KEY);
        }
    }

    /**
     * The test checks if the index starting with the .ds- can be created if the node has
     * number of shards equivalent to the cluster.max_shards_per_node and the cluster.ignore_Dot_indexes
     * setting is set to true. If the cluster.ignore_Dot_indexes is set to true index creation of
     * indexes starting with dot would only succeed and dataStream indexes would still have validation applied.
     */
    public void testIndexCreationOverLimitForDataStreamIndexes() {
        int dataNodes = client().admin().cluster().prepareState().get().getState().getNodes().getDataNodes().size();
        int maxAllowedShards = dataNodes * dataNodes;

        // Setting the cluster.max_shards_per_node setting according to the data node count.
        setMaxShardLimit(dataNodes, shardsPerNodeKey);
        setIgnoreDotIndex(true);

        /*
            Create an index that will bring us up to the limit. It would create index with primary equal to the
            dataNodes * dataNodes so that cluster.max_shards_per_node setting is reached.
         */
        createIndex(
            "test",
            Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, maxAllowedShards)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );

        // Getting total active shards in the cluster.
        int currentActiveShards = client().admin().cluster().prepareHealth().get().getActiveShards();

        // Getting cluster.max_shards_per_node setting
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        String maxShardsPerNode = clusterState.getMetadata().settings().get(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey());

        // Checking if the total shards created are equivalent to dataNodes * cluster.max_shards_per_node
        assertEquals(dataNodes * Integer.parseInt(maxShardsPerNode), currentActiveShards);

        int extraShardCount = 1;
        try {
            createIndex(
                ".ds-test-index",
                Settings.builder()
                    .put(indexSettings())
                    .put(SETTING_NUMBER_OF_SHARDS, extraShardCount)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .build()
            );
        } catch (IllegalArgumentException e) {
            verifyException(maxAllowedShards, currentActiveShards, extraShardCount, e);
        }
        clusterState = client().admin().cluster().prepareState().get().getState();
        assertFalse(clusterState.getMetadata().hasIndex(".ds-test-index"));
    }

    public void testIndexCreationOverLimitFromTemplate() {
        int dataNodes = client().admin().cluster().prepareState().get().getState().getNodes().getDataNodes().size();

        final ShardCounts counts = ShardCounts.forDataNodeCount(dataNodes);

        setMaxShardLimit(counts.getShardsPerNode(), shardsPerNodeKey);

        if (counts.getFirstIndexShards() > 0) {
            createIndex(
                "test",
                Settings.builder()
                    .put(indexSettings())
                    .put(SETTING_NUMBER_OF_SHARDS, counts.getFirstIndexShards())
                    .put(SETTING_NUMBER_OF_REPLICAS, counts.getFirstIndexReplicas())
                    .build()
            );
        }

        assertAcked(
            client().admin()
                .indices()
                .preparePutTemplate("should-fail")
                .setPatterns(Collections.singletonList("should-fail"))
                .setOrder(1)
                .setSettings(
                    Settings.builder()
                        .put(SETTING_NUMBER_OF_SHARDS, counts.getFailingIndexShards())
                        .put(SETTING_NUMBER_OF_REPLICAS, counts.getFailingIndexReplicas())
                )
                .get()
        );

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareCreate("should-fail").get()
        );
        verifyException(dataNodes, counts, e);
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        assertFalse(clusterState.getMetadata().hasIndex("should-fail"));
    }

    public void testIncreaseReplicasOverLimit() {
        int dataNodes = client().admin().cluster().prepareState().get().getState().getNodes().getDataNodes().size();

        dataNodes = ensureMultipleDataNodes(dataNodes);

        int firstShardCount = between(2, 10);
        int shardsPerNode = firstShardCount - 1;
        setMaxShardLimit(shardsPerNode, shardsPerNodeKey);

        prepareCreate(
            "growing-should-fail",
            Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_SHARDS, firstShardCount).put(SETTING_NUMBER_OF_REPLICAS, 0)
        ).get();

        try {
            client().admin()
                .indices()
                .prepareUpdateSettings("growing-should-fail")
                .setSettings(Settings.builder().put("number_of_replicas", dataNodes))
                .get();
            fail("shouldn't be able to increase the number of replicas");
        } catch (IllegalArgumentException e) {
            String expectedError = "Validation Failed: 1: this action would add ["
                + (dataNodes * firstShardCount)
                + "] total shards, but this cluster currently has ["
                + firstShardCount
                + "]/["
                + dataNodes * shardsPerNode
                + "] maximum shards open;";
            assertEquals(expectedError, e.getMessage());
        }
        Metadata clusterState = client().admin().cluster().prepareState().get().getState().metadata();
        assertEquals(0, clusterState.index("growing-should-fail").getNumberOfReplicas());
    }

    public void testChangingMultipleIndicesOverLimit() {
        int dataNodes = client().admin().cluster().prepareState().get().getState().getNodes().getDataNodes().size();

        dataNodes = ensureMultipleDataNodes(dataNodes);

        // Create two indexes: One that ends up with fewer shards, and one
        // that ends up with more to verify that we check the _total_ number of
        // shards the operation would add.

        int firstIndexFactor = between(5, 10);
        int firstIndexShards = firstIndexFactor * dataNodes;
        int firstIndexReplicas = 0;

        int secondIndexFactor = between(1, 3);
        int secondIndexShards = secondIndexFactor * dataNodes;
        int secondIndexReplicas = dataNodes;

        int shardsPerNode = firstIndexFactor + (secondIndexFactor * (1 + secondIndexReplicas));
        setMaxShardLimit(shardsPerNode, shardsPerNodeKey);

        createIndex(
            "test-1-index",
            Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, firstIndexShards)
                .put(SETTING_NUMBER_OF_REPLICAS, firstIndexReplicas)
                .build()
        );
        createIndex(
            "test-2-index",
            Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, secondIndexShards)
                .put(SETTING_NUMBER_OF_REPLICAS, secondIndexReplicas)
                .build()
        );
        try {
            client().admin()
                .indices()
                .prepareUpdateSettings(randomFrom("_all", "test-*", "*-index"))
                .setSettings(Settings.builder().put("number_of_replicas", dataNodes - 1))
                .get();
            fail("should not have been able to increase shards above limit");
        } catch (IllegalArgumentException e) {
            int totalShardsBefore = (firstIndexShards * (1 + firstIndexReplicas)) + (secondIndexShards * (1 + secondIndexReplicas));
            int totalShardsAfter = (dataNodes) * (firstIndexShards + secondIndexShards);
            int difference = totalShardsAfter - totalShardsBefore;

            String expectedError = "Validation Failed: 1: this action would add ["
                + difference
                + "] total shards, but this cluster currently has ["
                + totalShardsBefore
                + "]/["
                + dataNodes * shardsPerNode
                + "] maximum shards open;";
            assertEquals(expectedError, e.getMessage());
        }
        Metadata clusterState = client().admin().cluster().prepareState().get().getState().metadata();
        assertEquals(firstIndexReplicas, clusterState.index("test-1-index").getNumberOfReplicas());
        assertEquals(secondIndexReplicas, clusterState.index("test-2-index").getNumberOfReplicas());
    }

    public void testPreserveExistingSkipsCheck() {
        int dataNodes = client().admin().cluster().prepareState().get().getState().getNodes().getDataNodes().size();

        dataNodes = ensureMultipleDataNodes(dataNodes);

        int firstShardCount = between(2, 10);
        int shardsPerNode = firstShardCount - 1;
        setMaxShardLimit(shardsPerNode, shardsPerNodeKey);

        prepareCreate(
            "test-index",
            Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_SHARDS, firstShardCount).put(SETTING_NUMBER_OF_REPLICAS, 0)
        ).get();

        // Since a request with preserve_existing can't change the number of
        // replicas, we should never get an error here.
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("test-index")
                .setPreserveExisting(true)
                .setSettings(Settings.builder().put("number_of_replicas", dataNodes))
                .get()
        );
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        assertEquals(0, clusterState.getMetadata().index("test-index").getNumberOfReplicas());
    }

    public void testRestoreSnapshotOverLimit() {
        Client client = client();

        logger.info("-->  creating repository");
        Settings.Builder repoSettings = Settings.builder();
        repoSettings.put("location", randomRepoPath());
        repoSettings.put("compress", randomBoolean());
        repoSettings.put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES);

        assertAcked(client.admin().cluster().preparePutRepository("test-repo").setType("fs").setSettings(repoSettings.build()));

        int dataNodes = client().admin().cluster().prepareState().get().getState().getNodes().getDataNodes().size();
        ShardCounts counts = ShardCounts.forDataNodeCount(dataNodes);
        createIndex(
            "snapshot-index",
            Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, counts.getFailingIndexShards())
                .put(SETTING_NUMBER_OF_REPLICAS, counts.getFailingIndexReplicas())
                .build()
        );
        ensureGreen();

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("snapshot-index")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        List<SnapshotInfo> snapshotInfos = client.admin()
            .cluster()
            .prepareGetSnapshots("test-repo")
            .setSnapshots("test-snap")
            .get()
            .getSnapshots();
        assertThat(snapshotInfos.size(), equalTo(1));
        SnapshotInfo snapshotInfo = snapshotInfos.get(0);
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.version(), equalTo(Version.CURRENT));

        // Test restore after index deletion
        logger.info("--> delete indices");
        cluster().wipeIndices("snapshot-index");

        // Reduce the shard limit and fill it up
        setMaxShardLimit(counts.getShardsPerNode(), shardsPerNodeKey);
        createIndex(
            "test-fill",
            Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, counts.getFirstIndexShards())
                .put(SETTING_NUMBER_OF_REPLICAS, counts.getFirstIndexReplicas())
                .build()
        );

        logger.info("--> restore one index after deletion");
        try {
            RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
                .cluster()
                .prepareRestoreSnapshot("test-repo", "test-snap")
                .setWaitForCompletion(true)
                .setIndices("snapshot-index")
                .execute()
                .actionGet();
            fail("Should not have been able to restore snapshot in full cluster");
        } catch (IllegalArgumentException e) {
            verifyException(dataNodes, counts, e);
        }
        ensureGreen();
        ClusterState clusterState = client.admin().cluster().prepareState().get().getState();
        assertFalse(clusterState.getMetadata().hasIndex("snapshot-index"));
    }

    public void testOpenIndexOverLimit() {
        Client client = client();
        int dataNodes = client().admin().cluster().prepareState().get().getState().getNodes().getDataNodes().size();
        ShardCounts counts = ShardCounts.forDataNodeCount(dataNodes);

        createIndex(
            "test-index-1",
            Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, counts.getFailingIndexShards())
                .put(SETTING_NUMBER_OF_REPLICAS, counts.getFailingIndexReplicas())
                .build()
        );

        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertFalse(healthResponse.isTimedOut());

        AcknowledgedResponse closeIndexResponse = client.admin().indices().prepareClose("test-index-1").execute().actionGet();
        assertTrue(closeIndexResponse.isAcknowledged());

        // Fill up the cluster
        setMaxShardLimit(counts.getShardsPerNode(), shardsPerNodeKey);
        createIndex(
            "test-fill",
            Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, counts.getFirstIndexShards())
                .put(SETTING_NUMBER_OF_REPLICAS, counts.getFirstIndexReplicas())
                .build()
        );

        try {
            client.admin().indices().prepareOpen("test-index-1").execute().actionGet();
            fail("should not have been able to open index");
        } catch (IllegalArgumentException e) {
            verifyException(dataNodes, counts, e);
        }
        ClusterState clusterState = client.admin().cluster().prepareState().get().getState();
        assertFalse(clusterState.getMetadata().hasIndex("snapshot-index"));
    }

    public void testIgnoreDotSettingOnMultipleNodes() throws IOException, InterruptedException {
        int maxAllowedShardsPerNode = 10, indexPrimaryShards = 11, indexReplicaShards = 1;

        InternalTestCluster cluster = new InternalTestCluster(
            randomLong(),
            createTempDir(),
            true,
            true,
            0,
            0,
            "cluster",
            new NodeConfigurationSource() {
                @Override
                public Settings nodeSettings(int nodeOrdinal) {
                    return Settings.builder()
                        .put(ClusterShardLimitIT.this.nodeSettings(nodeOrdinal))
                        .put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType())
                        .build();
                }

                @Override
                public Path nodeConfigPath(int nodeOrdinal) {
                    return null;
                }
            },
            0,
            "cluster-",
            Arrays.asList(
                TestSeedPlugin.class,
                MockHttpTransport.TestPlugin.class,
                MockTransportService.TestPlugin.class,
                MockNioTransportPlugin.class,
                InternalSettingsPlugin.class,
                MockRepository.Plugin.class
            ),
            Function.identity()
        );
        cluster.beforeTest(random());

        // Starting 3 ClusterManagerOnlyNode nodes
        cluster.startClusterManagerOnlyNode(Settings.builder().put("cluster.ignore_dot_indexes", true).build());
        cluster.startClusterManagerOnlyNode(Settings.builder().put("cluster.ignore_dot_indexes", false).build());
        cluster.startClusterManagerOnlyNode(Settings.builder().put("cluster.ignore_dot_indexes", false).build());

        // Starting 2 data nodes
        cluster.startDataOnlyNode(Settings.builder().put("cluster.ignore_dot_indexes", false).build());
        cluster.startDataOnlyNode(Settings.builder().put("cluster.ignore_dot_indexes", false).build());

        // Setting max shards per node to be 10
        cluster.client()
            .admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(shardsPerNodeKey, maxAllowedShardsPerNode))
            .get();

        // Creating an index starting with dot having shards greater thn the desired node limit
        cluster.client()
            .admin()
            .indices()
            .prepareCreate(".test-index")
            .setSettings(
                Settings.builder().put(SETTING_NUMBER_OF_SHARDS, indexPrimaryShards).put(SETTING_NUMBER_OF_REPLICAS, indexReplicaShards)
            )
            .get();

        // As active ClusterManagerNode setting takes precedence killing the active one.
        // This would be the first one where cluster.ignore_dot_indexes is true because the above calls are blocking.
        cluster.stopCurrentClusterManagerNode();

        // Waiting for all shards to get assigned
        cluster.client().admin().cluster().prepareHealth().setWaitForGreenStatus().get();

        // Creating an index starting with dot having shards greater thn the desired node limit
        try {
            cluster.client()
                .admin()
                .indices()
                .prepareCreate(".test-index1")
                .setSettings(
                    Settings.builder().put(SETTING_NUMBER_OF_SHARDS, indexPrimaryShards).put(SETTING_NUMBER_OF_REPLICAS, indexReplicaShards)
                )
                .get();
        } catch (IllegalArgumentException e) {
            ClusterHealthResponse clusterHealth = cluster.client().admin().cluster().prepareHealth().get();
            int currentActiveShards = clusterHealth.getActiveShards();
            int dataNodeCount = clusterHealth.getNumberOfDataNodes();
            int extraShardCount = indexPrimaryShards * (1 + indexReplicaShards);
            verifyException(maxAllowedShardsPerNode * dataNodeCount, currentActiveShards, extraShardCount, e);
        }

        IOUtils.close(cluster);
    }

    private int ensureMultipleDataNodes(int dataNodes) {
        if (dataNodes == 1) {
            internalCluster().startNode(dataNode());
            assertThat(
                client().admin()
                    .cluster()
                    .prepareHealth()
                    .setWaitForEvents(Priority.LANGUID)
                    .setWaitForNodes(">=2")
                    .setLocal(true)
                    .execute()
                    .actionGet()
                    .isTimedOut(),
                equalTo(false)
            );
            dataNodes = client().admin().cluster().prepareState().get().getState().getNodes().getDataNodes().size();
        }
        return dataNodes;
    }

    /**
     * Set max shard limit on either per node level or on cluster level.
     *
     * @param limit the limit value to set.
     * @param key node level or cluster level setting key.
     */
    private void setMaxShardLimit(int limit, String key) {
        try {
            ClusterUpdateSettingsResponse response;
            if (frequently()) {
                response = client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().put(key, limit).build())
                    .get();
                assertEquals(limit, response.getPersistentSettings().getAsInt(key, -1).intValue());
            } else {
                response = client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put(key, limit).build())
                    .get();
                assertEquals(limit, response.getTransientSettings().getAsInt(key, -1).intValue());
            }
        } catch (IllegalArgumentException ex) {
            fail(ex.getMessage());
        }

    }

    private void setIgnoreDotIndex(boolean ignoreDotIndex) {
        try {
            ClusterUpdateSettingsResponse response;
            if (frequently()) {
                response = client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().put(ignoreDotIndexKey, ignoreDotIndex).build())
                    .get();
                assertEquals(ignoreDotIndex, response.getPersistentSettings().getAsBoolean(ignoreDotIndexKey, true));
            } else {
                response = client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put(ignoreDotIndexKey, ignoreDotIndex).build())
                    .get();
                assertEquals(ignoreDotIndex, response.getTransientSettings().getAsBoolean(ignoreDotIndexKey, true));
            }
        } catch (IllegalArgumentException ex) {
            fail(ex.getMessage());
        }
    }

    private void verifyException(int dataNodes, ShardCounts counts, IllegalArgumentException e) {
        int totalShards = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
        int currentShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
        int maxShards = counts.getShardsPerNode() * dataNodes;
        String expectedError = "Validation Failed: 1: this action would add ["
            + totalShards
            + "] total shards, but this cluster currently has ["
            + currentShards
            + "]/["
            + maxShards
            + "] maximum shards open;";
        assertEquals(expectedError, e.getMessage());
    }

    private void verifyException(int maxShards, int currentShards, int extraShards, IllegalArgumentException e) {
        String expectedError = "Validation Failed: 1: this action would add ["
            + extraShards
            + "] total shards, but this cluster currently has ["
            + currentShards
            + "]/["
            + maxShards
            + "] maximum shards open;";
        assertEquals(expectedError, e.getMessage());
    }

}
