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

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingPool;
import org.opensearch.common.Priority;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.IndexModule;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.node.Node;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.MockHttpTransport;
import org.opensearch.test.NodeConfigurationSource;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.nio.MockNioTransportPlugin;
import org.junit.After;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.common.util.FeatureFlags.WRITABLE_WARM_INDEX_SETTING;
import static org.opensearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_REMOTE_CAPABLE_SHARDS_PER_NODE;
import static org.opensearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.opensearch.indices.ShardLimitValidator.SETTING_MAX_SHARDS_PER_CLUSTER_KEY;
import static org.opensearch.test.NodeRoles.dataNode;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ParameterizedStaticSettingsOpenSearchIntegTestCase.ClusterScope(scope = ParameterizedStaticSettingsOpenSearchIntegTestCase.Scope.TEST)
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class ClusterShardLimitIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public ClusterShardLimitIT(Settings nodeSettings) {
        super(nodeSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(WRITABLE_WARM_INDEX_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(WRITABLE_WARM_INDEX_SETTING.getKey(), true).build() }
        );
    }

    private static final String ignoreDotIndexKey = ShardLimitValidator.SETTING_CLUSTER_IGNORE_DOT_INDEXES.getKey();
    private static final long TOTAL_SPACE_BYTES = new ByteSizeValue(100, ByteSizeUnit.KB).getBytes();

    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected Path absolutePath;

    @After
    public void teardown() throws Exception {
        clusterAdmin().prepareCleanupRepository(REPOSITORY_NAME).get();
    }

    @Override
    public Settings indexSettings() {
        Boolean isWarmEnabled = WRITABLE_WARM_INDEX_SETTING.get(settings);
        if (isWarmEnabled) {
            return Settings.builder().put(super.indexSettings()).put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true).build();
        } else {
            return super.indexSettings();
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (absolutePath == null) {
            absolutePath = randomRepoPath().toAbsolutePath();
        }
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(REPOSITORY_NAME, absolutePath))
            .build();
    }

    @Override
    protected Settings featureFlagSettings() {
        Settings.Builder featureSettings = Settings.builder();
        featureSettings.put(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG, true);
        return featureSettings.build();
    }

    @Override
    protected boolean addMockIndexStorePlugin() {
        return WRITABLE_WARM_INDEX_SETTING.get(settings) == false;
    }

    public void testSettingClusterMaxShards() {
        int shardsPerNode = between(1, 500_000);
        setMaxShardLimit(shardsPerNode, getShardsPerNodeKey());
    }

    public void testSettingIgnoreDotIndexes() {
        boolean ignoreDotIndexes = randomBoolean();
        setIgnoreDotIndex(ignoreDotIndexes);
    }

    public void testMinimumPerNode() {
        startTestNodes(3);
        int negativeShardsPerNode = between(-50_000, 0);
        try {
            if (frequently()) {
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().put(getShardsPerNodeKey(), negativeShardsPerNode).build())
                    .get();
            } else {
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put(getShardsPerNodeKey(), negativeShardsPerNode).build())
                    .get();
            }
            fail("should not be able to set negative shards per node");
        } catch (IllegalArgumentException ex) {
            assertEquals(
                "Failed to parse value [" + negativeShardsPerNode + "] for setting [" + getShardsPerNodeKey() + "] must be >= 1",
                ex.getMessage()
            );
        }
    }

    public void testIndexCreationOverLimit() {
        int dataNodes = startTestNodes(3);

        ShardCounts counts = ShardCounts.forDataNodeCount(dataNodes);

        setMaxShardLimit(counts.getShardsPerNode(), getShardsPerNodeKey());
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
        int dataNodes = startTestNodes(3);

        // Setting the cluster.max_shards_per_node setting according to the data node count.
        setMaxShardLimit(dataNodes, getShardsPerNodeKey());
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
        String maxShardsPerNode = clusterState.getMetadata().settings().get(getShardsPerNodeKey());

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
        int dataNodes = startTestNodes(3);
        int maxAllowedShards = dataNodes * dataNodes;

        // Setting the cluster.max_shards_per_node setting according to the data node count.
        setMaxShardLimit(dataNodes, getShardsPerNodeKey());

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
        String maxShardsPerNode = clusterState.getMetadata().settings().get(getShardsPerNodeKey());

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

    public void testCreateIndexWithMaxClusterShardSetting() {
        int maxAllowedShardsPerNode = startTestNodes(3);
        setMaxShardLimit(maxAllowedShardsPerNode, getShardsPerNodeKey());

        // Always keep
        int maxAllowedShardsPerCluster = maxAllowedShardsPerNode * 1000;
        int extraShardCount = 1;
        // Getting total active shards in the cluster.
        int currentActiveShards = client().admin().cluster().prepareHealth().get().getActiveShards();
        try {
            setMaxShardLimit(maxAllowedShardsPerCluster, SETTING_MAX_SHARDS_PER_CLUSTER_KEY);
            prepareCreate("test_index_with_cluster_shard_limit").setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, extraShardCount).put(SETTING_NUMBER_OF_REPLICAS, 0).build()
            ).get();
        } catch (final IllegalArgumentException ex) {
            verifyException(maxAllowedShardsPerCluster, currentActiveShards, extraShardCount, ex);
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
        int dataNodes = startTestNodes(3);
        int maxAllowedShards = dataNodes * dataNodes;

        // Setting the cluster.max_shards_per_node setting according to the data node count.
        setMaxShardLimit(dataNodes, getShardsPerNodeKey());
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
        String maxShardsPerNode = clusterState.getMetadata().settings().get(getShardsPerNodeKey());

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
        int dataNodes = startTestNodes(3);

        final ShardCounts counts = ShardCounts.forDataNodeCount(dataNodes);

        setMaxShardLimit(counts.getShardsPerNode(), getShardsPerNodeKey());

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
                        .put(indexSettings())
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
        int dataNodes = startTestNodes(3);

        dataNodes = ensureMultipleDataNodes(dataNodes);

        int firstShardCount = between(2, 10);
        int shardsPerNode = firstShardCount - 1;
        setMaxShardLimit(shardsPerNode, getShardsPerNodeKey());

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
                + "] total "
                + getShardType()
                + " shards, but this cluster currently has ["
                + firstShardCount
                + "]/["
                + dataNodes * shardsPerNode
                + "] maximum "
                + getShardType()
                + " shards open;";
            assertEquals(expectedError, e.getMessage());
        }
        Metadata clusterState = client().admin().cluster().prepareState().get().getState().metadata();
        assertEquals(0, clusterState.index("growing-should-fail").getNumberOfReplicas());
    }

    public void testChangingMultipleIndicesOverLimit() {
        int dataNodes = startTestNodes(3);

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
        setMaxShardLimit(shardsPerNode, getShardsPerNodeKey());

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
                + "] total "
                + getShardType()
                + " shards, but this cluster currently has ["
                + totalShardsBefore
                + "]/["
                + dataNodes * shardsPerNode
                + "] maximum "
                + getShardType()
                + " shards open;";
            assertEquals(expectedError, e.getMessage());
        }
        Metadata clusterState = client().admin().cluster().prepareState().get().getState().metadata();
        assertEquals(firstIndexReplicas, clusterState.index("test-1-index").getNumberOfReplicas());
        assertEquals(secondIndexReplicas, clusterState.index("test-2-index").getNumberOfReplicas());
    }

    public void testPreserveExistingSkipsCheck() {
        int dataNodes = startTestNodes(3);

        dataNodes = ensureMultipleDataNodes(dataNodes);

        int firstShardCount = between(2, 10);
        int shardsPerNode = firstShardCount - 1;
        setMaxShardLimit(shardsPerNode, getShardsPerNodeKey());

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
        int dataNodes = startTestNodes(3);
        Client client = client();

        logger.info("-->  creating repository");
        Settings.Builder repoSettings = Settings.builder();
        repoSettings.put("location", randomRepoPath());
        repoSettings.put("compress", randomBoolean());
        repoSettings.put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES);
        createRepository("test-repo", "fs", repoSettings);

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
        setMaxShardLimit(counts.getShardsPerNode(), getShardsPerNodeKey());
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
        int dataNodes = startTestNodes(3);
        Client client = client();
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
        setMaxShardLimit(counts.getShardsPerNode(), getShardsPerNodeKey());
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

        Path tempDir = absolutePath.getParent().getParent();
        InternalTestCluster cluster = new InternalTestCluster(
            randomLong(),
            tempDir,
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
            .setPersistentSettings(Settings.builder().put(getShardsPerNodeKey(), maxAllowedShardsPerNode))
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
            + "] total "
            + getShardType()
            + " shards, but this cluster currently has ["
            + currentShards
            + "]/["
            + maxShards
            + "] maximum "
            + getShardType()
            + " shards open;";
        assertEquals(expectedError, e.getMessage());
    }

    private void verifyException(int maxShards, int currentShards, int extraShards, IllegalArgumentException e) {
        String expectedError = "Validation Failed: 1: this action would add ["
            + extraShards
            + "] total "
            + getShardType()
            + " shards, but this cluster currently has ["
            + currentShards
            + "]/["
            + maxShards
            + "] maximum "
            + getShardType()
            + " shards open;";
        assertEquals(expectedError, e.getMessage());
    }

    private RoutingPool getShardType() {
        Boolean isWarmIndex = WRITABLE_WARM_INDEX_SETTING.get(settings);
        return isWarmIndex ? RoutingPool.REMOTE_CAPABLE : RoutingPool.LOCAL_ONLY;
    }

    private Settings warmNodeSettings(ByteSizeValue cacheSize) {
        return Settings.builder()
            .put(super.nodeSettings(0))
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), cacheSize.toString())
            .build();
    }

    /**
     * Helper method to start nodes that support both data and warm roles
     */
    private int startTestNodes(int nodeCount) {
        boolean isWarmIndex = WRITABLE_WARM_INDEX_SETTING.get(settings);
        if (isWarmIndex) {
            Settings nodeSettings = Settings.builder().put(warmNodeSettings(new ByteSizeValue(TOTAL_SPACE_BYTES))).build();
            internalCluster().startWarmOnlyNodes(nodeCount, nodeSettings);
            return client().admin().cluster().prepareState().get().getState().getNodes().getWarmNodes().size();
        } else {
            internalCluster().startDataOnlyNodes(nodeCount);
            return client().admin().cluster().prepareState().get().getState().getNodes().getDataNodes().size();
        }
    }

    private String getShardsPerNodeKey() {
        boolean isWarmIndex = WRITABLE_WARM_INDEX_SETTING.get(settings);
        if (isWarmIndex) {
            return SETTING_CLUSTER_MAX_REMOTE_CAPABLE_SHARDS_PER_NODE.getKey();
        } else {
            return SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey();
        }
    }

}
