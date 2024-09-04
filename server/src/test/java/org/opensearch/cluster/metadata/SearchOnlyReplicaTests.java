/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.indices.cluster.ClusterStateChanges;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_REPLICATION_TYPE_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

public class SearchOnlyReplicaTests extends OpenSearchSingleNodeTestCase {

    private ThreadPool threadPool;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.threadPool = new TestThreadPool(getClass().getName());
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder()
            .put(super.featureFlagSettings())
            .put(FeatureFlags.READER_WRITER_SPLIT_EXPERIMENTAL_SETTING.getKey(), true)
            .build();
    }

    public void testCreateWithDefaultSearchReplicasSetting() {
        final ClusterStateChanges cluster = new ClusterStateChanges(xContentRegistry(), threadPool);
        ClusterState state = createIndexWithSettings(cluster, Settings.builder().build());
        IndexShardRoutingTable indexShardRoutingTable = state.getRoutingTable().index("index").getShards().get(0);
        assertEquals(1, indexShardRoutingTable.replicaShards().size());
        assertEquals(0, indexShardRoutingTable.searchOnlyReplicas().size());
        assertEquals(1, indexShardRoutingTable.writerReplicas().size());
    }

    public void testSearchReplicasValidationWithDocumentReplication() {
        final ClusterStateChanges cluster = new ClusterStateChanges(xContentRegistry(), threadPool);
        RuntimeException exception = expectThrows(
            RuntimeException.class,
            () -> createIndexWithSettings(
                cluster,
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.DOCUMENT)
                    .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
                    .build()
            )
        );
        assertEquals(
            "To set index.number_of_search_only_replicas, index.replication.type must be set to SEGMENT",
            exception.getCause().getMessage()
        );
    }

    public void testUpdateSearchReplicaCount() {
        final ClusterStateChanges cluster = new ClusterStateChanges(xContentRegistry(), threadPool);

        ClusterState state = createIndexWithSettings(
            cluster,
            Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put(INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
                .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
                .build()
        );
        assertTrue(state.metadata().hasIndex("index"));
        rerouteUntilActive(state, cluster);
        IndexShardRoutingTable indexShardRoutingTable = state.getRoutingTable().index("index").getShards().get(0);
        assertEquals(1, indexShardRoutingTable.replicaShards().size());
        assertEquals(1, indexShardRoutingTable.searchOnlyReplicas().size());
        assertEquals(0, indexShardRoutingTable.writerReplicas().size());

        // add another replica
        state = cluster.updateSettings(
            state,
            new UpdateSettingsRequest("index").settings(Settings.builder().put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 2).build())
        );
        rerouteUntilActive(state, cluster);
        indexShardRoutingTable = state.getRoutingTable().index("index").getShards().get(0);
        assertEquals(2, indexShardRoutingTable.replicaShards().size());
        assertEquals(2, indexShardRoutingTable.searchOnlyReplicas().size());
        assertEquals(0, indexShardRoutingTable.writerReplicas().size());

        // remove all replicas
        state = cluster.updateSettings(
            state,
            new UpdateSettingsRequest("index").settings(Settings.builder().put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 0).build())
        );
        rerouteUntilActive(state, cluster);
        indexShardRoutingTable = state.getRoutingTable().index("index").getShards().get(0);
        assertEquals(0, indexShardRoutingTable.replicaShards().size());
        assertEquals(0, indexShardRoutingTable.searchOnlyReplicas().size());
        assertEquals(0, indexShardRoutingTable.writerReplicas().size());
    }

    private ClusterState createIndexWithSettings(ClusterStateChanges cluster, Settings settings) {
        List<DiscoveryNode> allNodes = new ArrayList<>();
        // node for primary/local
        DiscoveryNode localNode = createNode(Version.CURRENT, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE);
        allNodes.add(localNode);
        // node for search replicas - we'll start with 1 and add another
        for (int i = 0; i < 2; i++) {
            allNodes.add(createNode(Version.CURRENT, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE));
        }
        ClusterState state = ClusterStateCreationUtils.state(localNode, localNode, allNodes.toArray(new DiscoveryNode[0]));

        CreateIndexRequest request = new CreateIndexRequest("index", settings).waitForActiveShards(ActiveShardCount.NONE);
        state = cluster.createIndex(state, request);
        return state;
    }

    public void testUpdateSearchReplicasOverShardLimit() {
        final ClusterStateChanges cluster = new ClusterStateChanges(xContentRegistry(), threadPool);

        List<DiscoveryNode> allNodes = new ArrayList<>();
        // node for primary/local
        DiscoveryNode localNode = createNode(Version.CURRENT, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE);
        allNodes.add(localNode);

        allNodes.add(createNode(Version.CURRENT, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE));

        ClusterState state = ClusterStateCreationUtils.state(localNode, localNode, allNodes.toArray(new DiscoveryNode[0]));

        CreateIndexRequest request = new CreateIndexRequest(
            "index",
            Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put(INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
                .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
                .build()
        ).waitForActiveShards(ActiveShardCount.NONE);
        state = cluster.createIndex(state, request);
        assertTrue(state.metadata().hasIndex("index"));
        rerouteUntilActive(state, cluster);

        // add another replica
        ClusterState finalState = state;
        Integer maxShardPerNode = ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getDefault(Settings.EMPTY);
        expectThrows(
            RuntimeException.class,
            () -> cluster.updateSettings(
                finalState,
                new UpdateSettingsRequest("index").settings(
                    Settings.builder().put(SETTING_NUMBER_OF_SEARCH_REPLICAS, maxShardPerNode * 2).build()
                )
            )
        );
    }

    public void testUpdateSearchReplicasOnDocrepCluster() {
        final ClusterStateChanges cluster = new ClusterStateChanges(xContentRegistry(), threadPool);

        List<DiscoveryNode> allNodes = new ArrayList<>();
        // node for primary/local
        DiscoveryNode localNode = createNode(Version.CURRENT, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE);
        allNodes.add(localNode);

        allNodes.add(createNode(Version.CURRENT, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE));

        ClusterState state = ClusterStateCreationUtils.state(localNode, localNode, allNodes.toArray(new DiscoveryNode[0]));

        CreateIndexRequest request = new CreateIndexRequest(
            "index",
            Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put(INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.DOCUMENT)
                .build()
        ).waitForActiveShards(ActiveShardCount.NONE);
        state = cluster.createIndex(state, request);
        assertTrue(state.metadata().hasIndex("index"));
        rerouteUntilActive(state, cluster);

        // add another replica
        ClusterState finalState = state;
        Integer maxShardPerNode = ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getDefault(Settings.EMPTY);
        expectThrows(
            RuntimeException.class,
            () -> cluster.updateSettings(
                finalState,
                new UpdateSettingsRequest("index").settings(
                    Settings.builder().put(SETTING_NUMBER_OF_SEARCH_REPLICAS, maxShardPerNode * 2).build()
                )
            )
        );

    }

    private static void rerouteUntilActive(ClusterState state, ClusterStateChanges cluster) {
        while (state.routingTable().index("index").shard(0).allShardsStarted() == false) {
            state = cluster.applyStartedShards(
                state,
                state.routingTable().index("index").shard(0).shardsWithState(ShardRoutingState.INITIALIZING)
            );
            state = cluster.reroute(state, new ClusterRerouteRequest());
        }
    }

    private static final AtomicInteger nodeIdGenerator = new AtomicInteger();

    protected DiscoveryNode createNode(Version version, DiscoveryNodeRole... mustHaveRoles) {
        Set<DiscoveryNodeRole> roles = new HashSet<>(randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES));
        Collections.addAll(roles, mustHaveRoles);
        final String id = String.format(Locale.ROOT, "node_%03d", nodeIdGenerator.incrementAndGet());
        return new DiscoveryNode(id, id, buildNewFakeTransportAddress(), Collections.emptyMap(), roles, version);
    }
}
