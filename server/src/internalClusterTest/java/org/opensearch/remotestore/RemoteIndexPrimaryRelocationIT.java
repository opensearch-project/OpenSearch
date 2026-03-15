/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexService;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLease;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.indices.recovery.IndexPrimaryRelocationIT;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteIndexPrimaryRelocationIT extends IndexPrimaryRelocationIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(InternalSettingsPlugin.class);
    }

    protected static final String REPOSITORY_NAME = "test-remote-store-repo";

    protected Path absolutePath;

    protected Settings nodeSettings(int nodeOrdinal) {
        if (absolutePath == null) {
            absolutePath = randomRepoPath().toAbsolutePath();
        }
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(REPOSITORY_NAME, absolutePath))
            .build();
    }

    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
    }

    public void testPrimaryRelocationWhileIndexing() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        super.testPrimaryRelocationWhileIndexing();
    }

    public void testRetentionLeaseAfterPrimaryRelocation() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final Settings settings = Settings.builder()
            .put(indexSettings())
            .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
            .build();
        createIndex("test", settings);
        ensureGreen("test");

        // index a doc so the shard is not empty
        client().prepareIndex("test").setId("1").setSource("field", "value").get();
        refresh("test");

        final String dataNodeB = internalCluster().startDataOnlyNode();

        // get source node id
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        ShardRouting primaryShard = state.routingTable().shardRoutingTable("test", 0).primaryShard();
        final String sourceNodeId = primaryShard.currentNodeId();
        final DiscoveryNode targetNode = state.nodes()
            .getDataNodes()
            .values()
            .stream()
            .filter(n -> n.getId().equals(sourceNodeId) == false)
            .findFirst()
            .orElseThrow();

        // relocate primary
        client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand("test", 0, sourceNodeId, targetNode.getId()))
            .execute()
            .actionGet();
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setTimeout(TimeValue.timeValueSeconds(60))
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .execute()
            .actionGet();
        assertFalse("timed out waiting for relocation", clusterHealthResponse.isTimedOut());
        ensureGreen("test");

        // add a replica after relocation
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build())
            .get();
        ensureGreen("test");

        // verify only one retention lease exists and it belongs to the primary
        assertBusy(() -> {
            final IndicesStatsResponse statsResponse = client().admin().indices().prepareStats("test").get();
            for (ShardStats shardStats : statsResponse.getShards()) {
                if (shardStats.getShardRouting().primary() == false) {
                    continue;
                }
                final RetentionLeases retentionLeases = shardStats.getRetentionLeaseStats().retentionLeases();
                final List<RetentionLease> peerRecoveryLeases = retentionLeases.leases()
                    .stream()
                    .filter(l -> ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE.equals(l.source()))
                    .collect(Collectors.toList());
                assertEquals(
                    "expected exactly one peer recovery retention lease but got " + peerRecoveryLeases,
                    1,
                    peerRecoveryLeases.size()
                );

                // the single lease should be for the current primary node
                assertEquals(
                    ReplicationTracker.getPeerRecoveryRetentionLeaseId(shardStats.getShardRouting().currentNodeId()),
                    peerRecoveryLeases.get(0).id()
                );
            }
        });
    }
}
