/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.remote.RemoteSegmentStats;
import org.opensearch.index.seqno.RetentionLease;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.indices.IndexingMemoryController;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.multipart.mocks.MockFsRepositoryPlugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteDualReplicationIT extends MigrationBaseTestCase {
    private final String REMOTE_PRI_DOCREP_REP = "remote-primary-docrep-replica";
    private final String REMOTE_PRI_DOCREP_REMOTE_REP = "remote-primary-docrep-remote-replica";
    private final String FAILOVER_REMOTE_TO_DOCREP = "failover-remote-to-docrep";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        /* Adding the following mock plugins:
        - InternalSettingsPlugin : To override default intervals of retention lease and global ckp sync
        - MockFsRepositoryPlugin and MockTransportService.TestPlugin: To ensure remote interactions are not no-op and retention leases are properly propagated
         */
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(InternalSettingsPlugin.class, MockFsRepositoryPlugin.class, MockTransportService.TestPlugin.class)
        ).collect(Collectors.toList());
    }

    /*
    Scenario:
    - Starts 2 docrep backed node
    - Creates index with 1 replica
    - Index some docs
    - Start 1 remote backed node
    - Move primary copy from docrep to remote through _cluster/reroute
    - Index some more docs
    - Assert primary-replica consistency
     */
    public void testRemotePrimaryDocRepReplica() throws Exception {
        internalCluster().startClusterManagerOnlyNode();

        logger.info("---> Starting 2 docrep data nodes");
        internalCluster().startDataOnlyNodes(2);
        internalCluster().validateClusterFormed();
        assertEquals(internalCluster().client().admin().cluster().prepareGetRepositories().get().repositories().size(), 0);

        logger.info("---> Creating index with 1 replica");
        Settings oneReplica = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "1s")
            .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
            .build();
        createIndex(REMOTE_PRI_DOCREP_REP, oneReplica);
        ensureGreen(REMOTE_PRI_DOCREP_REP);

        int initialBatch = randomIntBetween(1, 1000);
        logger.info("---> Indexing {} docs", initialBatch);
        indexBulk(REMOTE_PRI_DOCREP_REP, initialBatch);

        initDocRepToRemoteMigration();

        logger.info("---> Starting 1 remote enabled data node");
        addRemote = true;
        String remoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        assertEquals(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareGetRepositories(REPOSITORY_NAME, REPOSITORY_2_NAME)
                .get()
                .repositories()
                .size(),
            2
        );

        String primaryShardHostingNode = primaryNodeName(REMOTE_PRI_DOCREP_REP);
        logger.info("---> Moving primary copy from {} to remote enabled node {}", primaryShardHostingNode, remoteNodeName);
        assertAcked(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(REMOTE_PRI_DOCREP_REP, 0, primaryShardHostingNode, remoteNodeName))
                .get()
        );
        ensureGreen(REMOTE_PRI_DOCREP_REP);
        ClusterState clusterState = internalCluster().client().admin().cluster().prepareState().get().getState();
        String primaryShardHostingNodeId = clusterState.getRoutingTable()
            .index(REMOTE_PRI_DOCREP_REP)
            .shard(0)
            .primaryShard()
            .currentNodeId();
        assertTrue(clusterState.getNodes().get(primaryShardHostingNodeId).isRemoteStoreNode());

        int secondBatch = randomIntBetween(1, 10);
        logger.info("---> Indexing another {} docs", secondBatch);
        indexBulk(REMOTE_PRI_DOCREP_REP, secondBatch);
        // Defensive check to ensure that doc count in replica shard catches up to the primary copy
        refreshAndWaitForReplication(REMOTE_PRI_DOCREP_REP);
        assertReplicaAndPrimaryConsistency(REMOTE_PRI_DOCREP_REP, initialBatch, secondBatch);
    }

    /*
    Scenario:
    - Starts 1 docrep backed data node
    - Creates an index with 0 replica
    - Starts 1 remote backed data node
    - Index some docs
    - Move primary copy from docrep to remote through _cluster/reroute
    - Starts another remote backed data node
    - Expands index to 2 replicas. One replica copy lies in remote backed node and other in docrep backed node
    - Index some more docs
    - Assert primary-replica consistency
     */
    public void testRemotePrimaryDocRepAndRemoteReplica() throws Exception {
        internalCluster().startClusterManagerOnlyNode();

        logger.info("---> Starting 1 docrep data nodes");
        String docrepNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        assertEquals(internalCluster().client().admin().cluster().prepareGetRepositories().get().repositories().size(), 0);

        logger.info("---> Creating index with 0 replica");
        Settings zeroReplicas = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "1s")
            .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
            .build();
        createIndex(REMOTE_PRI_DOCREP_REMOTE_REP, zeroReplicas);
        ensureGreen(REMOTE_PRI_DOCREP_REMOTE_REP);
        initDocRepToRemoteMigration();

        logger.info("---> Starting 1 remote enabled data node");
        addRemote = true;

        String remoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        assertEquals(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareGetRepositories(REPOSITORY_NAME, REPOSITORY_2_NAME)
                .get()
                .repositories()
                .size(),
            2
        );

        int firstBatch = randomIntBetween(1, 100);
        logger.info("---> Indexing {} docs", firstBatch);
        indexBulk(REMOTE_PRI_DOCREP_REMOTE_REP, firstBatch);

        String primaryShardHostingNode = primaryNodeName(REMOTE_PRI_DOCREP_REMOTE_REP);
        logger.info("---> Moving primary copy from {} to remote enabled node {}", primaryShardHostingNode, remoteNodeName);
        assertAcked(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(REMOTE_PRI_DOCREP_REMOTE_REP, 0, primaryShardHostingNode, remoteNodeName))
                .get()
        );
        ensureGreen(REMOTE_PRI_DOCREP_REMOTE_REP);
        ClusterState clusterState = internalCluster().client().admin().cluster().prepareState().get().getState();
        String primaryShardHostingNodeId = clusterState.getRoutingTable()
            .index(REMOTE_PRI_DOCREP_REMOTE_REP)
            .shard(0)
            .primaryShard()
            .currentNodeId();
        assertTrue(clusterState.getNodes().get(primaryShardHostingNodeId).isRemoteStoreNode());

        logger.info("---> Starting another remote enabled node");
        internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();

        logger.info("---> Expanding index to 2 replica copies");
        Settings twoReplicas = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2).build();
        assertAcked(
            internalCluster().client()
                .admin()
                .indices()
                .prepareUpdateSettings()
                .setIndices(REMOTE_PRI_DOCREP_REMOTE_REP)
                .setSettings(twoReplicas)
                .get()
        );
        ensureGreen(REMOTE_PRI_DOCREP_REMOTE_REP);

        int secondBatch = randomIntBetween(1, 10);
        logger.info("---> Indexing another {} docs", secondBatch);
        indexBulk(REMOTE_PRI_DOCREP_REMOTE_REP, secondBatch);
        // Defensive check to ensure that doc count in replica shard catches up to the primary copy
        refreshAndWaitForReplication(REMOTE_PRI_DOCREP_REMOTE_REP);
        assertReplicaAndPrimaryConsistency(REMOTE_PRI_DOCREP_REMOTE_REP, firstBatch, secondBatch);
    }

    /*
    Checks if retention leases are published on primary shard and it's docrep copies, but not on remote copies
     */
    public void testRetentionLeasePresentOnDocrepReplicaButNotRemote() throws Exception {
        /* Reducing indices.memory.shard_inactive_time to force a flush and trigger translog sync,
        instead of relying on Global CKP Sync action which doesn't run on remote enabled copies

        Under steady state, RetentionLeases would be on (GlobalCkp + 1) on a
        docrep enabled shard copy and (GlobalCkp) for a remote enabled shard copy.
        This is because we block translog sync on remote enabled shard copies during the GlobalCkpSync background task.

        RLs on remote enabled copies are brought up to (GlobalCkp + 1) upon a flush request issued by IndexingMemoryController
        when the shard becomes inactive after SHARD_INACTIVE_TIME_SETTING interval.

        Flush triggers a force sync of translog which bumps the RetentionLease sequence number along with it
        */
        extraSettings = Settings.builder().put(IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING.getKey(), "3s").build();
        testRemotePrimaryDocRepAndRemoteReplica();
        DiscoveryNodes nodes = internalCluster().client().admin().cluster().prepareState().get().getState().getNodes();
        assertBusy(() -> {
            for (ShardStats shardStats : internalCluster().client()
                .admin()
                .indices()
                .prepareStats(REMOTE_PRI_DOCREP_REMOTE_REP)
                .get()
                .getShards()) {
                ShardRouting shardRouting = shardStats.getShardRouting();
                DiscoveryNode discoveryNode = nodes.get(shardRouting.currentNodeId());
                RetentionLeases retentionLeases = shardStats.getRetentionLeaseStats().retentionLeases();
                if (shardRouting.primary()) {
                    // Primary copy should be on remote node and should have retention leases
                    assertTrue(discoveryNode.isRemoteStoreNode());
                    assertCheckpointsConsistency(shardStats);
                    assertRetentionLeaseConsistency(shardStats, retentionLeases);
                } else {
                    // Checkpoints and Retention Leases are not synced to remote replicas
                    if (discoveryNode.isRemoteStoreNode()) {
                        assertTrue(shardStats.getRetentionLeaseStats().retentionLeases().leases().isEmpty());
                    } else {
                        // Replica copy on docrep node should have retention leases
                        assertCheckpointsConsistency(shardStats);
                        assertRetentionLeaseConsistency(shardStats, retentionLeases);
                    }
                }
            }
        });
    }

    /*
    Scenario:
    - Starts 1 docrep backed data node
    - Creates an index with 0 replica
    - Starts 1 remote backed data node
    - Move primary copy from docrep to remote through _cluster/reroute
    - Expands index to 1 replica
    - Stops remote enabled node
    - Ensure doc count is same after failover
    - Index some more docs to ensure working of failed-over primary
     */
    public void testFailoverRemotePrimaryToDocrepReplica() throws Exception {
        internalCluster().startClusterManagerOnlyNode();

        logger.info("---> Starting 1 docrep data nodes");
        String docrepNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        assertEquals(internalCluster().client().admin().cluster().prepareGetRepositories().get().repositories().size(), 0);

        logger.info("---> Creating index with 0 replica");
        Settings excludeRemoteNode = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
        createIndex(FAILOVER_REMOTE_TO_DOCREP, excludeRemoteNode);
        ensureGreen(FAILOVER_REMOTE_TO_DOCREP);
        initDocRepToRemoteMigration();
        logger.info("---> Starting 1 remote enabled data node");
        addRemote = true;
        String remoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        assertEquals(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareGetRepositories(REPOSITORY_NAME, REPOSITORY_2_NAME)
                .get()
                .repositories()
                .size(),
            2
        );

        logger.info("---> Starting doc ingestion in parallel thread");
        AsyncIndexingService asyncIndexingService = new AsyncIndexingService(FAILOVER_REMOTE_TO_DOCREP);
        asyncIndexingService.startIndexing();

        String primaryShardHostingNode = primaryNodeName(FAILOVER_REMOTE_TO_DOCREP);
        logger.info("---> Moving primary copy from {} to remote enabled node {}", primaryShardHostingNode, remoteNodeName);
        assertAcked(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(FAILOVER_REMOTE_TO_DOCREP, 0, primaryShardHostingNode, remoteNodeName))
                .get()
        );
        ensureGreen(FAILOVER_REMOTE_TO_DOCREP);

        logger.info("---> Expanding index to 1 replica copy");
        Settings twoReplicas = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build();
        assertAcked(
            internalCluster().client()
                .admin()
                .indices()
                .prepareUpdateSettings()
                .setIndices(FAILOVER_REMOTE_TO_DOCREP)
                .setSettings(twoReplicas)
                .get()
        );
        ensureGreen(FAILOVER_REMOTE_TO_DOCREP);
        logger.info("---> Stopping indexing thread");
        asyncIndexingService.stopIndexing();

        refreshAndWaitForReplication(FAILOVER_REMOTE_TO_DOCREP);
        Map<ShardRouting, ShardStats> shardStatsMap = internalCluster().client()
            .admin()
            .indices()
            .prepareStats(FAILOVER_REMOTE_TO_DOCREP)
            .setDocs(true)
            .get()
            .asMap();
        DiscoveryNodes nodes = internalCluster().client().admin().cluster().prepareState().get().getState().getNodes();
        long initialPrimaryDocCount = 0;
        for (ShardRouting shardRouting : shardStatsMap.keySet()) {
            if (shardRouting.primary()) {
                assertTrue(nodes.get(shardRouting.currentNodeId()).isRemoteStoreNode());
                initialPrimaryDocCount = shardStatsMap.get(shardRouting).getStats().getDocs().getCount();
            }
        }
        int firstBatch = (int) asyncIndexingService.getIndexedDocs();
        assertReplicaAndPrimaryConsistency(FAILOVER_REMOTE_TO_DOCREP, firstBatch, 0);

        logger.info("---> Stop remote store enabled node");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(remoteNodeName));
        ensureStableCluster(2);
        ensureYellow(FAILOVER_REMOTE_TO_DOCREP);

        shardStatsMap = internalCluster().client().admin().indices().prepareStats(FAILOVER_REMOTE_TO_DOCREP).setDocs(true).get().asMap();
        nodes = internalCluster().client().admin().cluster().prepareState().get().getState().getNodes();
        long primaryDocCountAfterFailover = 0;
        for (ShardRouting shardRouting : shardStatsMap.keySet()) {
            if (shardRouting.primary()) {
                assertFalse(nodes.get(shardRouting.currentNodeId()).isRemoteStoreNode());
                primaryDocCountAfterFailover = shardStatsMap.get(shardRouting).getStats().getDocs().getCount();
            }
        }
        assertEquals(initialPrimaryDocCount, primaryDocCountAfterFailover);

        logger.info("---> Index some more docs to ensure that the failed over primary is ingesting new docs");
        int secondBatch = randomIntBetween(1, 10);
        logger.info("---> Indexing {} more docs", secondBatch);
        indexBulk(FAILOVER_REMOTE_TO_DOCREP, secondBatch);
        refreshAndWaitForReplication(FAILOVER_REMOTE_TO_DOCREP);

        shardStatsMap = internalCluster().client().admin().indices().prepareStats(FAILOVER_REMOTE_TO_DOCREP).setDocs(true).get().asMap();
        assertEquals(1, shardStatsMap.size());
        shardStatsMap.forEach(
            (shardRouting, shardStats) -> { assertEquals(firstBatch + secondBatch, shardStats.getStats().getDocs().getCount()); }
        );
    }

    /*
    Scenario:
    - Starts 1 docrep backed data node
    - Creates an index with 0 replica
    - Starts 1 remote backed data node
    - Move primary copy from docrep to remote through _cluster/reroute
    - Expands index to 1 replica
    - Stops remote enabled node
    - Ensure doc count is same after failover
    - Index some more docs to ensure working of failed-over primary
    - Starts another remote node
    - Move primary copy from docrep to remote through _cluster/reroute
    - Ensure that remote store is seeded in the new remote node by asserting remote uploads from that node > 0
     */
    public void testFailoverRemotePrimaryToDocrepReplicaReseedToRemotePrimary() throws Exception {
        testFailoverRemotePrimaryToDocrepReplica();

        logger.info("---> Removing replica copy");
        assertAcked(
            internalCluster().client()
                .admin()
                .indices()
                .prepareUpdateSettings(FAILOVER_REMOTE_TO_DOCREP)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
                .get()
        );
        ensureGreen(FAILOVER_REMOTE_TO_DOCREP);

        logger.info("---> Starting a new remote enabled node");
        addRemote = true;
        String remoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        assertEquals(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareGetRepositories(REPOSITORY_NAME, REPOSITORY_2_NAME)
                .get()
                .repositories()
                .size(),
            2
        );

        String primaryShardHostingNode = primaryNodeName(FAILOVER_REMOTE_TO_DOCREP);
        logger.info("---> Moving primary copy from {} to remote enabled node {}", primaryShardHostingNode, remoteNodeName);
        assertAcked(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(FAILOVER_REMOTE_TO_DOCREP, 0, primaryShardHostingNode, remoteNodeName))
                .get()
        );
        ensureGreen(FAILOVER_REMOTE_TO_DOCREP);

        Map<ShardRouting, ShardStats> shardStatsMap = internalCluster().client()
            .admin()
            .indices()
            .prepareStats(FAILOVER_REMOTE_TO_DOCREP)
            .get()
            .asMap();
        DiscoveryNodes discoveryNodes = internalCluster().client().admin().cluster().prepareState().get().getState().getNodes();
        assertEquals(1, shardStatsMap.size());
        shardStatsMap.forEach((shardRouting, shardStats) -> {
            if (discoveryNodes.get(shardRouting.currentNodeId()).isRemoteStoreNode()) {
                RemoteSegmentStats remoteSegmentStats = shardStats.getStats().getSegments().getRemoteSegmentStats();
                assertTrue(remoteSegmentStats.getTotalUploadTime() > 0);
                assertTrue(remoteSegmentStats.getUploadBytesSucceeded() > 0);
            }
        });
    }

    private void assertReplicaAndPrimaryConsistency(String indexName, int firstBatch, int secondBatch) throws Exception {
        assertBusy(() -> {
            Map<ShardRouting, ShardStats> shardStatsMap = internalCluster().client()
                .admin()
                .indices()
                .prepareStats(indexName)
                .setDocs(true)
                .get()
                .asMap();
            DiscoveryNodes nodes = internalCluster().client().admin().cluster().prepareState().get().getState().getNodes();
            for (ShardRouting shardRouting : shardStatsMap.keySet()) {
                CommonStats shardStats = shardStatsMap.get(shardRouting).getStats();
                if (shardRouting.primary()) {
                    assertEquals(firstBatch + secondBatch, shardStats.getDocs().getCount());
                    assertTrue(nodes.get(shardRouting.currentNodeId()).isRemoteStoreNode());
                    RemoteSegmentStats remoteSegmentStats = shardStats.getSegments().getRemoteSegmentStats();
                    assertTrue(remoteSegmentStats.getUploadBytesSucceeded() > 0);
                    assertTrue(remoteSegmentStats.getTotalUploadTime() > 0);
                } else {
                    boolean remoteNode = nodes.get(shardRouting.currentNodeId()).isRemoteStoreNode();
                    assertEquals(
                        "Mismatched doc count. Is this on remote node ? " + remoteNode,
                        firstBatch + secondBatch,
                        shardStats.getDocs().getCount()
                    );
                    RemoteSegmentStats remoteSegmentStats = shardStats.getSegments().getRemoteSegmentStats();
                    if (remoteNode) {
                        assertTrue(remoteSegmentStats.getDownloadBytesStarted() > 0);
                        assertTrue(remoteSegmentStats.getTotalDownloadTime() > 0);
                    } else {
                        assertEquals(0, remoteSegmentStats.getUploadBytesSucceeded());
                        assertEquals(0, remoteSegmentStats.getTotalUploadTime());
                    }
                }
            }
        });
    }

    /**
     * For a docrep enabled shard copy or a primary shard copy,
     * asserts that the stored Retention Leases equals to 1 + maxSeqNo ingested on the node
     *
     * @param shardStats ShardStats object from NodesStats API
     * @param retentionLeases RetentionLeases from NodesStats API
     */
    private static void assertRetentionLeaseConsistency(ShardStats shardStats, RetentionLeases retentionLeases) {
        long maxSeqNo = shardStats.getSeqNoStats().getMaxSeqNo();
        for (RetentionLease rl : retentionLeases.leases()) {
            assertEquals(maxSeqNo + 1, rl.retainingSequenceNumber());
        }
    }

    /**
     * For a docrep enabled shard copy or a primary shard copy,
     * asserts that local and global checkpoints are up-to-date with maxSeqNo of doc operations
     *
     * @param shardStats ShardStats object from NodesStats API
     */
    private static void assertCheckpointsConsistency(ShardStats shardStats) {
        long maxSeqNo = shardStats.getSeqNoStats().getMaxSeqNo();
        long localCkp = shardStats.getSeqNoStats().getLocalCheckpoint();
        long globalCkp = shardStats.getSeqNoStats().getGlobalCheckpoint();

        assertEquals(maxSeqNo, localCkp);
        assertEquals(maxSeqNo, globalCkp);
    }
}
