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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.remote.RemoteSegmentStats;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.junit.annotations.TestLogging;

import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class DocrepToRemoteDualReplicationIT extends MigrationBaseTestCase {
    public String REMOTE_PRI_DOCREP_REP = "remote-primary-docrep-replica";
    public String REMOTE_PRI_DOCREP_REMOTE_REP = "remote-primary-docrep-remote-replica";
    public String FAILOVER_REMOTE_TO_DOCREP = "failover-remote-to-docrep";

    /*
    Scenario:
    - Starts 2 docrep backed and 1 remote backed data node
    - Exclude remote backed node from shard assignment
    - Index some docs
    - Move primary copy from docrep to remote through _cluster/reroute
    - Index some more docs
    - Assert primary-replica consistency
     */
    public void testRemotePrimaryDocRepReplica() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        internalCluster().startClusterManagerOnlyNode();
        initDocRepToRemoteMigration();

        logger.info("---> Starting 2 docrep data nodes");
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        assertEquals(internalCluster().client().admin().cluster().prepareGetRepositories().get().repositories().size(), 0);

        logger.info("---> Starting 1 remote enabled data node");
        addRemote = true;
        String remoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        assertEquals(internalCluster().client().admin().cluster().prepareGetRepositories(REPOSITORY_NAME, REPOSITORY_2_NAME).get().repositories().size(), 2);

        logger.info("---> Excluding remote node from shard assignment");
        Settings excludeRemoteNode = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(), remoteNodeName)
            .build();
        createIndex(REMOTE_PRI_DOCREP_REP, excludeRemoteNode);
        ensureGreen(REMOTE_PRI_DOCREP_REP);

        int initialBatch = randomIntBetween(1, 1000);
        logger.info("---> Indexing {} docs", initialBatch);
        SyncIndexingService indexingService = new SyncIndexingService(REMOTE_PRI_DOCREP_REP, initialBatch);
        indexingService.startIndexing();

        Settings includeRemoteNode = Settings.builder()
            .putNull(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey())
            .build();
        assertAcked(internalCluster().client().admin().indices().prepareUpdateSettings().setIndices(REMOTE_PRI_DOCREP_REP).setSettings(includeRemoteNode).get());
        ensureGreen(REMOTE_PRI_DOCREP_REP);

        String primaryShardHostingNode = primaryNodeName(REMOTE_PRI_DOCREP_REP);
        logger.info("---> Moving primary copy from {} to remote enabled node {}", primaryShardHostingNode, remoteNodeName);
        assertAcked(internalCluster().client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(REMOTE_PRI_DOCREP_REP, 0, primaryShardHostingNode, remoteNodeName)).get());
        ensureGreen(REMOTE_PRI_DOCREP_REP);

        int secondBatch = randomIntBetween(1, 10);
        logger.info("---> Indexing another {} docs", secondBatch);
        indexBulk(REMOTE_PRI_DOCREP_REP, secondBatch);
        // Defensive check to ensure that doc count in replica shard catches up to the primary copy
        flush(REMOTE_PRI_DOCREP_REP);
        Map<ShardRouting, ShardStats> shardStatsMap = internalCluster().client().admin().indices().prepareStats(REMOTE_PRI_DOCREP_REP).setDocs(true).get().asMap();
        DiscoveryNodes nodes = internalCluster().client().admin().cluster().prepareState().get().getState().getNodes();
        assertReplicaAndPrimaryConsistencySingle(shardStatsMap, initialBatch, secondBatch, nodes);
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
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        internalCluster().startClusterManagerOnlyNode();
        initDocRepToRemoteMigration();

        logger.info("---> Starting 1 docrep data nodes");
        String docrepNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        assertEquals(internalCluster().client().admin().cluster().prepareGetRepositories().get().repositories().size(), 0);

        logger.info("---> Creating index with 0 replica");
        Settings excludeRemoteNode = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
        createIndex(REMOTE_PRI_DOCREP_REMOTE_REP, excludeRemoteNode);
        ensureGreen(REMOTE_PRI_DOCREP_REMOTE_REP);

        logger.info("---> Starting 1 remote enabled data node");
        addRemote = true;
        String remoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        assertEquals(internalCluster().client().admin().cluster().prepareGetRepositories(REPOSITORY_NAME, REPOSITORY_2_NAME).get().repositories().size(), 2);

        int firstBatch = randomIntBetween(1, 100);
        logger.info("---> Indexing {} docs", firstBatch);
        SyncIndexingService indexingService = new SyncIndexingService(REMOTE_PRI_DOCREP_REMOTE_REP, firstBatch);
        indexingService.startIndexing();

        String primaryShardHostingNode = primaryNodeName(REMOTE_PRI_DOCREP_REMOTE_REP);
        logger.info("---> Moving primary copy from {} to remote enabled node {}", primaryShardHostingNode, remoteNodeName);
        assertAcked(internalCluster().client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(REMOTE_PRI_DOCREP_REMOTE_REP, 0, primaryShardHostingNode, remoteNodeName)).get());
        ensureGreen(REMOTE_PRI_DOCREP_REMOTE_REP);

        logger.info("---> Starting another remote enabled node");
        internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();

        logger.info("---> Expanding index to 2 replica copies");
        Settings twoReplicas = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
            .build();
        assertAcked(internalCluster().client().admin().indices().prepareUpdateSettings().setIndices(REMOTE_PRI_DOCREP_REMOTE_REP).setSettings(twoReplicas).get());
        ensureGreen(REMOTE_PRI_DOCREP_REMOTE_REP);

        int secondBatch = randomIntBetween(1, 10);
        logger.info("---> Indexing another {} docs", secondBatch);
        indexBulk(REMOTE_PRI_DOCREP_REMOTE_REP, secondBatch);
        // Defensive check to ensure that doc count in replica shard catches up to the primary copy
        flush(REMOTE_PRI_DOCREP_REMOTE_REP);

        Map<ShardRouting, ShardStats> shardStatsMap = internalCluster().client().admin().indices().prepareStats(REMOTE_PRI_DOCREP_REMOTE_REP).setDocs(true).get().asMap();
        DiscoveryNodes nodes = internalCluster().client().admin().cluster().prepareState().get().getState().getNodes();
        assertReplicaAndPrimaryConsistencyMultiCopy(shardStatsMap, firstBatch, secondBatch, nodes);
    }

    public void testFailoverRemotePrimaryToDocrepReplica() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        internalCluster().startClusterManagerOnlyNode();
        initDocRepToRemoteMigration();

        logger.info("---> Starting 1 docrep data nodes");
        String docrepNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        assertEquals(internalCluster().client().admin().cluster().prepareGetRepositories().get().repositories().size(), 0);

        logger.info("---> Creating index with 0 replica");
        Settings excludeRemoteNode = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
        createIndex(FAILOVER_REMOTE_TO_DOCREP, excludeRemoteNode);
        ensureGreen(FAILOVER_REMOTE_TO_DOCREP);

        logger.info("---> Starting 1 remote enabled data node");
        addRemote = true;
        String remoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        assertEquals(internalCluster().client().admin().cluster().prepareGetRepositories(REPOSITORY_NAME, REPOSITORY_2_NAME).get().repositories().size(), 2);

        int firstBatch = randomIntBetween(1, 100);
        logger.info("---> Indexing {} docs", firstBatch);
        SyncIndexingService indexingService = new SyncIndexingService(FAILOVER_REMOTE_TO_DOCREP, firstBatch);
        indexingService.startIndexing();

        String primaryShardHostingNode = primaryNodeName(FAILOVER_REMOTE_TO_DOCREP);
        logger.info("---> Moving primary copy from {} to remote enabled node {}", primaryShardHostingNode, remoteNodeName);
        assertAcked(internalCluster().client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(FAILOVER_REMOTE_TO_DOCREP, 0, primaryShardHostingNode, remoteNodeName)).get());
        ensureGreen(FAILOVER_REMOTE_TO_DOCREP);

        logger.info("---> Expanding index to 1 replica copy");
        Settings twoReplicas = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        assertAcked(internalCluster().client().admin().indices().prepareUpdateSettings().setIndices(FAILOVER_REMOTE_TO_DOCREP).setSettings(twoReplicas).get());
        ensureGreen(FAILOVER_REMOTE_TO_DOCREP);

        flush(FAILOVER_REMOTE_TO_DOCREP);
        Map<ShardRouting, ShardStats> shardStatsMap = internalCluster().client().admin().indices().prepareStats(FAILOVER_REMOTE_TO_DOCREP).setDocs(true).get().asMap();
        DiscoveryNodes nodes = internalCluster().client().admin().cluster().prepareState().get().getState().getNodes();
        long initialPrimaryDocCount = 0;
        for (ShardRouting shardRouting: shardStatsMap.keySet()) {
            if (shardRouting.primary()) {
                initialPrimaryDocCount = shardStatsMap.get(shardRouting).getStats().getDocs().getCount();
            }
        }
        assertReplicaAndPrimaryConsistencySingle(shardStatsMap, firstBatch, 0, nodes);

        logger.info("---> Stop remote store enabled node");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(remoteNodeName));
        ensureStableCluster(2);
        ensureYellow(FAILOVER_REMOTE_TO_DOCREP);

        shardStatsMap = internalCluster().client().admin().indices().prepareStats(FAILOVER_REMOTE_TO_DOCREP).setDocs(true).get().asMap();
        long primaryDocCountAfterFailover = 0;
        for (ShardRouting shardRouting: shardStatsMap.keySet()) {
            if (shardRouting.primary()) {
                assertFalse(shardRouting.isAssignedToRemoteStoreNode());
                primaryDocCountAfterFailover = shardStatsMap.get(shardRouting).getStats().getDocs().getCount();
            }
        }
        assertEquals(initialPrimaryDocCount, primaryDocCountAfterFailover);

        logger.info("---> Index some more docs to ensure that the failed over primary is ingesting new docs");
        int secondBatch = randomIntBetween(1, 10);
        logger.info("---> Indexing {} more docs", firstBatch);
        indexingService = new SyncIndexingService(FAILOVER_REMOTE_TO_DOCREP, secondBatch);
        indexingService.startIndexing();
        flush(FAILOVER_REMOTE_TO_DOCREP);

        shardStatsMap = internalCluster().client().admin().indices().prepareStats(FAILOVER_REMOTE_TO_DOCREP).setDocs(true).get().asMap();
        assertEquals(1, shardStatsMap.size());
        shardStatsMap.forEach(
            (shardRouting, shardStats) -> {
                assertEquals(firstBatch + secondBatch, shardStats.getStats().getDocs().getCount());
            }
        );
    }

    private void assertReplicaAndPrimaryConsistencyMultiCopy(Map<ShardRouting, ShardStats> shardStatsMap, int firstBatch, int secondBatch, DiscoveryNodes nodes) throws Exception {
        assertBusy(
            () -> {
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
                        assertEquals("Mismatched doc count. Is this on remote node ? " + remoteNode, firstBatch + secondBatch, shardStats.getDocs().getCount());
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
            }
        );
    }

    private void assertReplicaAndPrimaryConsistencySingle(Map<ShardRouting, ShardStats> shardStatsMap, int initialBatch, int secondBatch, DiscoveryNodes nodes) throws Exception {
        assertBusy(
            () -> {
                long primaryDocCount = 0, replicaDocCount = 0;
                for (ShardRouting shardRouting : shardStatsMap.keySet()) {
                    CommonStats shardStats = shardStatsMap.get(shardRouting).getStats();
                    if (shardRouting.primary()) {
                        primaryDocCount = shardStats.getDocs().getCount();
                        assertTrue(nodes.get(shardRouting.currentNodeId()).isRemoteStoreNode());
                        RemoteSegmentStats remoteSegmentStats = shardStats.getSegments().getRemoteSegmentStats();
                        assertTrue(remoteSegmentStats.getUploadBytesSucceeded() > 0);
                        assertTrue(remoteSegmentStats.getTotalUploadTime() > 0);
                    } else {
                        replicaDocCount = shardStats.getDocs().getCount();
                        assertFalse(nodes.get(shardRouting.currentNodeId()).isRemoteStoreNode());
                        RemoteSegmentStats remoteSegmentStats = shardStats.getSegments().getRemoteSegmentStats();
                        assertEquals(0, remoteSegmentStats.getDownloadBytesStarted());
                        assertEquals(0, remoteSegmentStats.getTotalDownloadTime());
                    }
                }
                assertTrue(replicaDocCount > 0);
                assertEquals(replicaDocCount, initialBatch + secondBatch);
                assertEquals(primaryDocCount, replicaDocCount);
            }
        );
    }

}
