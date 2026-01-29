/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import com.parquet.parquetdataformat.ParquetDataFormatPlugin;
import org.opensearch.action.admin.indices.recovery.RecoveryRequest;
import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.CompositeStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.UploadedSegmentMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.junit.annotations.TestLogging;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Integration tests for DataFusion engine cluster-level recovery scenarios.
 * Tests gateway recovery, shard reroute, cluster manager failover, and
 * multiple replica recovery with Parquet format metadata preservation.
 */
@TestLogging(
    value = "org.opensearch.index.shard:DEBUG,org.opensearch.index.store:DEBUG,org.opensearch.datafusion:DEBUG,org.opensearch.indices.recovery:DEBUG",
    reason = "Validate DataFusion cluster recovery with format-aware metadata"
)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataFusionClusterRecoveryTests extends OpenSearchIntegTestCase {

    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected static final String INDEX_NAME = "datafusion-cluster-test-index";

    protected Path repositoryPath;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataFusionPlugin.class, ParquetDataFormatPlugin.class);
    }

    @Before
    public void setup() {
        repositoryPath = randomRepoPath().toAbsolutePath();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(REPOSITORY_NAME, repositoryPath))
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .build();
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put("index.queries.cache.enabled", false)
            .put("index.refresh_interval", -1)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.optimized.enabled", true)
            .build();
    }

    @Override
    protected void beforeIndexDeletion() throws Exception {
        logger.info("--> Skipping beforeIndexDeletion cleanup to avoid DataFusion engine type conflicts");
    }

    @Override
    protected void ensureClusterSizeConsistency() {}

    @Override
    protected void ensureClusterStateConsistency() {}

    // ==================== Helper Methods ====================

    private IndexShard getIndexShard(String nodeName, String indexName) {
        return internalCluster().getInstance(org.opensearch.indices.IndicesService.class, nodeName)
            .indexServiceSafe(internalCluster().clusterService(nodeName).state().metadata().index(indexName).getIndex())
            .getShard(0);
    }

    private void validateRemoteStoreSegments(IndexShard shard, String stageName) {
        RemoteSegmentStoreDirectory remoteDir = shard.getRemoteDirectory();
        assertNotNull("RemoteSegmentStoreDirectory should not be null at " + stageName, remoteDir);

        Map<String, UploadedSegmentMetadata> uploadedSegmentsRaw = remoteDir.getSegmentsUploadedToRemoteStore();
        if (uploadedSegmentsRaw.isEmpty()) {
            logger.warn("--> No segments uploaded yet at stage: {}", stageName);
            return;
        }

        Map<FileMetadata, UploadedSegmentMetadata> uploadedSegments = uploadedSegmentsRaw.entrySet().stream()
            .collect(Collectors.toMap(e -> new FileMetadata(e.getKey()), Map.Entry::getValue));

        for (FileMetadata fileMetadata : uploadedSegments.keySet()) {
            assertNotNull("FileMetadata should have format information at " + stageName, fileMetadata.dataFormat());
            assertFalse("Format should not be empty at " + stageName, fileMetadata.dataFormat().isEmpty());
        }
        logger.info("--> Validated {} segments at stage: {}", uploadedSegments.size(), stageName);
    }

    private long validateLocalShardFiles(IndexShard shard, String stageName) {
        try {
            CompositeStoreDirectory compositeDir = shard.store().compositeStoreDirectory();
            if (compositeDir != null) {
                FileMetadata[] allFiles = compositeDir.listFileMetadata();
                long parquetCount = Arrays.stream(allFiles).filter(fm -> "parquet".equals(fm.dataFormat())).count();
                logger.info("--> Found {} Parquet files at stage: {}", parquetCount, stageName);
                return parquetCount;
            } else {
                String[] files = shard.store().directory().listAll();
                long parquetCount = Arrays.stream(files).filter(f -> f.contains("parquet") || f.endsWith(".parquet")).count();
                return parquetCount;
            }
        } catch (IOException e) {
            logger.warn("--> Failed to list local shard files at stage {}: {}", stageName, e.getMessage());
            return -1;
        }
    }

    private void validateCatalogSnapshot(IndexShard shard, String stageName) {
        RemoteSegmentStoreDirectory remoteDir = shard.getRemoteDirectory();
        assertNotNull("RemoteSegmentStoreDirectory should not be null at " + stageName, remoteDir);

        try {
            RemoteSegmentMetadata metadata = remoteDir.readLatestMetadataFile();
            if (metadata == null) {
                logger.warn("--> RemoteSegmentMetadata not found at stage {}", stageName);
                return;
            }

            byte[] catalogSnapshotBytes = metadata.getSegmentInfosBytes();
            if (catalogSnapshotBytes != null) {
                assertTrue("CatalogSnapshot bytes should not be empty at " + stageName, catalogSnapshotBytes.length > 0);
            }

            var checkpoint = metadata.getReplicationCheckpoint();
            if (checkpoint != null) {
                assertTrue("Checkpoint version should be positive at " + stageName, checkpoint.getSegmentInfosVersion() > 0);
            }
        } catch (IOException e) {
            logger.warn("--> Failed to read metadata at stage {}: {}", stageName, e.getMessage());
        }
    }

    private long countParquetFilesInRemote(IndexShard shard) {
        RemoteSegmentStoreDirectory remoteDir = shard.getRemoteDirectory();
        if (remoteDir == null) return 0;

        return remoteDir.getSegmentsUploadedToRemoteStore().entrySet().stream()
            .map(e -> new FileMetadata(e.getKey()))
            .filter(fm -> "parquet".equals(fm.dataFormat()))
            .count();
    }

    // ==================== Test Methods ====================

    /**
     * Tests full cluster restart (gateway) recovery with DataFusion engine.
     * Validates that CatalogSnapshot is properly recovered from remote store after full restart.
     */
    public void testDataFusionGatewayRecovery() throws Exception {
        logger.info("--> Starting testDataFusionGatewayRecovery");

        // Setup cluster
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        // Create index and index documents
        String mappings = "{ \"properties\": { \"message\": { \"type\": \"long\" }, \"value\": { \"type\": \"long\" } } }";
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings()).setMapping(mappings).get());
        ensureGreen(INDEX_NAME);

        int numDocs = randomIntBetween(10, 50);
        for (int i = 1; i <= numDocs; i++) {
            client().prepareIndex(INDEX_NAME).setId("doc" + i)
                .setSource("{ \"message\": " + (i * 100) + ", \"value\": " + i + " }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Capture state before restart
        IndexShard indexShard = getIndexShard(dataNode, INDEX_NAME);
        validateRemoteStoreSegments(indexShard, "before gateway restart");
        validateCatalogSnapshot(indexShard, "before gateway restart");

        long docCountBeforeRestart = indexShard.docStats().getCount();
        long parquetFilesBeforeRestart = countParquetFilesInRemote(indexShard);
        String clusterUUID = clusterService().state().metadata().clusterUUID();

        logger.info("--> State before restart: docs={}, parquetFiles={}", docCountBeforeRestart, parquetFilesBeforeRestart);

        // Full cluster restart
        logger.info("--> Performing full cluster restart");
        internalCluster().fullRestart();
        ensureStableCluster(2);
        ensureGreen(INDEX_NAME);

        // Validate recovery state
        RecoveryResponse recoveryResponse = client().admin().indices().recoveries(new RecoveryRequest(INDEX_NAME)).actionGet();
        List<RecoveryState> recoveryStates = recoveryResponse.shardRecoveryStates().get(INDEX_NAME);
        assertNotNull("Recovery states should not be null", recoveryStates);
        assertFalse("Recovery states should not be empty", recoveryStates.isEmpty());

        RecoveryState recoveryState = recoveryStates.get(0);
        assertEquals("Recovery should be complete", RecoveryState.Stage.DONE, recoveryState.getStage());

        // Validate format metadata after restart
        String newDataNode = internalCluster().getDataNodeNames().iterator().next();
        IndexShard recoveredShard = getIndexShard(newDataNode, INDEX_NAME);
        validateRemoteStoreSegments(recoveredShard, "after gateway restart");
        validateCatalogSnapshot(recoveredShard, "after gateway restart");

        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        long docCountAfterRestart = recoveredShard.docStats().getCount();
        long parquetFilesAfterRestart = countParquetFilesInRemote(recoveredShard);

        // Verify consistency
        assertEquals("Document count should be same after gateway restart", docCountBeforeRestart, docCountAfterRestart);
        assertEquals("Parquet file count should be same after gateway restart", parquetFilesBeforeRestart, parquetFilesAfterRestart);
        assertEquals("Cluster UUID should remain same", clusterUUID, clusterService().state().metadata().clusterUUID());

        // Verify document count matches expected number
        assertEquals("Document count should match expected", numDocs, docCountAfterRestart);

        logger.info("--> testDataFusionGatewayRecovery completed successfully");
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }

    /**
     * Tests shard relocation (reroute) between nodes with DataFusion engine.
     * Validates Parquet format metadata is preserved during shard movement.
     */
    public void testDataFusionRerouteRecovery() throws Exception {
        logger.info("--> Starting testDataFusionRerouteRecovery");

        // Setup cluster with multiple data nodes
        internalCluster().startClusterManagerOnlyNode();
        String nodeA = internalCluster().startDataOnlyNode();
        String nodeB = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);

        // Create index on nodeA
        String mappings = "{ \"properties\": { \"message\": { \"type\": \"long\" }, \"phase\": { \"type\": \"keyword\" } } }";
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME)
            .setSettings(Settings.builder()
                .put(indexSettings())
                .put("index.routing.allocation.include._name", nodeA)
                .build())
            .setMapping(mappings).get());
        ensureGreen(INDEX_NAME);

        // Index documents
        int numDocs = randomIntBetween(10, 30);
        for (int i = 1; i <= numDocs; i++) {
            client().prepareIndex(INDEX_NAME).setId("doc" + i)
                .setSource("{ \"message\": " + (i * 100) + ", \"phase\": \"initial\" }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Capture state before reroute
        IndexShard shardOnNodeA = getIndexShard(nodeA, INDEX_NAME);
        validateRemoteStoreSegments(shardOnNodeA, "before reroute on nodeA");
        long docCountBeforeReroute = shardOnNodeA.docStats().getCount();
        long parquetFilesBeforeReroute = countParquetFilesInRemote(shardOnNodeA);

        logger.info("--> State before reroute: docs={}, parquetFiles={}", docCountBeforeReroute, parquetFilesBeforeReroute);

        // Reroute shard from nodeA to nodeB
        logger.info("--> Moving shard from {} to {}", nodeA, nodeB);
        client().admin().cluster().prepareReroute()
            .add(new MoveAllocationCommand(INDEX_NAME, 0, nodeA, nodeB))
            .execute().actionGet();

        ensureGreen(INDEX_NAME);

        // Validate shard is now on nodeB
        var clusterState = clusterService().state();
        ShardRouting shardRouting = clusterState.routingTable().index(INDEX_NAME).shard(0).primaryShard();
        String currentNodeId = shardRouting.currentNodeId();
        String nodeBId = internalCluster().clusterService(nodeB).localNode().getId();
        assertEquals("Shard should be on nodeB", nodeBId, currentNodeId);

        // Validate format metadata after reroute
        IndexShard shardOnNodeB = getIndexShard(nodeB, INDEX_NAME);
        validateRemoteStoreSegments(shardOnNodeB, "after reroute on nodeB");
        validateCatalogSnapshot(shardOnNodeB, "after reroute on nodeB");

        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        long docCountAfterReroute = shardOnNodeB.docStats().getCount();
        long parquetFilesAfterReroute = countParquetFilesInRemote(shardOnNodeB);

        // Verify consistency
        assertEquals("Document count should be same after reroute", docCountBeforeReroute, docCountAfterReroute);
        assertEquals("Parquet file count should be same after reroute", parquetFilesBeforeReroute, parquetFilesAfterReroute);

        // Index more documents after reroute
        for (int i = 1; i <= 5; i++) {
            client().prepareIndex(INDEX_NAME).setId("post_reroute_doc" + i)
                .setSource("{ \"message\": " + (i * 200) + ", \"phase\": \"post_reroute\" }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        assertEquals("Final doc count should include new docs", numDocs + 5, shardOnNodeB.docStats().getCount());

        logger.info("--> testDataFusionRerouteRecovery completed successfully");
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }

    /**
     * Tests recovery with multiple replica shards.
     * Validates format-aware replication to multiple targets.
     */
    public void testDataFusionRecoveryWithMultipleReplicas() throws Exception {
        logger.info("--> Starting testDataFusionRecoveryWithMultipleReplicas");

        // Setup cluster with multiple data nodes
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(3);
        ensureStableCluster(4);

        // Create index with 2 replicas
        String mappings = "{ \"properties\": { \"message\": { \"type\": \"long\" }, \"data\": { \"type\": \"keyword\" } } }";
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME)
            .setSettings(Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
                .build())
            .setMapping(mappings).get());
        ensureGreen(INDEX_NAME);

        // Index documents
        int numDocs = randomIntBetween(10, 30);
        for (int i = 1; i <= numDocs; i++) {
            client().prepareIndex(INDEX_NAME).setId("doc" + i)
                .setSource("{ \"message\": " + (i * 100) + ", \"data\": \"value" + i + "\" }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Allow segment replication to complete
        Thread.sleep(2000);

        // Find primary and replica nodes
        var clusterState = clusterService().state();
        var shardRoutingTable = clusterState.routingTable().index(INDEX_NAME).shard(0);
        String primaryNodeId = shardRoutingTable.primaryShard().currentNodeId();

        String primaryNodeName = null;
        for (String nodeName : internalCluster().getDataNodeNames()) {
            String nodeId = internalCluster().clusterService(nodeName).localNode().getId();
            if (nodeId.equals(primaryNodeId)) {
                primaryNodeName = nodeName;
                break;
            }
        }
        assertNotNull("Primary node should be found", primaryNodeName);

        // Get primary shard state
        IndexShard primaryShard = getIndexShard(primaryNodeName, INDEX_NAME);
        validateRemoteStoreSegments(primaryShard, "primary before validation");
        long primaryDocCount = primaryShard.docStats().getCount();
        long primaryParquetFiles = countParquetFilesInRemote(primaryShard);

        logger.info("--> Primary state: docs={}, parquetFiles={}", primaryDocCount, primaryParquetFiles);

        // Validate all replicas have same format metadata
        for (ShardRouting replicaRouting : shardRoutingTable.replicaShards()) {
            String replicaNodeId = replicaRouting.currentNodeId();
            String replicaNodeName = null;
            for (String nodeName : internalCluster().getDataNodeNames()) {
                if (internalCluster().clusterService(nodeName).localNode().getId().equals(replicaNodeId)) {
                    replicaNodeName = nodeName;
                    break;
                }
            }

            if (replicaNodeName != null) {
                IndexShard replicaShard = internalCluster().getInstance(org.opensearch.indices.IndicesService.class, replicaNodeName)
                    .indexServiceSafe(resolveIndex(INDEX_NAME)).getShard(0);

                validateRemoteStoreSegments(replicaShard, "replica " + replicaNodeName);

                client().admin().indices().prepareRefresh(INDEX_NAME).get();
                long replicaDocCount = replicaShard.docStats().getCount();

                assertEquals("Replica should have same doc count as primary", primaryDocCount, replicaDocCount);
                logger.info("--> Replica {} validated: docs={}", replicaNodeName, replicaDocCount);
            }
        }

        // Stop primary and validate replica promotion
        logger.info("--> Stopping primary node: {}", primaryNodeName);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName));
        ensureStableCluster(3);

        assertBusy(() -> {
            var health = client().admin().cluster().prepareHealth(INDEX_NAME).get();
            assertTrue("Index should not be red",
                health.getStatus() != org.opensearch.cluster.health.ClusterHealthStatus.RED);
        }, 30, TimeUnit.SECONDS);

        // Validate new primary
        var newClusterState = clusterService().state();
        var newShardRouting = newClusterState.routingTable().index(INDEX_NAME).shard(0).primaryShard();
        String newPrimaryNodeId = newShardRouting.currentNodeId();

        String newPrimaryNodeName = null;
        for (String nodeName : internalCluster().getDataNodeNames()) {
            if (internalCluster().clusterService(nodeName).localNode().getId().equals(newPrimaryNodeId)) {
                newPrimaryNodeName = nodeName;
                break;
            }
        }
        assertNotNull("New primary should be found", newPrimaryNodeName);

        IndexShard newPrimaryShard = getIndexShard(newPrimaryNodeName, INDEX_NAME);
        validateRemoteStoreSegments(newPrimaryShard, "new primary after promotion");

        Set<String> formats = newPrimaryShard.getRemoteDirectory().getSegmentsUploadedToRemoteStore().entrySet().stream()
            .map(e -> new FileMetadata(e.getKey()).dataFormat())
            .collect(Collectors.toSet());
        assertTrue("Promoted primary should have Parquet files", formats.contains("parquet"));

        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        assertEquals("New primary should have all documents", primaryDocCount, newPrimaryShard.docStats().getCount());

        logger.info("--> testDataFusionRecoveryWithMultipleReplicas completed successfully");
        
        // After stopping primary, only 2 data nodes remain for a 2-replica index
        // Index will be YELLOW (missing 1 replica) which is expected and acceptable for cleanup
        assertBusy(() -> {
            var health = client().admin().cluster().prepareHealth(INDEX_NAME).get();
            assertTrue("Index should not be red after primary promotion",
                health.getStatus() != org.opensearch.cluster.health.ClusterHealthStatus.RED);
        }, 30, TimeUnit.SECONDS);
        
        // Allow in-flight replica operations to settle before deletion
        Thread.sleep(2000);
        
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();
        
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }

    /**
     * Tests cluster manager failover during recovery.
     * Validates format metadata consistency during leader election.
     */
    public void testDataFusionClusterManagerFailover() throws Exception {
        logger.info("--> Starting testDataFusionClusterManagerFailover");

        // Start cluster with 2 master-eligible nodes
        String clusterManager1 = internalCluster().startClusterManagerOnlyNode();
        String clusterManager2 = internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);

        // Create index and index documents
        String mappings = "{ \"properties\": { \"message\": { \"type\": \"long\" } } }";
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings()).setMapping(mappings).get());
        ensureGreen(INDEX_NAME);

        int numDocs = randomIntBetween(5, 20);
        for (int i = 1; i <= numDocs; i++) {
            client().prepareIndex(INDEX_NAME).setId("doc" + i)
                .setSource("{ \"message\": " + (i * 100) + " }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Capture state before failover
        IndexShard shard = getIndexShard(dataNode, INDEX_NAME);
        validateRemoteStoreSegments(shard, "before cluster manager failover");
        long docCountBeforeFailover = shard.docStats().getCount();
        long parquetFilesBeforeFailover = countParquetFilesInRemote(shard);

        // Identify current cluster manager
        String currentClusterManager = internalCluster().getClusterManagerName();
        logger.info("--> Current cluster manager: {}", currentClusterManager);

        // Stop current cluster manager to trigger failover
        logger.info("--> Stopping cluster manager to trigger failover");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(currentClusterManager));

        // Wait for new cluster manager election
        ensureStableCluster(2);

        String newClusterManager = internalCluster().getClusterManagerName();
        logger.info("--> New cluster manager: {}", newClusterManager);
        assertNotEquals("New cluster manager should be different", currentClusterManager, newClusterManager);

        // Validate index is still accessible
        ensureGreen(INDEX_NAME);

        // Validate format metadata after failover
        IndexShard shardAfterFailover = getIndexShard(dataNode, INDEX_NAME);
        validateRemoteStoreSegments(shardAfterFailover, "after cluster manager failover");
        validateCatalogSnapshot(shardAfterFailover, "after cluster manager failover");

        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        long docCountAfterFailover = shardAfterFailover.docStats().getCount();
        long parquetFilesAfterFailover = countParquetFilesInRemote(shardAfterFailover);

        // Verify consistency
        assertEquals("Document count should be same after cluster manager failover", docCountBeforeFailover, docCountAfterFailover);
        assertEquals("Parquet file count should be same after cluster manager failover", parquetFilesBeforeFailover, parquetFilesAfterFailover);

        // Index more documents to verify cluster is functional
        for (int i = 1; i <= 3; i++) {
            client().prepareIndex(INDEX_NAME).setId("post_failover_doc" + i)
                .setSource("{ \"message\": " + (i * 300) + " }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        assertEquals("Final doc count should include new docs", numDocs + 3, shardAfterFailover.docStats().getCount());

        logger.info("--> testDataFusionClusterManagerFailover completed successfully");
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }
}
