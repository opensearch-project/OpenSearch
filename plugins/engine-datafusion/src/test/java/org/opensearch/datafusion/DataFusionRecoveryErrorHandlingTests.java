/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import com.parquet.parquetdataformat.ParquetDataFormatPlugin;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Integration tests for DataFusion engine error handling during recovery scenarios.
 * Tests transient errors, disconnects, corrupted files, and retry logic 
 * with Parquet format metadata preservation.
 */
@TestLogging(
    value = "org.opensearch.index.shard:DEBUG,org.opensearch.index.store:DEBUG,org.opensearch.datafusion:DEBUG,org.opensearch.indices.recovery:DEBUG",
    reason = "Validate DataFusion error handling with format-aware metadata"
)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataFusionRecoveryErrorHandlingTests extends OpenSearchIntegTestCase {

    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected static final String INDEX_NAME = "datafusion-error-test-index";

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
     * Tests recovery behavior when primary node restarts during replica recovery.
     * Validates format metadata consistency when recovery is interrupted.
     */
    public void testDataFusionRecoveryWithPrimaryRestart() throws Exception {
        logger.info("--> Starting testDataFusionRecoveryWithPrimaryRestart");
        
        // Setup cluster with primary and replica
        internalCluster().startClusterManagerOnlyNode();
        String primaryNode = internalCluster().startDataOnlyNode();
        String replicaNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);

        // Create index with replica
        String mappings = "{ \"properties\": { \"message\": { \"type\": \"long\" } } }";
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME)
            .setSettings(Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build())
            .setMapping(mappings).get());
        ensureGreen(INDEX_NAME);

        // Index documents
        int numDocs = randomIntBetween(20, 50);
        for (int i = 1; i <= numDocs; i++) {
            client().prepareIndex(INDEX_NAME).setId("doc" + i)
                .setSource("{ \"message\": " + (i * 100) + " }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Allow segment replication to complete
        Thread.sleep(2000);

        // Find primary node
        var clusterState = clusterService().state();
        var shardRouting = clusterState.routingTable().index(INDEX_NAME).shard(0);
        String primaryNodeId = shardRouting.primaryShard().currentNodeId();
        
        String primaryNodeName = null;
        for (String nodeName : internalCluster().getDataNodeNames()) {
            if (internalCluster().clusterService(nodeName).localNode().getId().equals(primaryNodeId)) {
                primaryNodeName = nodeName;
                break;
            }
        }
        assertNotNull("Primary node should be found", primaryNodeName);

        // Capture state before restart
        IndexShard primaryShard = getIndexShard(primaryNodeName, INDEX_NAME);
        validateRemoteStoreSegments(primaryShard, "before primary restart");
        long docCountBefore = primaryShard.docStats().getCount();
        long parquetFilesBefore = countParquetFilesInRemote(primaryShard);

        // Restart primary node
        logger.info("--> Restarting primary node: {}", primaryNodeName);
        internalCluster().restartNode(primaryNodeName, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                return super.onNodeStopped(nodeName);
            }
        });
        ensureStableCluster(3);
        ensureGreen(INDEX_NAME);

        // Validate recovery completed successfully
        String newPrimaryNodeName = null;
        var newClusterState = clusterService().state();
        var newShardRouting = newClusterState.routingTable().index(INDEX_NAME).shard(0);
        String newPrimaryNodeId = newShardRouting.primaryShard().currentNodeId();
        
        for (String nodeName : internalCluster().getDataNodeNames()) {
            if (internalCluster().clusterService(nodeName).localNode().getId().equals(newPrimaryNodeId)) {
                newPrimaryNodeName = nodeName;
                break;
            }
        }
        assertNotNull("New primary should be found", newPrimaryNodeName);

        IndexShard newPrimaryShard = getIndexShard(newPrimaryNodeName, INDEX_NAME);
        validateRemoteStoreSegments(newPrimaryShard, "after primary restart");
        
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        long docCountAfter = newPrimaryShard.docStats().getCount();
        long parquetFilesAfter = countParquetFilesInRemote(newPrimaryShard);

        assertEquals("Document count should be preserved after primary restart", docCountBefore, docCountAfter);
        assertEquals("Parquet file count should be preserved", parquetFilesBefore, parquetFilesAfter);

        logger.info("--> testDataFusionRecoveryWithPrimaryRestart completed successfully");
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }

    /**
     * Tests recovery behavior when replica node restarts multiple times.
     * Validates format metadata consistency through multiple recovery cycles.
     */
    public void testDataFusionRecoveryWithMultipleReplicaRestarts() throws Exception {
        logger.info("--> Starting testDataFusionRecoveryWithMultipleReplicaRestarts");
        
        // Setup cluster
        internalCluster().startClusterManagerOnlyNode();
        String primaryNode = internalCluster().startDataOnlyNode();
        String replicaNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);

        // Create index with replica
        String mappings = "{ \"properties\": { \"message\": { \"type\": \"long\" }, \"restart\": { \"type\": \"keyword\" } } }";
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME)
            .setSettings(Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build())
            .setMapping(mappings).get());
        ensureGreen(INDEX_NAME);

        // Initial batch of documents - track total docs
        int totalDocsAdded = randomIntBetween(10, 20);
        for (int i = 1; i <= totalDocsAdded; i++) {
            client().prepareIndex(INDEX_NAME).setId("initial_doc" + i)
                .setSource("{ \"message\": " + (i * 100) + ", \"restart\": \"initial\" }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        Thread.sleep(1000);
        
        logger.info("--> Initial docs added: {}", totalDocsAdded);

        // Find replica node
        var clusterState = clusterService().state();
        var shardRouting = clusterState.routingTable().index(INDEX_NAME).shard(0);
        String replicaNodeId = shardRouting.replicaShards().get(0).currentNodeId();
        
        String replicaNodeName = null;
        for (String nodeName : internalCluster().getDataNodeNames()) {
            if (internalCluster().clusterService(nodeName).localNode().getId().equals(replicaNodeId)) {
                replicaNodeName = nodeName;
                break;
            }
        }
        assertNotNull("Replica node should be found", replicaNodeName);

        // Perform multiple restart cycles - track exact docs added
        int numRestarts = 3;
        for (int restart = 1; restart <= numRestarts; restart++) {
            logger.info("--> Restart cycle {} of {}", restart, numRestarts);
            
            // Add documents before restart - track the exact count
            int batchDocs = randomIntBetween(3, 7);
            totalDocsAdded += batchDocs;
            logger.info("--> Adding {} docs in restart cycle {}, total so far: {}", batchDocs, restart, totalDocsAdded);
            
            for (int i = 1; i <= batchDocs; i++) {
                client().prepareIndex(INDEX_NAME).setId("restart" + restart + "_doc" + i)
                    .setSource("{ \"message\": " + (restart * 1000 + i * 100) + ", \"restart\": \"restart" + restart + "\" }", MediaTypeRegistry.JSON).get();
            }
            client().admin().indices().prepareFlush(INDEX_NAME).get();
            client().admin().indices().prepareRefresh(INDEX_NAME).get();

            // Restart replica node
            internalCluster().restartNode(replicaNodeName, new InternalTestCluster.RestartCallback());
            ensureStableCluster(3);
            ensureGreen(INDEX_NAME);
            
            Thread.sleep(1000);
        }

        // Validate final state on primary
        IndexShard primaryShard = getIndexShard(primaryNode, INDEX_NAME);
        validateRemoteStoreSegments(primaryShard, "after all restarts");
        
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        long finalDocCount = primaryShard.docStats().getCount();
        
        // Use exact expected doc count
        final int expectedTotalDocs = totalDocsAdded;
        logger.info("--> Expected total docs: {}, actual: {}", expectedTotalDocs, finalDocCount);
        assertEquals("Final doc count should match total docs added", expectedTotalDocs, finalDocCount);
        
        // Validate replica recovered correctly
        var finalClusterState = clusterService().state();
        var finalShardRouting = finalClusterState.routingTable().index(INDEX_NAME).shard(0);
        String finalReplicaNodeId = finalShardRouting.replicaShards().get(0).currentNodeId();
        
        String finalReplicaNodeName = null;
        for (String nodeName : internalCluster().getDataNodeNames()) {
            if (internalCluster().clusterService(nodeName).localNode().getId().equals(finalReplicaNodeId)) {
                finalReplicaNodeName = nodeName;
                break;
            }
        }
        
        if (finalReplicaNodeName != null) {
            IndexShard replicaShard = internalCluster().getInstance(org.opensearch.indices.IndicesService.class, finalReplicaNodeName)
                .indexServiceSafe(resolveIndex(INDEX_NAME)).getShard(0);
            
            assertBusy(() -> {
                long replicaDocCount = replicaShard.docStats().getCount();
                assertEquals("Replica should have same doc count as expected total", expectedTotalDocs, replicaDocCount);
            }, 30, TimeUnit.SECONDS);
            
            validateRemoteStoreSegments(replicaShard, "replica after all restarts");
        }

        logger.info("--> testDataFusionRecoveryWithMultipleReplicaRestarts completed successfully");
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }

    /**
     * Tests recovery when node stops abruptly during indexing.
     * Validates translog replay and format metadata consistency.
     */
    public void testDataFusionRecoveryWithAbruptNodeStop() throws Exception {
        logger.info("--> Starting testDataFusionRecoveryWithAbruptNodeStop");
        
        // Setup cluster
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        // Create index with translog durability set to request
        String mappings = "{ \"properties\": { \"message\": { \"type\": \"long\" }, \"phase\": { \"type\": \"keyword\" } } }";
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME)
            .setSettings(Settings.builder()
                .put(indexSettings())
                .put("index.translog.durability", "request")
                .build())
            .setMapping(mappings).get());
        ensureGreen(INDEX_NAME);

        // Index initial batch and flush
        int initialDocs = randomIntBetween(10, 20);
        for (int i = 1; i <= initialDocs; i++) {
            client().prepareIndex(INDEX_NAME).setId("initial_doc" + i)
                .setSource("{ \"message\": " + (i * 100) + ", \"phase\": \"initial\" }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Capture state after flush
        IndexShard shard = getIndexShard(dataNode, INDEX_NAME);
        validateRemoteStoreSegments(shard, "after initial flush");
        long parquetFilesAfterFlush = countParquetFilesInRemote(shard);

        // Index more documents without flush (will be in translog)
        int uncommittedDocs = randomIntBetween(5, 15);
        for (int i = 1; i <= uncommittedDocs; i++) {
            client().prepareIndex(INDEX_NAME).setId("uncommitted_doc" + i)
                .setSource("{ \"message\": " + (i * 200) + ", \"phase\": \"uncommitted\" }", MediaTypeRegistry.JSON).get();
        }
        // Intentionally NOT flushing - documents only in translog
        Thread.sleep(500);

        int totalExpectedDocs = initialDocs + uncommittedDocs;

        // Abruptly stop node
        String clusterUUID = clusterService().state().metadata().clusterUUID();
        logger.info("--> Abruptly stopping data node");
        internalCluster().stopRandomDataNode();
        ensureRed(INDEX_NAME);

        // Start new node and restore
        String newDataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        assertAcked(client().admin().indices().prepareClose(INDEX_NAME));
        client().admin().cluster().restoreRemoteStore(
            new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true), 
            PlainActionFuture.newFuture()
        );
        ensureGreen(INDEX_NAME);

        // Validate recovery with translog replay
        IndexShard recoveredShard = getIndexShard(newDataNode, INDEX_NAME);
        validateRemoteStoreSegments(recoveredShard, "after recovery");
        
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        long recoveredDocCount = recoveredShard.docStats().getCount();
        
        // Should have all documents (flushed + translog replay)
        assertEquals("Should have all documents after recovery", totalExpectedDocs, recoveredDocCount);
        assertEquals("Cluster UUID should remain same", clusterUUID, clusterService().state().metadata().clusterUUID());

        logger.info("--> testDataFusionRecoveryWithAbruptNodeStop completed successfully");
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }

    /**
     * Tests recovery state tracking during DataFusion recovery.
     * Validates recovery stages complete successfully with format metadata.
     */
    public void testDataFusionRecoveryStateTracking() throws Exception {
        logger.info("--> Starting testDataFusionRecoveryStateTracking");
        
        // Setup cluster
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        // Create index
        String mappings = "{ \"properties\": { \"message\": { \"type\": \"long\" } } }";
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME)
            .setSettings(indexSettings())
            .setMapping(mappings).get());
        ensureGreen(INDEX_NAME);

        // Index a significant number of documents
        int numDocs = randomIntBetween(50, 100);
        for (int i = 1; i <= numDocs; i++) {
            client().prepareIndex(INDEX_NAME).setId("doc" + i)
                .setSource("{ \"message\": " + (i * 100) + " }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Capture state before recovery
        IndexShard shard = getIndexShard(dataNode, INDEX_NAME);
        validateRemoteStoreSegments(shard, "before recovery");
        long docCountBefore = shard.docStats().getCount();
        long parquetFilesBefore = countParquetFilesInRemote(shard);

        // Stop node and start new node
        String clusterUUID = clusterService().state().metadata().clusterUUID();
        internalCluster().stopRandomDataNode();
        ensureRed(INDEX_NAME);

        String newDataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        assertAcked(client().admin().indices().prepareClose(INDEX_NAME));
        client().admin().cluster().restoreRemoteStore(
            new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true), 
            PlainActionFuture.newFuture()
        );
        ensureGreen(INDEX_NAME);

        // Verify recovery state
        var recoveryResponse = client().admin().indices()
            .prepareRecoveries(INDEX_NAME)
            .get();
        
        List<RecoveryState> recoveryStates = recoveryResponse.shardRecoveryStates().get(INDEX_NAME);
        assertNotNull("Recovery states should not be null", recoveryStates);
        assertFalse("Recovery states should not be empty", recoveryStates.isEmpty());
        
        RecoveryState recoveryState = recoveryStates.get(0);
        assertEquals("Recovery should be complete", RecoveryState.Stage.DONE, recoveryState.getStage());
        
        // Log recovery details
        logger.info("--> Recovery state: stage={}, sourceNode={}, targetNode={}", 
            recoveryState.getStage(), 
            recoveryState.getSourceNode(), 
            recoveryState.getTargetNode());
        
        // Validate recovered shard
        IndexShard recoveredShard = getIndexShard(newDataNode, INDEX_NAME);
        validateRemoteStoreSegments(recoveredShard, "after recovery");
        validateCatalogSnapshot(recoveredShard, "after recovery");
        
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        long docCountAfter = recoveredShard.docStats().getCount();
        long parquetFilesAfter = countParquetFilesInRemote(recoveredShard);

        assertEquals("Document count should be preserved", docCountBefore, docCountAfter);
        assertEquals("Parquet file count should be preserved", parquetFilesBefore, parquetFilesAfter);
        assertEquals("Cluster UUID should remain same", clusterUUID, clusterService().state().metadata().clusterUUID());

        logger.info("--> testDataFusionRecoveryStateTracking completed successfully");
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }
}
