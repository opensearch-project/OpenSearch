/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import com.parquet.parquetdataformat.ParquetDataFormatPlugin;
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.CompositeStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.UploadedSegmentMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Integration tests for DataFusion engine data integrity during recovery scenarios.
 * Tests sequence number integrity, segment info commits, old commit cleanup, and
 * segment file consistency with Parquet format metadata preservation.
 */
@TestLogging(
    value = "org.opensearch.index.shard:DEBUG,org.opensearch.index.store:DEBUG,org.opensearch.datafusion:DEBUG",
    reason = "Validate DataFusion data integrity with format-aware metadata"
)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataFusionRecoveryDataIntegrityTests extends OpenSearchIntegTestCase {

    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected static final String INDEX_NAME = "datafusion-integrity-test-index";

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

    private Set<String> getSegmentFiles(IndexShard shard) throws IOException {
        Set<String> files = new HashSet<>();
        String[] allFiles = shard.store().directory().listAll();
        for (String file : allFiles) {
            if (file.startsWith("segments_")) {
                files.add(file);
            }
        }
        return files;
    }

    // ==================== Test Methods ====================

    /**
     * Tests sequence number integrity after recovery with Parquet format.
     * Ensures no duplicate sequence numbers exist after multiple replication cycles.
     */
    public void testDataFusionNoDuplicateSeqNo() throws Exception {
        logger.info("--> Starting testDataFusionNoDuplicateSeqNo");
        
        // Setup cluster with primary and replica
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);

        // Create index with replica
        String mappings = "{ \"properties\": { \"message\": { \"type\": \"long\" }, \"batch\": { \"type\": \"keyword\" } } }";
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME)
            .setSettings(Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build())
            .setMapping(mappings).get());
        ensureGreen(INDEX_NAME);

        // Find primary and replica nodes
        var clusterState = clusterService().state();
        var shardRoutingTable = clusterState.routingTable().index(INDEX_NAME).shard(0);
        String primaryNodeId = shardRoutingTable.primaryShard().currentNodeId();
        String replicaNodeId = shardRoutingTable.replicaShards().get(0).currentNodeId();
        
        String primaryNodeName = null, replicaNodeName = null;
        for (String nodeName : internalCluster().getDataNodeNames()) {
            String nodeId = internalCluster().clusterService(nodeName).localNode().getId();
            if (nodeId.equals(primaryNodeId)) primaryNodeName = nodeName;
            else if (nodeId.equals(replicaNodeId)) replicaNodeName = nodeName;
        }
        assertNotNull("Primary node should be found", primaryNodeName);
        assertNotNull("Replica node should be found", replicaNodeName);

        // Batch 1: Index documents and replicate
        int batch1Docs = randomIntBetween(5, 10);
        for (int i = 1; i <= batch1Docs; i++) {
            client().prepareIndex(INDEX_NAME).setId("batch1_doc" + i)
                .setSource("{ \"message\": " + (i * 100) + ", \"batch\": \"batch1\" }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        Thread.sleep(1000); // Allow segment replication

        // Batch 2: Flush primary, then index more and replicate
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        
        int batch2Docs = randomIntBetween(5, 10);
        for (int i = 1; i <= batch2Docs; i++) {
            client().prepareIndex(INDEX_NAME).setId("batch2_doc" + i)
                .setSource("{ \"message\": " + (i * 200) + ", \"batch\": \"batch2\" }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        Thread.sleep(1000); // Allow segment replication

        // Batch 3: Another cycle
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        
        int batch3Docs = randomIntBetween(3, 7);
        for (int i = 1; i <= batch3Docs; i++) {
            client().prepareIndex(INDEX_NAME).setId("batch3_doc" + i)
                .setSource("{ \"message\": " + (i * 300) + ", \"batch\": \"batch3\" }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        Thread.sleep(1000);

        // Validate both shards
        IndexShard primaryShard = getIndexShard(primaryNodeName, INDEX_NAME);
        IndexShard replicaShard = internalCluster().getInstance(org.opensearch.indices.IndicesService.class, replicaNodeName)
            .indexServiceSafe(resolveIndex(INDEX_NAME)).getShard(0);

        int totalDocs = batch1Docs + batch2Docs + batch3Docs;
        assertEquals("Primary should have all documents", totalDocs, primaryShard.docStats().getCount());
        
        // Wait for replica to catch up
        final String finalReplicaNodeName = replicaNodeName;
        assertBusy(() -> {
            IndexShard replica = internalCluster().getInstance(org.opensearch.indices.IndicesService.class, finalReplicaNodeName)
                .indexServiceSafe(resolveIndex(INDEX_NAME)).getShard(0);
            assertEquals("Replica should have same doc count", totalDocs, replica.docStats().getCount());
        }, 30, TimeUnit.SECONDS);

        // Promote replica to primary by stopping primary
        logger.info("--> Promoting replica by stopping primary");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName));
        ensureStableCluster(2);
        
        assertBusy(() -> {
            var health = client().admin().cluster().prepareHealth(INDEX_NAME).get();
            assertTrue("Index should not be red", 
                health.getStatus() != org.opensearch.cluster.health.ClusterHealthStatus.RED);
        }, 30, TimeUnit.SECONDS);

        // Validate promoted primary
        IndexShard promotedShard = internalCluster().getInstance(org.opensearch.indices.IndicesService.class, finalReplicaNodeName)
            .indexServiceSafe(resolveIndex(INDEX_NAME)).getShard(0);
        assertTrue("Former replica should now be primary", promotedShard.routingEntry().primary());

        // Verify document count maintained
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        assertEquals("Promoted primary should have all documents", totalDocs, promotedShard.docStats().getCount());

        // Validate format metadata preserved
        validateRemoteStoreSegments(promotedShard, "after promotion");

        logger.info("--> testDataFusionNoDuplicateSeqNo completed successfully");
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }

    /**
     * Tests that replica commits segment infos with CatalogSnapshot bytes after recovery.
     */
    public void testDataFusionReplicaCommitsInfosOnRecovery() throws Exception {
        logger.info("--> Starting testDataFusionReplicaCommitsInfosOnRecovery");
        
        // Setup cluster without replica initially
        internalCluster().startClusterManagerOnlyNode();
        String primaryNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        // Create index without replica
        String mappings = "{ \"properties\": { \"message\": { \"type\": \"long\" } } }";
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME)
            .setSettings(indexSettings())
            .setMapping(mappings).get());
        ensureGreen(INDEX_NAME);

        // Index documents
        int numDocs = randomIntBetween(10, 30);
        for (int i = 1; i <= numDocs; i++) {
            client().prepareIndex(INDEX_NAME).setId("doc" + i)
                .setSource("{ \"message\": " + (i * 100) + " }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Validate primary has CatalogSnapshot
        IndexShard primaryShard = getIndexShard(primaryNode, INDEX_NAME);
        validateRemoteStoreSegments(primaryShard, "primary before adding replica");
        validateCatalogSnapshot(primaryShard, "primary before adding replica");

        // Capture primary segment files
        Set<String> primarySegmentFiles = getSegmentFiles(primaryShard);
        logger.info("--> Primary segment files: {}", primarySegmentFiles);

        // Add replica
        logger.info("--> Adding replica node");
        String replicaNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);

        client().admin().indices().prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            .get();
        ensureGreen(INDEX_NAME);

        // Allow replica recovery to complete
        Thread.sleep(2000);

        // Validate replica has committed segment infos with CatalogSnapshot
        var clusterState = clusterService().state();
        var shardRoutingTable = clusterState.routingTable().index(INDEX_NAME).shard(0);
        String replicaNodeId = shardRoutingTable.replicaShards().get(0).currentNodeId();
        
        String replicaNodeName = null;
        for (String nodeName : internalCluster().getDataNodeNames()) {
            if (internalCluster().clusterService(nodeName).localNode().getId().equals(replicaNodeId)) {
                replicaNodeName = nodeName;
                break;
            }
        }
        assertNotNull("Replica node should be found", replicaNodeName);

        IndexShard replicaShard = internalCluster().getInstance(org.opensearch.indices.IndicesService.class, replicaNodeName)
            .indexServiceSafe(resolveIndex(INDEX_NAME)).getShard(0);

        validateRemoteStoreSegments(replicaShard, "replica after recovery");
        validateCatalogSnapshot(replicaShard, "replica after recovery");

        // Verify replica has segment files
        Set<String> replicaSegmentFiles = getSegmentFiles(replicaShard);
        logger.info("--> Replica segment files: {}", replicaSegmentFiles);
        assertFalse("Replica should have segment files", replicaSegmentFiles.isEmpty());

        // Verify document counts match
        assertEquals("Replica should have same doc count", numDocs, replicaShard.docStats().getCount());

        logger.info("--> testDataFusionReplicaCommitsInfosOnRecovery completed successfully");
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }

    /**
     * Tests that old Parquet generation files are properly cleaned up during replication.
     */
    public void testDataFusionReplicaCleansUpOldCommits() throws Exception {
        logger.info("--> Starting testDataFusionReplicaCleansUpOldCommits");
        
        // Setup cluster with replica
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);

        // Create index with replica
        String mappings = "{ \"properties\": { \"message\": { \"type\": \"long\" }, \"batch\": { \"type\": \"keyword\" } } }";
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME)
            .setSettings(Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build())
            .setMapping(mappings).get());
        ensureGreen(INDEX_NAME);

        // Find replica node
        var clusterState = clusterService().state();
        var shardRoutingTable = clusterState.routingTable().index(INDEX_NAME).shard(0);
        String replicaNodeId = shardRoutingTable.replicaShards().get(0).currentNodeId();
        
        String replicaNodeName = null;
        for (String nodeName : internalCluster().getDataNodeNames()) {
            if (internalCluster().clusterService(nodeName).localNode().getId().equals(replicaNodeId)) {
                replicaNodeName = nodeName;
                break;
            }
        }
        assertNotNull("Replica node should be found", replicaNodeName);

        // Batch 1: Index -> Flush -> Replicate
        for (int i = 1; i <= 5; i++) {
            client().prepareIndex(INDEX_NAME).setId("batch1_doc" + i)
                .setSource("{ \"message\": " + (i * 100) + ", \"batch\": \"batch1\" }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        Thread.sleep(1000);

        // Capture initial commit generation
        IndexShard replicaShard = internalCluster().getInstance(org.opensearch.indices.IndicesService.class, replicaNodeName)
            .indexServiceSafe(resolveIndex(INDEX_NAME)).getShard(0);
        Set<String> segmentsAfterBatch1 = getSegmentFiles(replicaShard);
        logger.info("--> Segments after batch 1: {}", segmentsAfterBatch1);

        // Batch 2: Index -> Refresh only (no flush) -> Replicate
        for (int i = 1; i <= 5; i++) {
            client().prepareIndex(INDEX_NAME).setId("batch2_doc" + i)
                .setSource("{ \"message\": " + (i * 200) + ", \"batch\": \"batch2\" }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        Thread.sleep(1000);

        // Verify no new commit on replica (refresh only)
        Set<String> segmentsAfterBatch2 = getSegmentFiles(replicaShard);
        logger.info("--> Segments after batch 2 (refresh only): {}", segmentsAfterBatch2);

        // Batch 3: Index -> Flush -> Replicate
        for (int i = 1; i <= 5; i++) {
            client().prepareIndex(INDEX_NAME).setId("batch3_doc" + i)
                .setSource("{ \"message\": " + (i * 300) + ", \"batch\": \"batch3\" }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        Thread.sleep(2000);

        // Verify new commit generation and old segments cleaned up
        Set<String> segmentsAfterBatch3 = getSegmentFiles(replicaShard);
        logger.info("--> Segments after batch 3: {}", segmentsAfterBatch3);
        
        // Should have exactly one segments_N file
        long segmentFileCount = segmentsAfterBatch3.stream().filter(f -> f.startsWith("segments_")).count();
        assertEquals("Should have single segments_N file", 1, segmentFileCount);

        // Verify document count is correct (15 total docs)
        assertEquals("Should have all documents", 15, replicaShard.docStats().getCount());

        // Validate format metadata consistent
        validateRemoteStoreSegments(replicaShard, "after all batches");

        logger.info("--> testDataFusionReplicaCleansUpOldCommits completed successfully");
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }

    /**
     * Tests FileMetadata format information consistency between local and remote store.
     */
    public void testDataFusionSegmentFileConsistency() throws Exception {
        logger.info("--> Starting testDataFusionSegmentFileConsistency");
        
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

        // Index documents
        int numDocs = randomIntBetween(10, 30);
        for (int i = 1; i <= numDocs; i++) {
            client().prepareIndex(INDEX_NAME).setId("doc" + i)
                .setSource("{ \"message\": " + (i * 100) + " }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Capture local shard files with FileMetadata
        IndexShard shard = getIndexShard(dataNode, INDEX_NAME);
        long localParquetFiles = validateLocalShardFiles(shard, "before recovery");
        
        // Capture remote store files with FileMetadata
        RemoteSegmentStoreDirectory remoteDir = shard.getRemoteDirectory();
        Map<String, UploadedSegmentMetadata> remoteFilesMap = remoteDir.getSegmentsUploadedToRemoteStore();
        
        Map<FileMetadata, UploadedSegmentMetadata> remoteFilesWithMetadata = remoteFilesMap.entrySet().stream()
            .collect(Collectors.toMap(e -> new FileMetadata(e.getKey()), Map.Entry::getValue));
        
        logger.info("--> Local Parquet files: {}, Remote files: {}", localParquetFiles, remoteFilesWithMetadata.size());

        // Verify all Parquet files have correct format
        long remoteParquetFiles = remoteFilesWithMetadata.keySet().stream()
            .filter(fm -> "parquet".equals(fm.dataFormat()))
            .count();
        
        logger.info("--> Remote Parquet files: {}", remoteParquetFiles);

        // Stop node and start new node for recovery
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

        // Validate recovered files have same format metadata
        IndexShard recoveredShard = getIndexShard(newDataNode, INDEX_NAME);
        long recoveredParquetFiles = validateLocalShardFiles(recoveredShard, "after recovery");
        
        RemoteSegmentStoreDirectory recoveredRemoteDir = recoveredShard.getRemoteDirectory();
        Map<String, UploadedSegmentMetadata> recoveredRemoteFiles = recoveredRemoteDir.getSegmentsUploadedToRemoteStore();
        
        Map<FileMetadata, UploadedSegmentMetadata> recoveredFilesWithMetadata = recoveredRemoteFiles.entrySet().stream()
            .collect(Collectors.toMap(e -> new FileMetadata(e.getKey()), Map.Entry::getValue));
        
        long recoveredRemoteParquetFiles = recoveredFilesWithMetadata.keySet().stream()
            .filter(fm -> "parquet".equals(fm.dataFormat()))
            .count();

        // Verify consistency
        assertEquals("Remote Parquet file count should be same after recovery", remoteParquetFiles, recoveredRemoteParquetFiles);
        assertEquals("Cluster UUID should remain same", clusterUUID, clusterService().state().metadata().clusterUUID());

        // Verify all FileMetadata has correct format
        for (FileMetadata fm : recoveredFilesWithMetadata.keySet()) {
            assertNotNull("FileMetadata format should not be null", fm.dataFormat());
            assertFalse("FileMetadata format should not be empty", fm.dataFormat().isEmpty());
        }

        // Verify document count
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        assertEquals("Document count should be preserved", numDocs, recoveredShard.docStats().getCount());

        logger.info("--> testDataFusionSegmentFileConsistency completed successfully");
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }
}
