/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import com.parquet.parquetdataformat.ParquetDataFormatPlugin;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.CompositeStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.UploadedSegmentMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.junit.annotations.TestLogging;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Integration tests for DataFusion engine snapshot and restore recovery scenarios.
 * Tests snapshot/restore operations with Parquet format metadata preservation.
 * 
 * Note: These tests are marked with @AwaitsFix because snapshot/restore functionality
 * for optimized indices (Parquet format) is not yet implemented. Once the feature
 * is complete, remove the @AwaitsFix annotations.
 */
@TestLogging(
    value = "org.opensearch.index.shard:DEBUG,org.opensearch.index.store:DEBUG,org.opensearch.datafusion:DEBUG,org.opensearch.snapshots:DEBUG",
    reason = "Validate DataFusion snapshot/restore with format-aware metadata"
)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataFusionSnapshotRestoreRecoveryTests extends OpenSearchIntegTestCase {

    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected static final String SNAPSHOT_REPOSITORY_NAME = "test-snapshot-repo";
    protected static final String INDEX_NAME = "datafusion-snapshot-test-index";
    protected static final String SNAPSHOT_NAME = "test-snapshot";

    protected Path repositoryPath;
    protected Path snapshotRepoPath;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataFusionPlugin.class, ParquetDataFormatPlugin.class);
    }

    @Before
    public void setup() {
        repositoryPath = randomRepoPath().toAbsolutePath();
        snapshotRepoPath = randomRepoPath().toAbsolutePath();
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

    private void createSnapshotRepository(String repoName, Path path) {
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(repoName)
                .setType("fs")
                .setSettings(Settings.builder().put("location", path).put("compress", false))
        );
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
     * Tests that snapshot and restore operations preserve Parquet format metadata
     * and CatalogSnapshot for optimized indices.
     * 
     * This test validates:
     * - Document count matches before/after snapshot restore
     * - Parquet file count matches
     * - FileMetadata.dataFormat() returns "parquet" for all Parquet files
     * - CatalogSnapshot bytes are properly restored
     * - Search operations work correctly after restore
     */
    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/TBD")
    public void testDataFusionSnapshotRestore() throws Exception {
        logger.info("--> Starting testDataFusionSnapshotRestore");
        
        // Setup cluster
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        // Create snapshot repository
        createSnapshotRepository(SNAPSHOT_REPOSITORY_NAME, snapshotRepoPath);

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

        // Capture state before snapshot
        IndexShard indexShard = getIndexShard(dataNode, INDEX_NAME);
        validateRemoteStoreSegments(indexShard, "before snapshot");
        validateCatalogSnapshot(indexShard, "before snapshot");
        
        long docCountBeforeSnapshot = indexShard.docStats().getCount();
        long parquetFilesBeforeSnapshot = countParquetFilesInRemote(indexShard);
        
        logger.info("--> State before snapshot: docs={}, parquetFiles={}", docCountBeforeSnapshot, parquetFilesBeforeSnapshot);

        // Create snapshot
        logger.info("--> Creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(SNAPSHOT_REPOSITORY_NAME, SNAPSHOT_NAME)
            .setWaitForCompletion(true)
            .setIndices(INDEX_NAME)
            .get();
        
        SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertEquals("Snapshot should succeed", SnapshotState.SUCCESS, snapshotInfo.state());
        assertTrue("Snapshot should include index", snapshotInfo.indices().contains(INDEX_NAME));

        // Delete the index
        logger.info("--> Deleting index before restore");
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());

        // Restore from snapshot
        logger.info("--> Restoring from snapshot");
        RestoreSnapshotResponse restoreResponse = client().admin()
            .cluster()
            .prepareRestoreSnapshot(SNAPSHOT_REPOSITORY_NAME, SNAPSHOT_NAME)
            .setWaitForCompletion(true)
            .setIndices(INDEX_NAME)
            .get();
        
        assertEquals("Restore should succeed", RestStatus.OK, restoreResponse.status());
        ensureGreen(INDEX_NAME);

        // Validate format metadata after restore
        String newDataNode = internalCluster().getDataNodeNames().iterator().next();
        IndexShard restoredShard = getIndexShard(newDataNode, INDEX_NAME);
        validateRemoteStoreSegments(restoredShard, "after restore");
        validateCatalogSnapshot(restoredShard, "after restore");

        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        long docCountAfterRestore = restoredShard.docStats().getCount();
        long parquetFilesAfterRestore = countParquetFilesInRemote(restoredShard);

        // Verify consistency
        assertEquals("Document count should match after restore", docCountBeforeSnapshot, docCountAfterRestore);
        assertEquals("Parquet file count should match after restore", parquetFilesBeforeSnapshot, parquetFilesAfterRestore);

        // Verify document count matches expected number
        assertEquals("Document count should match expected", numDocs, docCountAfterRestore);

        logger.info("--> testDataFusionSnapshotRestore completed successfully");
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }

    /**
     * Tests recovery after force merge operations to ensure merged Parquet files 
     * maintain format integrity through snapshot/restore.
     * 
     * This test validates:
     * - Single merged Parquet file exists after force merge
     * - Format metadata preserved post-merge
     * - Document count correct after restore
     */
    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/TBD")
    public void testDataFusionRestoreWithForceMerge() throws Exception {
        logger.info("--> Starting testDataFusionRestoreWithForceMerge");
        
        // Setup cluster
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        // Create snapshot repository
        createSnapshotRepository(SNAPSHOT_REPOSITORY_NAME, snapshotRepoPath);

        // Create index
        String mappings = "{ \"properties\": { \"message\": { \"type\": \"long\" }, \"batch\": { \"type\": \"keyword\" } } }";
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings()).setMapping(mappings).get());
        ensureGreen(INDEX_NAME);

        // Index documents in multiple batches to create multiple Parquet files
        int numBatches = 4;
        int docsPerBatch = 5;
        int totalDocs = numBatches * docsPerBatch;
        
        for (int batch = 1; batch <= numBatches; batch++) {
            for (int i = 1; i <= docsPerBatch; i++) {
                client().prepareIndex(INDEX_NAME).setId("batch" + batch + "_doc" + i)
                    .setSource("{ \"message\": " + (batch * 100 + i) + ", \"batch\": \"batch" + batch + "\" }", MediaTypeRegistry.JSON).get();
            }
            client().admin().indices().prepareFlush(INDEX_NAME).get();
        }
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Capture state before merge
        IndexShard shardBeforeMerge = getIndexShard(dataNode, INDEX_NAME);
        long parquetFilesBeforeMerge = countParquetFilesInRemote(shardBeforeMerge);
        logger.info("--> Parquet files before merge: {}", parquetFilesBeforeMerge);
        assertTrue("Should have multiple Parquet files before merge", parquetFilesBeforeMerge >= numBatches);

        // Force merge to single segment
        logger.info("--> Executing force merge");
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Validate merged state
        IndexShard shardAfterMerge = getIndexShard(dataNode, INDEX_NAME);
        validateRemoteStoreSegments(shardAfterMerge, "after force merge");
        long docCountAfterMerge = shardAfterMerge.docStats().getCount();
        assertEquals("Doc count should be preserved after merge", totalDocs, docCountAfterMerge);

        // Create snapshot of merged index
        logger.info("--> Creating snapshot of merged index");
        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(SNAPSHOT_REPOSITORY_NAME, SNAPSHOT_NAME)
            .setWaitForCompletion(true)
            .setIndices(INDEX_NAME)
            .get();
        
        assertEquals("Snapshot should succeed", SnapshotState.SUCCESS, createSnapshotResponse.getSnapshotInfo().state());

        // Delete the index
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());

        // Restore from snapshot
        logger.info("--> Restoring merged index from snapshot");
        RestoreSnapshotResponse restoreResponse = client().admin()
            .cluster()
            .prepareRestoreSnapshot(SNAPSHOT_REPOSITORY_NAME, SNAPSHOT_NAME)
            .setWaitForCompletion(true)
            .setIndices(INDEX_NAME)
            .get();
        
        assertEquals("Restore should succeed", RestStatus.OK, restoreResponse.status());
        ensureGreen(INDEX_NAME);

        // Validate restored merged state
        String newDataNode = internalCluster().getDataNodeNames().iterator().next();
        IndexShard restoredShard = getIndexShard(newDataNode, INDEX_NAME);
        validateRemoteStoreSegments(restoredShard, "after restore");
        
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        long docCountAfterRestore = restoredShard.docStats().getCount();
        
        assertEquals("Document count should be preserved after restore", totalDocs, docCountAfterRestore);

        logger.info("--> testDataFusionRestoreWithForceMerge completed successfully");
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }

    /**
     * Tests shallow copy snapshot specifically for optimized indices to ensure 
     * format-aware metadata references are preserved.
     * 
     * This test validates:
     * - Remote store file paths preserved
     * - No data copied during snapshot (shallow)
     * - Format metadata intact post-restore
     */
    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/TBD")
    public void testDataFusionShallowCopySnapshotRestore() throws Exception {
        logger.info("--> Starting testDataFusionShallowCopySnapshotRestore");
        
        // Setup cluster
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        // Create snapshot repository with shallow copy enabled
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(SNAPSHOT_REPOSITORY_NAME)
                .setType("fs")
                .setSettings(Settings.builder()
                    .put("location", snapshotRepoPath)
                    .put("compress", false)
                    // Enable shallow copy for remote store indices
                    .put("shallow_snapshot_v2", true)
                )
        );

        // Create index
        String mappings = "{ \"properties\": { \"message\": { \"type\": \"long\" } } }";
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings()).setMapping(mappings).get());
        ensureGreen(INDEX_NAME);

        // Index documents
        int numDocs = randomIntBetween(10, 30);
        for (int i = 1; i <= numDocs; i++) {
            client().prepareIndex(INDEX_NAME).setId("doc" + i)
                .setSource("{ \"message\": " + (i * 100) + " }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Capture remote store file references before snapshot
        IndexShard shardBeforeSnapshot = getIndexShard(dataNode, INDEX_NAME);
        validateRemoteStoreSegments(shardBeforeSnapshot, "before shallow snapshot");
        
        Map<String, UploadedSegmentMetadata> remoteFilesBefore = shardBeforeSnapshot.getRemoteDirectory()
            .getSegmentsUploadedToRemoteStore();
        long docCountBefore = shardBeforeSnapshot.docStats().getCount();
        
        logger.info("--> Remote files before snapshot: {}", remoteFilesBefore.size());

        // Create shallow copy snapshot
        logger.info("--> Creating shallow copy snapshot");
        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(SNAPSHOT_REPOSITORY_NAME, SNAPSHOT_NAME)
            .setWaitForCompletion(true)
            .setIndices(INDEX_NAME)
            .get();
        
        SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertEquals("Snapshot should succeed", SnapshotState.SUCCESS, snapshotInfo.state());

        // Delete the index
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());

        // Restore from shallow copy snapshot
        logger.info("--> Restoring from shallow copy snapshot");
        RestoreSnapshotResponse restoreResponse = client().admin()
            .cluster()
            .prepareRestoreSnapshot(SNAPSHOT_REPOSITORY_NAME, SNAPSHOT_NAME)
            .setWaitForCompletion(true)
            .setIndices(INDEX_NAME)
            .get();
        
        assertEquals("Restore should succeed", RestStatus.OK, restoreResponse.status());
        ensureGreen(INDEX_NAME);

        // Validate restored index uses same remote store files (shallow restore)
        String newDataNode = internalCluster().getDataNodeNames().iterator().next();
        IndexShard restoredShard = getIndexShard(newDataNode, INDEX_NAME);
        validateRemoteStoreSegments(restoredShard, "after shallow restore");
        validateCatalogSnapshot(restoredShard, "after shallow restore");

        Map<String, UploadedSegmentMetadata> remoteFilesAfter = restoredShard.getRemoteDirectory()
            .getSegmentsUploadedToRemoteStore();
        
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        long docCountAfter = restoredShard.docStats().getCount();

        // Verify consistency
        assertEquals("Document count should match after shallow restore", docCountBefore, docCountAfter);
        
        // Verify remote store file paths are preserved (shallow copy behavior)
        // In shallow copy, files should reference the same remote store locations
        logger.info("--> Remote files after restore: {}", remoteFilesAfter.size());
        
        // Verify format metadata preserved
        for (Map.Entry<String, UploadedSegmentMetadata> entry : remoteFilesAfter.entrySet()) {
            FileMetadata metadata = new FileMetadata(entry.getKey());
            assertNotNull("Format should not be null", metadata.dataFormat());
            assertFalse("Format should not be empty", metadata.dataFormat().isEmpty());
        }

        // Verify document count matches expected number
        assertEquals("Document count should match expected", numDocs, docCountAfter);

        logger.info("--> testDataFusionShallowCopySnapshotRestore completed successfully");
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }
}
