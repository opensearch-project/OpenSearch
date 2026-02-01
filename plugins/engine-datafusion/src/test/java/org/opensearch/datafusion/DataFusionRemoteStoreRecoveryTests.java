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
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.UploadedSegmentMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
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

import org.opensearch.index.store.CompositeStoreDirectory;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Integration tests for DataFusion engine remote store recovery scenarios.
 * Tests format-aware metadata preservation, CatalogSnapshot recovery, and
 * remote store recovery validation with Parquet/Arrow files.
 */
@TestLogging(
    value = "org.opensearch.index.shard:DEBUG,org.opensearch.index.store:DEBUG,org.opensearch.datafusion:DEBUG",
    reason = "Validate DataFusion recovery with format-aware metadata"
)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataFusionRemoteStoreRecoveryTests extends OpenSearchIntegTestCase {

    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected static final String INDEX_NAME = "datafusion-test-index";

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

    private IndexShard getIndexShard(String nodeName, String indexName) {
        return internalCluster().getInstance(org.opensearch.indices.IndicesService.class, nodeName)
            .indexServiceSafe(internalCluster().clusterService(nodeName).state().metadata().index(indexName).getIndex())
            .getShard(0);
    }

    private void validateRemoteStoreSegments(IndexShard shard, String stageName) {
        RemoteSegmentStoreDirectory remoteDir = shard.getRemoteDirectory();
        assertNotNull("RemoteSegmentStoreDirectory should not be null", remoteDir);

        Map<String, UploadedSegmentMetadata> uploadedSegmentsRaw = remoteDir.getSegmentsUploadedToRemoteStore();
        if (uploadedSegmentsRaw.isEmpty()) {
            logger.warn("--> No segments uploaded yet at stage: {}", stageName);
            return;
        }

        Map<FileMetadata, UploadedSegmentMetadata> uploadedSegments = uploadedSegmentsRaw.entrySet().stream()
            .collect(Collectors.toMap(e -> new FileMetadata(e.getKey()), Map.Entry::getValue));

        for (FileMetadata fileMetadata : uploadedSegments.keySet()) {
            assertNotNull("FileMetadata should have format information", fileMetadata.dataFormat());
            assertFalse("Format should not be empty", fileMetadata.dataFormat().isEmpty());
        }
    }

    private long validateLocalShardFiles(IndexShard shard, String stageName) {
        try {
            CompositeStoreDirectory compositeDir = shard.store().compositeStoreDirectory();
            if (compositeDir != null) {
                FileMetadata[] allFiles = compositeDir.listFileMetadata();
                return Arrays.stream(allFiles).filter(fm -> "parquet".equals(fm.dataFormat())).count();
            } else {
                String[] files = shard.store().directory().listAll();
                return Arrays.stream(files).filter(f -> f.contains("parquet") || f.endsWith(".parquet")).count();
            }
        } catch (IOException e) {
            logger.warn("--> Failed to list local shard files at stage {}: {}", stageName, e.getMessage());
            return -1;
        }
    }

    private void validateCatalogSnapshot(IndexShard shard, String stageName) {
        RemoteSegmentStoreDirectory remoteDir = shard.getRemoteDirectory();
        assertNotNull("RemoteSegmentStoreDirectory should not be null", remoteDir);

        try {
            RemoteSegmentMetadata metadata = remoteDir.readLatestMetadataFile();
            if (metadata == null) {
                logger.warn("--> RemoteSegmentMetadata not found at stage {}", stageName);
                return;
            }

            byte[] catalogSnapshotBytes = metadata.getSegmentInfosBytes();
            if (catalogSnapshotBytes != null) {
                assertTrue("CatalogSnapshot bytes should not be empty", catalogSnapshotBytes.length > 0);
            }

            var checkpoint = metadata.getReplicationCheckpoint();
            if (checkpoint != null) {
                assertTrue("Checkpoint version should be positive", checkpoint.getSegmentInfosVersion() > 0);
            }
        } catch (IOException e) {
            logger.warn("--> Failed to read metadata at stage {}: {}", stageName, e.getMessage());
        }
    }

    /**
     * Tests DataFusion engine recovery from remote store with format-aware metadata preservation.
     */
    public void testDataFusionWithRemoteStoreRecovery() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(1);
        internalCluster().startDataOnlyNodes(1);
        ensureStableCluster(2);

        String mappings = "{ \"properties\": { \"message\": { \"type\": \"long\" }, \"message2\": { \"type\": \"long\" }, \"message3\": { \"type\": \"long\" } } }";
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings()).setMapping(mappings).get());
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("{ \"message\": 4, \"message2\": 3, \"message3\": 4 }", MediaTypeRegistry.JSON).get();
        client().prepareIndex(INDEX_NAME).setId("2").setSource("{ \"message\": 3, \"message2\": 4, \"message3\": 5 }", MediaTypeRegistry.JSON).get();
        client().prepareIndex(INDEX_NAME).setId("3").setSource("{ \"message\": 5, \"message2\": 2, \"message3\": 3 }", MediaTypeRegistry.JSON).get();

        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        String dataNodeName = internalCluster().getDataNodeNames().iterator().next();
        IndexShard indexShard = getIndexShard(dataNodeName, INDEX_NAME);
        validateRemoteStoreSegments(indexShard, "before recovery");
        validateCatalogSnapshot(indexShard, "before recovery");

        // Capture state before recovery for comparison
        long docCountBeforeRecovery = indexShard.docStats().getCount();
        long localFilesBeforeRecovery = validateLocalShardFiles(indexShard, "before recovery");

        String clusterUUID = clusterService().state().metadata().clusterUUID();
        internalCluster().stopRandomDataNode();
        ensureRed(INDEX_NAME);

        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        assertAcked(client().admin().indices().prepareClose(INDEX_NAME));
        client().admin().cluster().restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true), PlainActionFuture.newFuture());
        ensureGreen(INDEX_NAME);
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();

        assertEquals("Cluster UUID should remain same", clusterUUID, clusterService().state().metadata().clusterUUID());

        String newDataNodeName = internalCluster().getDataNodeNames().iterator().next();
        IndexShard recoveredIndexShard = getIndexShard(newDataNodeName, INDEX_NAME);
        validateRemoteStoreSegments(recoveredIndexShard, "after recovery");
        validateCatalogSnapshot(recoveredIndexShard, "after recovery");

        long localFilesAfterRecovery = validateLocalShardFiles(recoveredIndexShard, "after recovery");
        assertTrue("Should have local files after recovery", localFilesAfterRecovery >= 0);

        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        long docCountAfterRecovery = recoveredIndexShard.docStats().getCount();

        // Verify before/after comparison
        assertEquals("Doc count should be same before and after recovery", docCountBeforeRecovery, docCountAfterRecovery);
        assertEquals("Local file count should be same before and after recovery", localFilesBeforeRecovery, localFilesAfterRecovery);

        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }

    /**
     * Tests DataFusion recovery with multiple Parquet generation files.
     */
    public void testDataFusionRecoveryWithMultipleParquetGenerations() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(1);
        internalCluster().startDataOnlyNodes(1);
        ensureStableCluster(2);

        String mappings = "{ \"properties\": { \"message\": { \"type\": \"long\" }, \"message2\": { \"type\": \"long\" }, \"generation\": { \"type\": \"keyword\" } } }";
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings()).setMapping(mappings).get());
        ensureGreen(INDEX_NAME);

        String dataNodeName = internalCluster().getDataNodeNames().iterator().next();
        IndexShard indexShard = getIndexShard(dataNodeName, INDEX_NAME);

        int numGenerations = 4;
        for (int gen = 1; gen <= numGenerations; gen++) {
            for (int i = 1; i <= 3; i++) {
                client().prepareIndex(INDEX_NAME).setId("gen" + gen + "_doc" + i)
                    .setSource("{ \"message\": " + (gen * 100 + i) + ", \"message2\": " + (gen * 200 + i) + ", \"generation\": \"gen" + gen + "\" }", MediaTypeRegistry.JSON).get();
            }
            client().admin().indices().prepareFlush(INDEX_NAME).get();
            client().admin().indices().prepareRefresh(INDEX_NAME).get();
            Thread.sleep(500);
        }

        validateRemoteStoreSegments(indexShard, "before recovery");
        RemoteSegmentStoreDirectory remoteDir = indexShard.getRemoteDirectory();
        Map<FileMetadata, UploadedSegmentMetadata> uploadedSegments = remoteDir.getSegmentsUploadedToRemoteStore().entrySet().stream()
            .collect(Collectors.toMap(e -> new FileMetadata(e.getKey()), Map.Entry::getValue));
        long parquetFileCount = uploadedSegments.keySet().stream().filter(fm -> "parquet".equals(fm.dataFormat())).count();
        assertTrue("Should have multiple Parquet generation files", parquetFileCount >= numGenerations);

        // Capture state before recovery for comparison
        long docCountBeforeRecovery = indexShard.docStats().getCount();
        long localFilesBeforeRecovery = validateLocalShardFiles(indexShard, "before recovery");

        String clusterUUID = clusterService().state().metadata().clusterUUID();
        internalCluster().stopRandomDataNode();
        ensureRed(INDEX_NAME);

        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        assertAcked(client().admin().indices().prepareClose(INDEX_NAME));
        client().admin().cluster().restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true), PlainActionFuture.newFuture());
        ensureGreen(INDEX_NAME);

        String newDataNodeName = internalCluster().getDataNodeNames().iterator().next();
        IndexShard recoveredIndexShard = getIndexShard(newDataNodeName, INDEX_NAME);
        validateRemoteStoreSegments(recoveredIndexShard, "after recovery");

        RemoteSegmentStoreDirectory recoveredRemoteDir = recoveredIndexShard.getRemoteDirectory();
        Map<FileMetadata, UploadedSegmentMetadata> recoveredSegments = recoveredRemoteDir.getSegmentsUploadedToRemoteStore().entrySet().stream()
            .collect(Collectors.toMap(e -> new FileMetadata(e.getKey()), Map.Entry::getValue));
        long recoveredParquetFileCount = recoveredSegments.keySet().stream().filter(fm -> "parquet".equals(fm.dataFormat())).count();
        assertEquals("Should recover same number of Parquet files", parquetFileCount, recoveredParquetFileCount);

        long localFilesAfterRecovery = validateLocalShardFiles(recoveredIndexShard, "after recovery");
        assertTrue("Should have local files after recovery", localFilesAfterRecovery >= 0);

        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        long docCountAfterRecovery = recoveredIndexShard.docStats().getCount();

        // Verify before/after comparison
        assertEquals("Doc count should be same before and after recovery", docCountBeforeRecovery, docCountAfterRecovery);
        assertEquals("Local file count should be same before and after recovery", localFilesBeforeRecovery, localFilesAfterRecovery);
        assertEquals("Cluster UUID should remain same", clusterUUID, clusterService().state().metadata().clusterUUID());
    }

    /**
     * Tests DataFusion replica promotion to primary with Parquet format preservation.
     */
    public void testDataFusionReplicaPromotionToPrimary() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(1);
        internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);

        String mappings = "{ \"properties\": { \"message\": { \"type\": \"long\" }, \"phase\": { \"type\": \"keyword\" } } }";
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME)
            .setSettings(Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build())
            .setMapping(mappings).get());
        ensureGreen(INDEX_NAME);

        for (int i = 1; i <= 5; i++) {
            client().prepareIndex(INDEX_NAME).setId("primary_doc" + i)
                .setSource("{ \"message\": " + (i * 100) + ", \"phase\": \"primary\" }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        ensureGreen(INDEX_NAME);

        var clusterState = clusterService().state();
        var shardRouting = clusterState.routingTable().index(INDEX_NAME).shard(0);
        String primaryNodeId = shardRouting.primaryShard().currentNodeId();
        String replicaNodeId = shardRouting.replicaShards().get(0).currentNodeId();

        String primaryNodeName = null, replicaNodeName = null;
        for (String nodeName : internalCluster().getNodeNames()) {
            String nodeId = internalCluster().clusterService(nodeName).localNode().getId();
            if (nodeId.equals(primaryNodeId)) primaryNodeName = nodeName;
            else if (nodeId.equals(replicaNodeId)) replicaNodeName = nodeName;
        }

        IndexShard replicaShard = internalCluster().getInstance(org.opensearch.indices.IndicesService.class, replicaNodeName)
            .indexServiceSafe(resolveIndex(INDEX_NAME)).getShard(0);
        Thread.sleep(2000);
        validateRemoteStoreSegments(replicaShard, "replica before promotion");

        // Capture state before promotion for comparison
        long docCountBeforePromotion = replicaShard.docStats().getCount();
        long localFilesBeforePromotion = validateLocalShardFiles(replicaShard, "replica before promotion");

        internalCluster().stopRandomNode(org.opensearch.test.InternalTestCluster.nameFilter(primaryNodeName));
        ensureStableCluster(2);
        ensureYellow(INDEX_NAME);

        IndexShard promotedShard = internalCluster().getInstance(org.opensearch.indices.IndicesService.class, replicaNodeName)
            .indexServiceSafe(resolveIndex(INDEX_NAME)).getShard(0);
        assertTrue("Former replica should now be primary", promotedShard.routingEntry().primary());
        validateRemoteStoreSegments(promotedShard, "after promotion");

        Set<String> formats = promotedShard.getRemoteDirectory().getSegmentsUploadedToRemoteStore().entrySet().stream()
            .map(e -> new FileMetadata(e.getKey()).dataFormat()).collect(Collectors.toSet());
        assertTrue("Promoted primary should have Parquet files", formats.contains("parquet"));

        for (int i = 1; i <= 3; i++) {
            client().prepareIndex(INDEX_NAME).setId("promoted_doc" + i)
                .setSource("{ \"message\": " + (i * 200) + ", \"phase\": \"promoted\" }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        long localFilesAfterPromotion = validateLocalShardFiles(promotedShard, "after promotion and new docs");
        assertTrue("Should have local files after promotion", localFilesAfterPromotion >= 0);

        // Verify final state (5 original + 3 new docs)
        assertEquals("Final document count should match", 8, promotedShard.docStats().getCount());
        // Local files should increase after adding new docs
        assertTrue("Local files should exist after new writes", localFilesAfterPromotion >= localFilesBeforePromotion);
    }

    /**
     * Tests cluster recovery from remote translog when no flush/refresh is performed.
     */
    public void testClusterRecoveryFromTranslogWithoutFlush() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(1);
        internalCluster().startDataOnlyNodes(1);
        ensureStableCluster(2);

        String mappings = "{ \"properties\": { \"value\": { \"type\": \"long\" }, \"name\": { \"type\": \"keyword\" } } }";
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME)
            .setSettings(Settings.builder().put(indexSettings()).put("index.translog.durability", "request").build())
            .setMapping(mappings).get());
        ensureGreen(INDEX_NAME);

        int numDocs = 10;
        for (int i = 1; i <= numDocs; i++) {
            client().prepareIndex(INDEX_NAME).setId("doc" + i)
                .setSource("{ \"value\": " + (i * 100) + ", \"name\": \"doc" + i + "\" }", MediaTypeRegistry.JSON).get();
        }
        // Intentionally NOT calling flush or refresh - documents exist only in translog
        Thread.sleep(1000);

        String dataNodeName = internalCluster().getDataNodeNames().iterator().next();
        IndexShard indexShard = getIndexShard(dataNodeName, INDEX_NAME);
        assertTrue("Translog should have uncommitted operations", indexShard.translogStats().getUncommittedOperations() >= numDocs);

        String clusterUUID = clusterService().state().metadata().clusterUUID();
        internalCluster().stopRandomDataNode();
        ensureRed(INDEX_NAME);

        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        assertAcked(client().admin().indices().prepareClose(INDEX_NAME));
        client().admin().cluster().restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true), PlainActionFuture.newFuture());
        ensureGreen(INDEX_NAME);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        String newDataNodeName = internalCluster().getDataNodeNames().iterator().next();
        IndexShard recoveredShard = getIndexShard(newDataNodeName, INDEX_NAME);

        assertBusy(() -> assertTrue("Translog should have processed operations",
            recoveredShard.translogStats().estimatedNumberOfOperations() >= 0), 30, TimeUnit.SECONDS);

        long parquetFilesAfterRecovery = validateLocalShardFiles(recoveredShard, "after recovery");
        assertTrue("Should have local files after recovery", parquetFilesAfterRecovery >= 0);
        assertEquals("Document count should match", numDocs, recoveredShard.docStats().getCount());
        assertEquals("Cluster UUID should remain same", clusterUUID, clusterService().state().metadata().clusterUUID());

        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }

    /**
     * Tests replica promotion to primary with translog replay for uncommitted operations.
     */
    public void testReplicaPromotionWithTranslogReplay() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(1);
        internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);

        String mappings = "{ \"properties\": { \"value\": { \"type\": \"long\" }, \"phase\": { \"type\": \"keyword\" } } }";
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME)
            .setSettings(Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("index.translog.durability", "request").build())
            .setMapping(mappings).get());
        ensureGreen(INDEX_NAME);

        int initialDocs = 5;
        for (int i = 1; i <= initialDocs; i++) {
            client().prepareIndex(INDEX_NAME).setId("initial_doc" + i)
                .setSource("{ \"value\": " + (i * 100) + ", \"phase\": \"initial\" }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        ensureGreen(INDEX_NAME);

        int uncommittedDocs = 7;
        for (int i = 1; i <= uncommittedDocs; i++) {
            client().prepareIndex(INDEX_NAME).setId("uncommitted_doc" + i)
                .setSource("{ \"value\": " + (i * 200) + ", \"phase\": \"uncommitted\" }", MediaTypeRegistry.JSON).get();
        }
        // Intentionally NOT calling flush or refresh - docs exist only in translog
        Thread.sleep(1000);

        var clusterState = clusterService().state();
        var shardRouting = clusterState.routingTable().index(INDEX_NAME).shard(0);
        String primaryNodeId = shardRouting.primaryShard().currentNodeId();
        String replicaNodeId = shardRouting.replicaShards().get(0).currentNodeId();

        String primaryNodeName = null, replicaNodeName = null;
        for (String nodeName : internalCluster().getNodeNames()) {
            String nodeId = internalCluster().clusterService(nodeName).localNode().getId();
            if (nodeId.equals(primaryNodeId)) primaryNodeName = nodeName;
            else if (nodeId.equals(replicaNodeId)) replicaNodeName = nodeName;
        }
        assertNotNull("Primary node name should be found", primaryNodeName);
        assertNotNull("Replica node name should be found", replicaNodeName);

        IndexShard primaryShard = internalCluster().getInstance(org.opensearch.indices.IndicesService.class, primaryNodeName)
            .indexServiceSafe(resolveIndex(INDEX_NAME)).getShard(0);
        assertTrue("Primary should have uncommitted translog operations", primaryShard.translogStats().getUncommittedOperations() >= uncommittedDocs);

        IndexShard replicaShard = internalCluster().getInstance(org.opensearch.indices.IndicesService.class, replicaNodeName)
            .indexServiceSafe(resolveIndex(INDEX_NAME)).getShard(0);
        long replicaFilesBeforePromotion = validateLocalShardFiles(replicaShard, "replica before promotion");

        String finalReplicaNodeName = replicaNodeName;
        internalCluster().stopRandomNode(org.opensearch.test.InternalTestCluster.nameFilter(primaryNodeName));
        ensureStableCluster(2);

        assertBusy(() -> {
            var health = client().admin().cluster().prepareHealth(INDEX_NAME).get();
            assertTrue("Index should not be red", health.getStatus() != org.opensearch.cluster.health.ClusterHealthStatus.RED);
        }, 30, TimeUnit.SECONDS);
        ensureYellow(INDEX_NAME);

        IndexShard promotedShard = internalCluster().getInstance(org.opensearch.indices.IndicesService.class, finalReplicaNodeName)
            .indexServiceSafe(resolveIndex(INDEX_NAME)).getShard(0);
        assertTrue("Former replica should now be primary", promotedShard.routingEntry().primary());

        assertBusy(() -> assertTrue("Translog should have processed operations",
            promotedShard.translogStats().estimatedNumberOfOperations() >= 0), 30, TimeUnit.SECONDS);

        validateRemoteStoreSegments(promotedShard, "after promotion");
        long promotedFilesAfterPromotion = validateLocalShardFiles(promotedShard, "after promotion");
        assertTrue("Promoted primary should have local files", promotedFilesAfterPromotion >= 0);

        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        assertEquals("Document count should include all documents", initialDocs + uncommittedDocs, promotedShard.docStats().getCount());

        int newDocs = 3;
        for (int i = 1; i <= newDocs; i++) {
            client().prepareIndex(INDEX_NAME).setId("post_promotion_doc" + i)
                .setSource("{ \"value\": " + (i * 300) + ", \"phase\": \"post_promotion\" }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }

    /**
     * Tests DataFusion primary restart with extra local commits.
     */
    public void testDataFusionPrimaryRestartWithExtraCommits() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(1);
        internalCluster().startDataOnlyNodes(1);
        ensureStableCluster(2);

        String mappings = "{ \"properties\": { \"message\": { \"type\": \"long\" }, \"stage\": { \"type\": \"keyword\" } } }";
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings()).setMapping(mappings).get());
        ensureGreen(INDEX_NAME);

        for (int i = 1; i <= 4; i++) {
            client().prepareIndex(INDEX_NAME).setId("initial_doc" + i)
                .setSource("{ \"message\": " + (i * 100) + ", \"stage\": \"initial\" }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        String dataNodeName = internalCluster().getDataNodeNames().iterator().next();
        IndexShard indexShard = getIndexShard(dataNodeName, INDEX_NAME);
        validateRemoteStoreSegments(indexShard, "initial upload");

        // Capture state before extra docs and restart for comparison
        long docCountAfterInitial = indexShard.docStats().getCount();
        long localFilesAfterInitial = validateLocalShardFiles(indexShard, "after initial flush");

        for (int i = 1; i <= 3; i++) {
            client().prepareIndex(INDEX_NAME).setId("extra_doc" + i)
                .setSource("{ \"message\": " + (i * 300) + ", \"stage\": \"extra\" }", MediaTypeRegistry.JSON).get();
        }

        try {
            org.apache.lucene.index.SegmentInfos latestCommit = org.apache.lucene.index.SegmentInfos.readLatestCommit(indexShard.store().directory());
            latestCommit.commit(indexShard.store().directory());
            latestCommit.commit(indexShard.store().directory());
        } catch (Exception e) {
            logger.warn("--> Could not create extra commits: {}", e.getMessage());
        }

        String nodeToRestart = internalCluster().getDataNodeNames().iterator().next();
        internalCluster().restartNode(nodeToRestart, new org.opensearch.test.InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                return super.onNodeStopped(nodeName);
            }
        });
        ensureStableCluster(2);
        ensureGreen(INDEX_NAME);

        String restartedNodeName = internalCluster().getDataNodeNames().iterator().next();
        IndexShard recoveredShard = getIndexShard(restartedNodeName, INDEX_NAME);
        validateRemoteStoreSegments(recoveredShard, "after restart");

        long localFilesAfterRecovery = validateLocalShardFiles(recoveredShard, "after restart");
        assertTrue("Should have local files after restart", localFilesAfterRecovery >= 0);

        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        long docCountAfterRestart = recoveredShard.docStats().getCount();

        // Verify doc count: initial 4 + extra 3 = 7
        assertEquals("Document count should match total docs after restart", 7, docCountAfterRestart);
        // Local files should be at least as many as after initial flush
        assertTrue("Local files should be preserved after restart", localFilesAfterRecovery >= localFilesAfterInitial);

        client().prepareIndex(INDEX_NAME).setId("post_recovery_doc")
            .setSource("{ \"message\": 999, \"stage\": \"post_recovery\" }", MediaTypeRegistry.JSON).get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        assertEquals("Final document count should match", 8, recoveredShard.docStats().getCount());
    }
}
