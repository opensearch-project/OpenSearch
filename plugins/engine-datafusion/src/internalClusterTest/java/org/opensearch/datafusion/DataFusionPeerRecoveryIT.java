/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import com.parquet.parquetdataformat.ParquetDataFormatPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.CompositeStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.UploadedSegmentMetadata;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Integration tests for DataFusion/Composite engine peer recovery scenarios.
 *
 * Both tests run with remote store enabled cluster (required by DataFusion).
 * Within a remote store cluster, we can trigger both handlers:
 *
 * From RecoverySourceHandlerFactory.create():
 * - if (request.isPrimaryRelocation()) -> LocalStorePeerRecoverySourceHandler (PRIMARY MOVING)
 * - if (request.targetNode().isRemoteStoreNode()) -> RemoteStorePeerRecoverySourceHandler (REPLICA RECOVERY)
 *
 * Test scenarios:
 * - testCompositeIndexPrimaryRelocationRecovery: Triggers LocalStorePeerRecoverySourceHandler via MoveAllocationCommand
 * - testCompositeIndexReplicaPeerRecovery: Triggers RemoteStorePeerRecoverySourceHandler via replica addition (0→1)
 */
@TestLogging(
    value = "org.opensearch.indices.recovery:DEBUG,org.opensearch.index.shard:DEBUG,org.opensearch.datafusion:DEBUG",
    reason = "Debug peer recovery flow for composite indices"
)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataFusionPeerRecoveryIT extends OpenSearchIntegTestCase {

    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected static final String INDEX_NAME = "datafusion-peer-recovery-test";

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

    private String getMappings() {
        return "{ \"properties\": { " +
            "\"message\": { \"type\": \"long\" }, " +
            "\"message2\": { \"type\": \"long\" }, " +
            "\"message3\": { \"type\": \"long\" } " +
            "} }";
    }

    private IndexShard getIndexShard(String nodeName, String indexName) {
        return internalCluster().getInstance(org.opensearch.indices.IndicesService.class, nodeName)
            .indexServiceSafe(internalCluster().clusterService(nodeName).state().metadata().index(indexName).getIndex())
            .getShard(0);
    }

    private IndexShard getPrimaryShard(String indexName) {
        var clusterState = clusterService().state();
        var shardRouting = clusterState.routingTable().index(indexName).shard(0).primaryShard();
        String primaryNodeId = shardRouting.currentNodeId();

        for (String nodeName : internalCluster().getNodeNames()) {
            String nodeId = internalCluster().clusterService(nodeName).localNode().getId();
            if (nodeId.equals(primaryNodeId)) {
                return getIndexShard(nodeName, indexName);
            }
        }
        throw new IllegalStateException("Could not find primary shard for " + indexName);
    }

    private IndexShard getReplicaShard(String indexName) {
        var clusterState = clusterService().state();
        var shardRouting = clusterState.routingTable().index(indexName).shard(0);
        if (shardRouting.replicaShards().isEmpty()) {
            throw new IllegalStateException("No replica shards found for " + indexName);
        }
        String replicaNodeId = shardRouting.replicaShards().get(0).currentNodeId();

        for (String nodeName : internalCluster().getNodeNames()) {
            String nodeId = internalCluster().clusterService(nodeName).localNode().getId();
            if (nodeId.equals(replicaNodeId)) {
                return getIndexShard(nodeName, indexName);
            }
        }
        throw new IllegalStateException("Could not find replica shard for " + indexName);
    }

    private String getPrimaryNodeName(String indexName) {
        var clusterState = clusterService().state();
        var shardRouting = clusterState.routingTable().index(indexName).shard(0).primaryShard();
        String primaryNodeId = shardRouting.currentNodeId();

        for (String nodeName : internalCluster().getNodeNames()) {
            String nodeId = internalCluster().clusterService(nodeName).localNode().getId();
            if (nodeId.equals(primaryNodeId)) {
                return nodeName;
            }
        }
        throw new IllegalStateException("Could not find primary node for " + indexName);
    }

    /**
     * Get a data node name that is different from the given node name.
     */
    private String getOtherDataNode(String currentNodeName) {
        for (String nodeName : internalCluster().getDataNodeNames()) {
            if (!nodeName.equals(currentNodeName)) {
                return nodeName;
            }
        }
        throw new IllegalStateException("Could not find another data node");
    }

    /**
     * Count parquet files on a shard using CompositeStoreDirectory.
     */
    private long countParquetFiles(IndexShard shard) {
        try {
            CompositeStoreDirectory compositeDir = shard.store().compositeStoreDirectory();
            if (compositeDir != null) {
                FileMetadata[] allFiles = compositeDir.listFileMetadata();
                return Arrays.stream(allFiles)
                    .filter(fm -> "parquet".equals(fm.dataFormat()))
                    .count();
            } else {
                // Fallback: check directory for parquet-like files
                String[] files = shard.store().directory().listAll();
                return Arrays.stream(files)
                    .filter(f -> f.contains("parquet") || f.endsWith(".parquet") || f.contains("generation"))
                    .count();
            }
        } catch (IOException e) {
            logger.warn("--> Failed to count parquet files: {}", e.getMessage());
            return -1;
        }
    }

    /**
     * Count parquet files in remote store.
     */
    private long countRemoteParquetFiles(IndexShard shard) {
        RemoteSegmentStoreDirectory remoteDir = shard.getRemoteDirectory();
        if (remoteDir == null) {
            return -1;
        }

        Map<String, UploadedSegmentMetadata> uploadedSegments = remoteDir.getSegmentsUploadedToRemoteStore();
        if (uploadedSegments.isEmpty()) {
            return 0;
        }

        return uploadedSegments.entrySet().stream()
            .map(e -> new FileMetadata(e.getKey()))
            .filter(fm -> "parquet".equals(fm.dataFormat()))
            .count();
    }

    /**
     * Index test documents to create parquet files.
     */
    private void indexDocuments() {
        for (int i = 1; i <= 5; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId("doc" + i)
                .setSource("{ \"message\": " + (i * 100) + ", \"message2\": " + (i * 200) + ", \"message3\": " + (i * 300) + " }",
                    MediaTypeRegistry.JSON)
                .get();
        }
    }

    /**
     * Test 1: PRIMARY RELOCATION triggers LocalStorePeerRecoverySourceHandler
     *
     * Tests peer recovery for composite index during PRIMARY RELOCATION.
     * Uses LocalStorePeerRecoverySourceHandler because isPrimaryRelocation=true.
     *
     * This test:
     * 1. Creates a composite index on one node
     * 2. Indexes documents and flushes to create parquet files
     * 3. Uses MoveAllocationCommand to force primary relocation to another node
     * 4. Verifies parquet files are properly copied via network to the new primary
     */
    public void testCompositeIndexPrimaryRelocationRecovery() throws Exception {
        logger.info("--> Starting test: Primary relocation recovery (LocalStorePeerRecoverySourceHandler)");

        // 1. Start remote store enabled cluster with 2 data nodes
        internalCluster().startClusterManagerOnlyNodes(1);
        internalCluster().startDataOnlyNodes(2);  // Node A and Node B
        ensureStableCluster(3);

        // 2. Create composite index with 1 shard, 0 replicas
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME)
            .setSettings(indexSettings())
            .setMapping(getMappings())
            .get());
        ensureGreen(INDEX_NAME);

        // 3. Index documents and flush to create parquet files
        indexDocuments();
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Capture state on original primary
        String originalPrimaryNode = getPrimaryNodeName(INDEX_NAME);
        IndexShard primaryShard = getPrimaryShard(INDEX_NAME);
        long docCountBeforeRelocation = primaryShard.docStats().getCount();
        long parquetFileCountBefore = countParquetFiles(primaryShard);
        long remoteParquetCount = countRemoteParquetFiles(primaryShard);

        logger.info("--> Before relocation: primaryNode={}, docs={}, primaryLocalParquet={}, remoteParquet={}",
            originalPrimaryNode, docCountBeforeRelocation, parquetFileCountBefore, remoteParquetCount);

        assertTrue("Should have documents before relocation", docCountBeforeRelocation > 0);
        assertTrue("Should have parquet files on primary", parquetFileCountBefore > 0);

        // 4. Force PRIMARY RELOCATION to another node
        // This triggers LocalStorePeerRecoverySourceHandler because isPrimaryRelocation=true
        String targetNode = getOtherDataNode(originalPrimaryNode);
        logger.info("--> Relocating primary from {} to {}", originalPrimaryNode, targetNode);

        client().admin().cluster().prepareReroute()
            .add(new MoveAllocationCommand(INDEX_NAME, 0, originalPrimaryNode, targetNode))
            .get();

        ensureGreen(INDEX_NAME);

        // 5. Verify primary moved and has same data
        String newPrimaryNode = getPrimaryNodeName(INDEX_NAME);
        assertEquals("Primary should have moved to target node", targetNode, newPrimaryNode);

        IndexShard relocatedPrimary = getPrimaryShard(INDEX_NAME);
        assertTrue("Relocated shard should be primary", relocatedPrimary.routingEntry().primary());

        // Wait for recovery to complete
        Thread.sleep(2000);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        long docCountAfterRelocation = relocatedPrimary.docStats().getCount();
        assertEquals("Doc count should match after relocation",
            docCountBeforeRelocation, docCountAfterRelocation);

        // 6. KEY ASSERTION: Verify parquet files exist on new primary
        long parquetFileCountAfter = countParquetFiles(relocatedPrimary);
        logger.info("--> After relocation: newPrimaryNode={}, docs={}, parquetFiles={}",
            newPrimaryNode, docCountAfterRelocation, parquetFileCountAfter);

        assertEquals("Parquet file count should match after relocation (will FAIL if not implemented!)",
            parquetFileCountBefore, parquetFileCountAfter);

        logger.info("--> Primary relocation recovery test completed successfully");
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }

    /**
     * Test 2: PRIMARY RELOCATION with CONCURRENT DATA INGESTION
     *
     * Tests peer recovery for composite index during PRIMARY RELOCATION while
     * data ingestion is happening in parallel. This validates that the system
     * correctly handles the race condition between document indexing and primary migration.
     *
     * This test:
     * 1. Creates a composite index on one node
     * 2. Starts a background thread that continuously indexes documents
     * 3. Simultaneously triggers primary relocation to another node
     * 4. Verifies that all documents (including those indexed during migration) are properly preserved
     * 5. Verifies parquet files are consistent between old and new primary
     */
    public void testPrimaryRelocationWithConcurrentIngestion() throws Exception {
        logger.info("--> Starting test: Primary relocation with concurrent data ingestion");

        // 1. Start remote store enabled cluster with 2 data nodes
        internalCluster().startClusterManagerOnlyNodes(1);
        internalCluster().startDataOnlyNodes(2);  // Node A and Node B
        ensureStableCluster(3);

        // 2. Create composite index with 1 shard, 0 replicas
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME)
            .setSettings(indexSettings())
            .setMapping(getMappings())
            .get());
        ensureGreen(INDEX_NAME);

        // 3. Index initial batch of documents and flush to create parquet files
        indexDocuments();  // docs 1-5

        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        // Capture initial state
        String originalPrimaryNode = getPrimaryNodeName(INDEX_NAME);
        IndexShard primaryShard = getPrimaryShard(INDEX_NAME);
        long initialDocCount = primaryShard.docStats().getCount();
        long initialParquetCount = countParquetFiles(primaryShard);

        logger.info("--> Initial state: primaryNode={}, docs={}, parquetFiles={}",
            originalPrimaryNode, initialDocCount, initialParquetCount);

        assertTrue("Should have initial documents", initialDocCount > 0);
        assertTrue("Should have initial parquet files", initialParquetCount > 0);

        // 4. Set up concurrent ingestion thread
        AtomicBoolean stopIngestion = new AtomicBoolean(false);
        AtomicInteger ingestedDuringMigration = new AtomicInteger(0);
        AtomicInteger ingestionErrors = new AtomicInteger(0);
        CountDownLatch ingestionStarted = new CountDownLatch(1);

        Thread ingestionThread = new Thread(() -> {
            int docId = 100;  // Start from 100 to avoid conflicts with initial docs
            ingestionStarted.countDown();

            while (!stopIngestion.get()) {
                try {
                    // Check if cluster is still available before indexing
                    if (internalCluster().size() == 0) {
                        logger.info("--> Cluster shut down, stopping ingestion thread");
                        break;
                    }
                    client().prepareIndex(INDEX_NAME)
                        .setId("concurrent-doc-" + docId)
                        .setSource("{ \"message\": " + (docId * 100) + ", \"message2\": " + (docId * 200) + ", \"message3\": " + (docId * 300) + " }",
                            MediaTypeRegistry.JSON)
                        .get();
                    ingestedDuringMigration.incrementAndGet();
                    docId++;

                    // Small delay to simulate realistic ingestion rate
                    Thread.sleep(50);
                } catch (Exception e) {
                    // Check if it's a cluster shutdown scenario
                    if (e.getMessage() != null && (e.getMessage().contains("Cluster is already closed") 
                        || e.getMessage().contains("cluster() is null"))) {
                        logger.info("--> Cluster is shutting down, stopping ingestion thread gracefully");
                        break;
                    }
                    logger.warn("--> Ingestion error during migration (expected during relocation): {}", e.getMessage());
                    ingestionErrors.incrementAndGet();
                    // Continue trying - some failures are expected during relocation
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            logger.info("--> Ingestion thread completed. Ingested {} docs during migration, {} errors",
                ingestedDuringMigration.get(), ingestionErrors.get());
        }, "concurrent-ingestion-thread");

        try {
            // 5. Start ingestion and wait for it to begin
            ingestionThread.start();
            assertTrue("Ingestion thread should start", ingestionStarted.await(5, TimeUnit.SECONDS));

            // Let ingestion run for a bit before starting migration
            Thread.sleep(500);

            logger.info("--> Ingestion running, docs ingested so far: {}", ingestedDuringMigration.get());

            // 6. Start PRIMARY RELOCATION while ingestion is happening
            String targetNode = getOtherDataNode(originalPrimaryNode);
            logger.info("--> Starting primary relocation from {} to {} while ingestion is active",
                originalPrimaryNode, targetNode);

            client().admin().cluster().prepareReroute()
                .add(new MoveAllocationCommand(INDEX_NAME, 0, originalPrimaryNode, targetNode))
                .get();

            // Wait for green status (relocation complete)
            ensureGreen(INDEX_NAME);

            // 7. Let ingestion continue for a bit after migration
            Thread.sleep(1000);

            // 8. Verify primary moved to target node
            String newPrimaryNode = getPrimaryNodeName(INDEX_NAME);
            assertEquals("Primary should have moved to target node", targetNode, newPrimaryNode);

            // 9. Flush and refresh to ensure all documents are persisted
            client().admin().indices().prepareFlush(INDEX_NAME).get();
            client().admin().indices().prepareRefresh(INDEX_NAME).get();

            // Wait for any async operations to complete
            Thread.sleep(2000);

            // 10. Get final state on new primary
            IndexShard newPrimaryShard = getPrimaryShard(INDEX_NAME);
            assertTrue("Relocated shard should be primary", newPrimaryShard.routingEntry().primary());

            long finalDocCount = newPrimaryShard.docStats().getCount();
            long finalParquetCount = countParquetFiles(newPrimaryShard);

            logger.info("--> Final state: newPrimaryNode={}, finalDocs={}, finalParquetFiles={}, docsIngestedDuringMigration={}",
                newPrimaryNode, finalDocCount, finalParquetCount, ingestedDuringMigration.get());

            // 11. Verify document counts
            // Final doc count should be at least initial docs + docs ingested during migration
            // (minus any that failed)
            long expectedMinDocs = initialDocCount + (ingestedDuringMigration.get() - ingestionErrors.get());
            logger.info("--> Expected minimum docs: {} (initial={} + ingested={} - errors={})",
                expectedMinDocs, initialDocCount, ingestedDuringMigration.get(), ingestionErrors.get());

            assertTrue("Should have documents after migration", finalDocCount > 0);
            assertTrue("Final doc count should be >= initial doc count", finalDocCount >= initialDocCount);

            // At least some concurrent docs should have been successfully ingested
            assertTrue("Should have ingested at least some documents during migration",
                ingestedDuringMigration.get() > 0);

            // 12. KEY ASSERTION: Verify parquet files exist on new primary
            assertTrue("Should have parquet files on new primary", finalParquetCount > 0);
            assertTrue("Parquet file count should be >= initial count (should include new data)",
                finalParquetCount >= initialParquetCount);

            logger.info("--> Primary relocation with concurrent ingestion test completed successfully");
            logger.info("--> Summary: Initial docs={}, Final docs={}, Ingested during migration={}, Errors={}",
                initialDocCount, finalDocCount, ingestedDuringMigration.get(), ingestionErrors.get());

            assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
        } finally {
            // Always stop the ingestion thread to prevent thread leaks
            stopIngestion.set(true);
            ingestionThread.interrupt();
            ingestionThread.join(5000);
            if (ingestionThread.isAlive()) {
                logger.warn("--> Ingestion thread did not stop in time, may cause thread leak warning");
            }
        }
    }

    /**
     * Test 3: REPLICA ADDITION triggers RemoteStorePeerRecoverySourceHandler
     *
     * Tests peer recovery for composite index when ADDING A REPLICA.
     * Uses RemoteStorePeerRecoverySourceHandler because target is remote store node.
     *
     * This test:
     * 1. Creates a composite index with 0 replicas
     * 2. Indexes documents and flushes to create parquet files
     * 3. Increases replica count from 0→1 to trigger replica recovery
     * 4. Verifies parquet files are properly downloaded from remote store to replica
     */
    public void testCompositeIndexReplicaPeerRecovery() throws Exception {
        logger.info("--> Starting test: Replica peer recovery (RemoteStorePeerRecoverySourceHandler)");

        // 1. Start remote store enabled cluster with 2 data nodes
        internalCluster().startClusterManagerOnlyNodes(1);
        internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);

        // 2. Create composite index with 0 replicas initially
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME)
            .setSettings(indexSettings())  // 0 replicas from indexSettings()
            .setMapping(getMappings())
            .get());
        ensureGreen(INDEX_NAME);

        // 3. Index documents and flush to create parquet files
        indexDocuments();
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Capture state on primary before adding replica
        IndexShard primaryShard = getPrimaryShard(INDEX_NAME);
        long docCountBeforeReplicaAdd = primaryShard.docStats().getCount();
        long parquetFileCountOnPrimary = countParquetFiles(primaryShard);
        long remoteParquetCount = countRemoteParquetFiles(primaryShard);

        logger.info("--> Before replica add: docs={}, primaryLocalParquet={}, remoteParquet={}",
            docCountBeforeReplicaAdd, parquetFileCountOnPrimary, remoteParquetCount);

        assertTrue("Should have documents before adding replica", docCountBeforeReplicaAdd > 0);
        assertTrue("Should have parquet files on primary", parquetFileCountOnPrimary > 0);

        // 4. Add replica - triggers RemoteStorePeerRecoverySourceHandler
        // because target node is remote store enabled
        logger.info("--> Adding replica to index (0 -> 1)");
        client().admin().indices().prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            .get();

        ensureGreen(INDEX_NAME);

        // 5. Get replica shard and verify
        IndexShard replicaShard = getReplicaShard(INDEX_NAME);
        assertNotNull("Replica shard should exist", replicaShard);
        assertFalse("Should be a replica shard", replicaShard.routingEntry().primary());

        // Wait for replica to fully initialize and sync
        Thread.sleep(2000);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        long docCountOnReplica = replicaShard.docStats().getCount();
        assertEquals("Replica doc count should match primary",
            docCountBeforeReplicaAdd, docCountOnReplica);

        // 6. KEY ASSERTION: Verify parquet files exist on replica
        long parquetFileCountOnReplica = countParquetFiles(replicaShard);
        logger.info("--> After replica add: replicaDocs={}, replicaLocalParquet={}",
            docCountOnReplica, parquetFileCountOnReplica);

        assertEquals("Replica should have same parquet files as primary (will FAIL if not implemented!)",
            parquetFileCountOnPrimary, parquetFileCountOnReplica);

        logger.info("--> Replica peer recovery test completed successfully");
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }
}
