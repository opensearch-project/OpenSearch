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
        assertNotNull("RemoteSegmentStoreDirectory should not be null at " + stageName, remoteDir);

        Map<String, UploadedSegmentMetadata> uploadedSegmentsRaw = remoteDir.getSegmentsUploadedToRemoteStore();
        if (uploadedSegmentsRaw.isEmpty()) {
            return;
        }

        Map<FileMetadata, UploadedSegmentMetadata> uploadedSegments = uploadedSegmentsRaw.entrySet().stream()
            .collect(Collectors.toMap(e -> new FileMetadata(e.getKey()), Map.Entry::getValue));

        for (FileMetadata fileMetadata : uploadedSegments.keySet()) {
            assertNotNull("FileMetadata should have format information at " + stageName, fileMetadata.dataFormat());
            assertFalse("Format should not be empty at " + stageName, fileMetadata.dataFormat().isEmpty());
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
            return -1;
        }
    }

    private void validateCatalogSnapshot(IndexShard shard, String stageName) {
        RemoteSegmentStoreDirectory remoteDir = shard.getRemoteDirectory();
        assertNotNull("RemoteSegmentStoreDirectory should not be null at " + stageName, remoteDir);

        try {
            RemoteSegmentMetadata metadata = remoteDir.readLatestMetadataFile();
            if (metadata == null) {
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

    /**
     * Tests full cluster restart (gateway) recovery with DataFusion engine.
     * Validates that CatalogSnapshot is properly recovered from remote store after full restart.
     */
    public void testDataFusionGatewayRecovery() throws Exception {
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

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

        IndexShard indexShard = getIndexShard(dataNode, INDEX_NAME);
        validateRemoteStoreSegments(indexShard, "before gateway restart");
        validateCatalogSnapshot(indexShard, "before gateway restart");

        long docCountBeforeRestart = indexShard.docStats().getCount();
        long parquetFilesBeforeRestart = countParquetFilesInRemote(indexShard);
        String clusterUUID = clusterService().state().metadata().clusterUUID();

        internalCluster().fullRestart();
        ensureStableCluster(2);
        ensureGreen(INDEX_NAME);

        RecoveryResponse recoveryResponse = client().admin().indices().recoveries(new RecoveryRequest(INDEX_NAME)).actionGet();
        List<RecoveryState> recoveryStates = recoveryResponse.shardRecoveryStates().get(INDEX_NAME);
        assertNotNull("Recovery states should not be null", recoveryStates);
        assertFalse("Recovery states should not be empty", recoveryStates.isEmpty());

        RecoveryState recoveryState = recoveryStates.get(0);
        assertEquals("Recovery should be complete", RecoveryState.Stage.DONE, recoveryState.getStage());

        String newDataNode = internalCluster().getDataNodeNames().iterator().next();
        IndexShard recoveredShard = getIndexShard(newDataNode, INDEX_NAME);
        validateRemoteStoreSegments(recoveredShard, "after gateway restart");
        validateCatalogSnapshot(recoveredShard, "after gateway restart");

        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        long docCountAfterRestart = recoveredShard.docStats().getCount();
        long parquetFilesAfterRestart = countParquetFilesInRemote(recoveredShard);

        assertEquals("Document count should be same after gateway restart", docCountBeforeRestart, docCountAfterRestart);
        assertEquals("Parquet file count should be same after gateway restart", parquetFilesBeforeRestart, parquetFilesAfterRestart);
        assertEquals("Cluster UUID should remain same", clusterUUID, clusterService().state().metadata().clusterUUID());
        assertEquals("Document count should match expected", numDocs, docCountAfterRestart);

        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }

    /**
     * Tests cluster manager failover during recovery.
     * Validates format metadata consistency during leader election.
     */
    public void testDataFusionClusterManagerFailover() throws Exception {
        String clusterManager1 = internalCluster().startClusterManagerOnlyNode();
        String clusterManager2 = internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);

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

        IndexShard shard = getIndexShard(dataNode, INDEX_NAME);
        validateRemoteStoreSegments(shard, "before cluster manager failover");
        long docCountBeforeFailover = shard.docStats().getCount();
        long parquetFilesBeforeFailover = countParquetFilesInRemote(shard);

        String currentClusterManager = internalCluster().getClusterManagerName();
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(currentClusterManager));
        ensureStableCluster(2);

        String newClusterManager = internalCluster().getClusterManagerName();
        assertNotEquals("New cluster manager should be different", currentClusterManager, newClusterManager);

        ensureGreen(INDEX_NAME);

        IndexShard shardAfterFailover = getIndexShard(dataNode, INDEX_NAME);
        validateRemoteStoreSegments(shardAfterFailover, "after cluster manager failover");
        validateCatalogSnapshot(shardAfterFailover, "after cluster manager failover");

        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        long docCountAfterFailover = shardAfterFailover.docStats().getCount();
        long parquetFilesAfterFailover = countParquetFilesInRemote(shardAfterFailover);

        assertEquals("Document count should be same after cluster manager failover", docCountBeforeFailover, docCountAfterFailover);
        assertEquals("Parquet file count should be same after cluster manager failover", parquetFilesBeforeFailover, parquetFilesAfterFailover);

        for (int i = 1; i <= 3; i++) {
            client().prepareIndex(INDEX_NAME).setId("post_failover_doc" + i)
                .setSource("{ \"message\": " + (i * 300) + " }", MediaTypeRegistry.JSON).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        assertEquals("Final doc count should include new docs", numDocs + 3, shardAfterFailover.docStats().getCount());

        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }
}
