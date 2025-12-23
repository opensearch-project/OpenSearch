/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat;

import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.UploadedSegmentMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.junit.annotations.TestLogging;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;

/**
 * Integration tests for Parquet segment replication with remote store.
 * Tests CatalogSnapshot-based replication, format-aware file downloads, and replica recovery.
 */
@TestLogging(
    value = "org.opensearch.indices.replication:DEBUG," +
            "org.opensearch.index.shard.RemoteStoreRefreshListener:DEBUG," +
            "org.opensearch.indices.replication.RemoteStoreReplicationSource:DEBUG",
    reason = "Validate replication with CatalogSnapshot and format-aware downloads"
)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ParquetSegmentReplicationIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "parquet-replication-test-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(ParquetDataFormatPlugin.class)
        ).collect(Collectors.toList());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // Increase timeout for mock repository
            .put("cluster.remote_store.translog.transfer_timeout", "5m")
            .build();
    }

    /**
     * Creates index with explicit mapping and segment replication enabled.
     */
    private void createReplicationIndex(String indexName, int replicaCount) throws Exception {
        client().admin().indices().prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put(remoteStoreIndexSettings(replicaCount, 1))
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                    .put(IndexSettings.OPTIMIZED_INDEX_ENABLED_SETTING.getKey(), true)
                    .build()
            )
            .setMapping(
                "id", "type=keyword",
                "field", "type=text",
                "value", "type=long"
            )
            .get();
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);
    }

    /**
     * Tests single refresh replication - index all documents then refresh once.
     * Verifies that replica fetches a single replication checkpoint.
     */
    public void testSingleRefreshReplication() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        createReplicationIndex(INDEX_NAME, 1);

        String primaryNode = getPrimaryNodeName(INDEX_NAME);
        String replicaNode = getReplicaNodeName(INDEX_NAME);

        IndexShard primaryShard = getIndexShard(primaryNode, INDEX_NAME);
        long initialVersion = primaryShard.getLatestReplicationCheckpoint().getSegmentInfosVersion();
        logger.info("--> Initial primary checkpoint version: {}", initialVersion);

        // Index documents WITHOUT immediate refresh
        int numDocs = randomIntBetween(20, 50);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("id", String.valueOf(i), "field", "value" + i, "value", i * 100L)
                .get();
        }
        logger.info("--> Indexed {} documents without refresh", numDocs);

        // Single refresh after all documents
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        logger.info("--> Performed single refresh");

        long primaryVersionAfterRefresh = primaryShard.getLatestReplicationCheckpoint().getSegmentInfosVersion();
        logger.info("--> Primary checkpoint version after refresh: {}", primaryVersionAfterRefresh);
        assertTrue("Primary version should increment after refresh", primaryVersionAfterRefresh > initialVersion);

        // Wait for replica to catch up
        assertBusy(() -> {
            IndexShard primary = getIndexShard(primaryNode, INDEX_NAME);
            IndexShard replica = getIndexShard(replicaNode, INDEX_NAME);

            long primaryVersion = primary.getLatestReplicationCheckpoint().getSegmentInfosVersion();
            long replicaVersion = replica.getLatestReplicationCheckpoint().getSegmentInfosVersion();

            logger.info("--> Primary version: {}, Replica version: {}", primaryVersion, replicaVersion);

            // Verify replica caught up to primary
            assertEquals("Replica should match primary checkpoint version", primaryVersion, replicaVersion);

            // Verify replica has Parquet files
            RemoteSegmentStoreDirectory replicaRemoteDir = replica.getRemoteDirectory();
            Map<String, UploadedSegmentMetadata> replicaSegments =
                replicaRemoteDir.getSegmentsUploadedToRemoteStore();

            assertFalse("Replica should have uploaded segments", replicaSegments.isEmpty());

            Set<String> replicaFormats = replicaSegments.keySet().stream()
                .map(file -> new FileMetadata(file).dataFormat())
                .collect(Collectors.toSet());

            logger.info("--> Replica formats: {}", replicaFormats);
            assertTrue("Replica should have Parquet files", replicaFormats.contains("parquet"));

        }, 120, TimeUnit.SECONDS);
    }

    /**
     * Tests that Parquet files are replicated from primary to replica
     * using CatalogSnapshot-based replication.
     */
    public void testParquetFilesReplicatedToReplica() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        createReplicationIndex(INDEX_NAME, 1);

        // Index documents on primary
        int numDocs = randomIntBetween(10, 30);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("id", String.valueOf(i), "field", "value" + i, "value", i * 100L)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }

        // Wait for replication to complete
        String primaryNode = getPrimaryNodeName(INDEX_NAME);
        String replicaNode = getReplicaNodeName(INDEX_NAME);

        assertBusy(() -> {
            IndexShard primaryShard = getIndexShard(primaryNode, INDEX_NAME);
            IndexShard replicaShard = getIndexShard(replicaNode, INDEX_NAME);

            // Verify both shards have same checkpoint
            assertEquals(
                "Primary and replica should have same checkpoint version",
                primaryShard.getLatestReplicationCheckpoint().getSegmentInfosVersion(),
                replicaShard.getLatestReplicationCheckpoint().getSegmentInfosVersion()
            );

            // Verify replica has Parquet files
            RemoteSegmentStoreDirectory replicaRemoteDir = replicaShard.getRemoteDirectory();
            Map<String, UploadedSegmentMetadata> replicaSegments =
                replicaRemoteDir.getSegmentsUploadedToRemoteStore();

            Set<String> replicaFormats = replicaSegments.keySet().stream()
                .map(file -> new FileMetadata(file).dataFormat())
                .collect(Collectors.toSet());

            logger.info("--> Replica formats: {}", replicaFormats);
            assertTrue("Replica should have Parquet files", replicaFormats.contains("parquet"));

        }, 120, TimeUnit.SECONDS);
    }

    /**
     * Tests that CatalogSnapshot is correctly downloaded and applied to replica.
     */
    public void testCatalogSnapshotReplication() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        createReplicationIndex(INDEX_NAME, 1);

        // Index documents
        for (int i = 0; i < randomIntBetween(20, 50); i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("id", String.valueOf(i), "field", "test" + i, "value", (long) i)
                .get();
        }
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        String primaryNode = getPrimaryNodeName(INDEX_NAME);
        String replicaNode = getReplicaNodeName(INDEX_NAME);

        assertBusy(() -> {
            IndexShard primaryShard = getIndexShard(primaryNode, INDEX_NAME);
            IndexShard replicaShard = getIndexShard(replicaNode, INDEX_NAME);

            // Verify CatalogSnapshot metadata is replicated
            RemoteSegmentStoreDirectory primaryRemoteDir = primaryShard.getRemoteDirectory();
            RemoteSegmentStoreDirectory replicaRemoteDir = replicaShard.getRemoteDirectory();

            RemoteSegmentMetadata primaryMetadata = primaryRemoteDir.readLatestMetadataFile();
            RemoteSegmentMetadata replicaMetadata = replicaRemoteDir.readLatestMetadataFile();

            assertNotNull("Primary should have metadata", primaryMetadata);
            assertNotNull("Replica should have metadata", replicaMetadata);

            // Verify CatalogSnapshot bytes are present
            assertNotNull("Primary CatalogSnapshot bytes", primaryMetadata.getSegmentInfosBytes());
            assertNotNull("Replica CatalogSnapshot bytes", replicaMetadata.getSegmentInfosBytes());

            // Verify checkpoints match
            assertEquals(
                "Checkpoints should match",
                primaryMetadata.getReplicationCheckpoint().getSegmentInfosVersion(),
                replicaMetadata.getReplicationCheckpoint().getSegmentInfosVersion()
            );

        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Tests that format-aware metadata is correctly replicated.
     */
    public void testFormatAwareMetadataReplication() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        createReplicationIndex(INDEX_NAME, 1);

        // Index and refresh
        for (int i = 0; i < 15; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("id", String.valueOf(i), "field", "data" + i, "value", (long) i)
                .get();
        }
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        String primaryNode = getPrimaryNodeName(INDEX_NAME);
        String replicaNode = getReplicaNodeName(INDEX_NAME);

        assertBusy(() -> {
            IndexShard primaryShard = getIndexShard(primaryNode, INDEX_NAME);
            IndexShard replicaShard = getIndexShard(replicaNode, INDEX_NAME);

            RemoteSegmentMetadata primaryMetadata = primaryShard.getRemoteDirectory().readLatestMetadataFile();
            RemoteSegmentMetadata replicaMetadata = replicaShard.getRemoteDirectory().readLatestMetadataFile();

            // Verify format information is preserved in metadata
            Map<String, UploadedSegmentMetadata> primaryMetadataMap =
                primaryMetadata.getMetadata();
            Map<String, UploadedSegmentMetadata> replicaMetadataMap =
                replicaMetadata.getMetadata();

            // Check that FileMetadata keys have format information
            for (String file : replicaMetadataMap.keySet()) {
                FileMetadata fileMetadata = new FileMetadata(file);
                assertNotNull("FileMetadata should have format", fileMetadata.dataFormat());
                assertEquals("Format should match in uploaded metadata",
                    fileMetadata.dataFormat(),
                    replicaMetadataMap.get(file).getDataFormat()
                );
            }

            // Verify replica has same formats as primary
            Set<String> primaryFormats = primaryMetadataMap.keySet().stream()
                .map(file -> new FileMetadata(file).dataFormat())
                .collect(Collectors.toSet());
            Set<String> replicaFormats = replicaMetadataMap.keySet().stream()
                .map(file -> new FileMetadata(file).dataFormat())
                .collect(Collectors.toSet());

            assertEquals("Replica should have same formats as primary", primaryFormats, replicaFormats);

        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Tests that replica can recover from remote store with Parquet files.
     */
//    public void testReplicaRecoveryWithParquetFiles() throws Exception {
//        internalCluster().startClusterManagerOnlyNode();
//        internalCluster().startDataOnlyNodes(2);
//        createReplicationIndex(INDEX_NAME, 1);
//
//        // Index documents
//        for (int i = 0; i < 20; i++) {
//            client().prepareIndex(INDEX_NAME)
//                .setId(String.valueOf(i))
//                .setSource("id", String.valueOf(i), "field", "recovery" + i, "value", (long) i)
//                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
//                .get();
//        }
//
//        String primaryNode = getPrimaryNodeName(INDEX_NAME);
//        String replicaNode = getReplicaNodeName(INDEX_NAME);
//
//        // Wait for initial replication
//        assertBusy(() -> {
//            IndexShard primaryShard = getIndexShard(primaryNode, INDEX_NAME);
//            IndexShard replicaShard = getIndexShard(replicaNode, INDEX_NAME);
//            assertEquals(
//                primaryShard.getLatestReplicationCheckpoint().getSegmentInfosVersion(),
//                replicaShard.getLatestReplicationCheckpoint().getSegmentInfosVersion()
//            );
//        }, 30, TimeUnit.SECONDS);
//
//        // Stop replica node to simulate failure
//        internalCluster().restartNode(replicaNode, new InternalTestCluster.RestartCallback() {
//            @Override
//            public Settings onNodeStopped(String nodeName) throws Exception {
//                // Index more documents on primary while replica is down
//                try {
//                    for (int i = 20; i < 40; i++) {
//                        client().prepareIndex(INDEX_NAME)
//                            .setId(String.valueOf(i))
//                            .setSource("id", String.valueOf(i), "field", "after_failure" + i, "value", (long) i)
//                            .get();
//                    }
//                    client().admin().indices().prepareRefresh(INDEX_NAME).get();
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//                return super.onNodeStopped(nodeName);
//            }
//        });
//
//        ensureGreen(INDEX_NAME);
//
//        // Verify replica recovered with Parquet files
//        assertBusy(() -> {
//            IndexShard primaryShard = getIndexShard(primaryNode, INDEX_NAME);
//            IndexShard replicaShard = getIndexShard(replicaNode, INDEX_NAME);
//
//            // Verify checkpoints match after recovery
//            assertEquals(
//                "Replica should catch up after recovery",
//                primaryShard.getLatestReplicationCheckpoint().getSegmentInfosVersion(),
//                replicaShard.getLatestReplicationCheckpoint().getSegmentInfosVersion()
//            );
//
//            // Verify replica has Parquet files
//            RemoteSegmentStoreDirectory replicaRemoteDir = replicaShard.getRemoteDirectory();
//            Map<String, UploadedSegmentMetadata> replicaSegments =
//                replicaRemoteDir.getSegmentsUploadedToRemoteStore();
//
//            Set<String> formats = replicaSegments.keySet().stream()
//                .map(file -> new FileMetadata(file).dataFormat())
//                .collect(Collectors.toSet());
//
//            assertTrue("Recovered replica should have Parquet files", formats.contains("parquet"));
//
//        }, 60, TimeUnit.SECONDS);
//    }

    /**
     * Tests that ReplicationCheckpoint contains format-aware metadata.
     */
    public void testReplicationCheckpointWithFormatAwareMetadata() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        createReplicationIndex(INDEX_NAME, 1);

        // Index documents
        for (int i = 0; i < 10; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("id", String.valueOf(i), "field", "checkpoint" + i, "value", (long) i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }

        String primaryNode = getPrimaryNodeName(INDEX_NAME);
        String replicaNode = getReplicaNodeName(INDEX_NAME);

        assertBusy(() -> {
            IndexShard primaryShard = getIndexShard(primaryNode, INDEX_NAME);
            IndexShard replicaShard = getIndexShard(replicaNode, INDEX_NAME);

            // Get checkpoints
            var primaryCheckpoint = primaryShard.getLatestReplicationCheckpoint();
            var replicaCheckpoint = replicaShard.getLatestReplicationCheckpoint();

            // Verify checkpoints have format-aware metadata
            Map<FileMetadata, org.opensearch.index.store.StoreFileMetadata> primaryMetadataMap =
                primaryCheckpoint.getFormatAwareMetadataMap();
            Map<FileMetadata, org.opensearch.index.store.StoreFileMetadata> replicaMetadataMap =
                replicaCheckpoint.getFormatAwareMetadataMap();

            assertNotNull("Primary checkpoint should have format-aware metadata", primaryMetadataMap);
            assertNotNull("Replica checkpoint should have format-aware metadata", replicaMetadataMap);

            // Verify FileMetadata keys have format information
            for (FileMetadata fm : replicaMetadataMap.keySet()) {
                assertNotNull("FileMetadata should have format", fm.dataFormat());
                assertFalse("Format should not be empty", fm.dataFormat().isEmpty());
            }

            // Verify versions match
            assertEquals(
                "Checkpoint versions should match",
                primaryCheckpoint.getSegmentInfosVersion(),
                replicaCheckpoint.getSegmentInfosVersion()
            );

        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Tests multiple refresh cycles with replication.
     */
    public void testMultipleRefreshCyclesWithReplication() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        createReplicationIndex(INDEX_NAME, 1);

        String primaryNode = getPrimaryNodeName(INDEX_NAME);
        String replicaNode = getReplicaNodeName(INDEX_NAME);

        IndexShard primaryShard = getIndexShard(primaryNode, INDEX_NAME);
        long previousVersion = primaryShard.getLatestReplicationCheckpoint().getSegmentInfosVersion();

        // Multiple index and refresh cycles
        for (int cycle = 0; cycle < 5; cycle++) {
            // Index documents
            for (int i = 0; i < 5; i++) {
                client().prepareIndex(INDEX_NAME)
                    .setId("cycle" + cycle + "_doc" + i)
                    .setSource("id", "c" + cycle + "d" + i, "field", "cycle" + cycle, "value", (long) i)
                    .get();
            }
            client().admin().indices().prepareRefresh(INDEX_NAME).get();

            final int currentCycle = cycle;
            long finalPreviousVersion = previousVersion;
            assertBusy(() -> {
                IndexShard primary = getIndexShard(primaryNode, INDEX_NAME);
                IndexShard replica = getIndexShard(replicaNode, INDEX_NAME);

                long primaryVersion = primary.getLatestReplicationCheckpoint().getSegmentInfosVersion();
                long replicaVersion = replica.getLatestReplicationCheckpoint().getSegmentInfosVersion();

                logger.info("--> Cycle {}: Primary version={}, Replica version={}",
                    currentCycle, primaryVersion, replicaVersion);

                // Verify version incremented
                assertTrue("Version should increment after refresh", primaryVersion > finalPreviousVersion);

                // Verify replica caught up
                assertEquals("Replica should match primary version", primaryVersion, replicaVersion);

            }, 30, TimeUnit.SECONDS);

            previousVersion = primaryShard.getLatestReplicationCheckpoint().getSegmentInfosVersion();
        }
    }

    /**
     * Helper to get primary node name for an index.
     */
    private String getPrimaryNodeName(String indexName) throws Exception {
        IndicesStatsResponse response = client().admin().indices().stats(new IndicesStatsRequest().indices(indexName)).get();
        String nodeId = response.getShards()[0].getShardRouting().currentNodeId();
        for (String nodeName : internalCluster().getNodeNames()) {
            if (internalCluster().clusterService(nodeName).localNode().getId().equals(nodeId)) {
                return nodeName;
            }
        }
        throw new IllegalStateException("Node not found for ID: " + nodeId);
    }

    /**
     * Helper to get replica node name for an index.
     */
    private String getReplicaNodeName(String indexName) throws Exception {
        IndicesStatsResponse response = client().admin().indices().stats(new IndicesStatsRequest().indices(indexName)).get();
        for (org.opensearch.action.admin.indices.stats.ShardStats shard : response.getShards()) {
            if (!shard.getShardRouting().primary()) {
                String nodeId = shard.getShardRouting().currentNodeId();
                for (String nodeName : internalCluster().getNodeNames()) {
                    if (internalCluster().clusterService(nodeName).localNode().getId().equals(nodeId)) {
                        return nodeName;
                    }
                }
                throw new IllegalStateException("Node not found for ID: " + nodeId);
            }
        }
        throw new IllegalStateException("No replica found for index: " + indexName);
    }
}
