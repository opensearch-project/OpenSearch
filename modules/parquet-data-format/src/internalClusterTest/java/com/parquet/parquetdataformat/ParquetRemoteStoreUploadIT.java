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
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.UploadedSegmentMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.junit.annotations.TestLogging;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.SEGMENTS;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.DATA;

/**
 * Integration test for Parquet data format with remote store upload functionality.
 * Tests multi-format segment upload, recovery, and replication scenarios.
 */
@TestLogging(
    value = "org.opensearch.index.shard.RemoteStoreRefreshListener:DEBUG," +
            "org.opensearch.index.shard.RemoteStoreUploaderService:DEBUG," +
            "org.opensearch.index.engine.exec.coord.CompositeEngine:DEBUG," +
            "org.opensearch.index.store.Store:DEBUG",
    reason = "Validate remote upload logs and engine lifecycle"
)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ParquetRemoteStoreUploadIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "parquet-remote-test-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(ParquetDataFormatPlugin.class)
        ).collect(Collectors.toList());
    }

    /**
     * Creates index with explicit mapping to avoid dynamic mapping.
     * Parquet plugin doesn't support dynamic mapping yet.
     */
    private void createIndexWithMapping(String indexName, int replicaCount, int shardCount) throws Exception {
        client().admin().indices().prepareCreate(indexName)
            .setSettings(remoteStoreIndexSettings(replicaCount, shardCount))
            .setMapping(
                "id", "type=keyword",
                "field", "type=text",
                "value", "type=long",
                "timestamp", "type=date"
            )
            .get();
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);
    }

    /**
     * Index a document using only predefined fields from mapping.
     */
    private void indexDocWithMapping(String indexName, int docId) {
        client().prepareIndex(indexName)
            .setId(String.valueOf(docId))
            .setSource(
                "id", String.valueOf(docId),
                "field", "test_value_" + docId,
                "value", docId * 100L,
                "timestamp", System.currentTimeMillis()
            )
            .get();
    }

    /**
     * Helper method to get segment files from a directory.
     * Gets all files that start with "_" (segment files).
     */
    protected Set<String> getSegmentFiles(Path location) {
        Set<String> segmentFiles = new HashSet<>();
        try {
            Files.walkFileTree(location, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    if (file.getFileName().toString().startsWith("_")) {
                        segmentFiles.add(file.getFileName().toString());
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            logger.error("Error reading segment files from {}", location, e);
        }
        return segmentFiles;
    }

    /**
     * Tests that Parquet files are uploaded to remote store alongside Lucene files
     * and validates format-aware directory structure in remote blob store.
     */
    public void testParquetFilesUploadedToRemoteStore() throws Exception {
        // Setup cluster with remote store
        prepareCluster(1, 1, Settings.EMPTY);
        createIndexWithMapping(INDEX_NAME, 0, 1);

        // Index documents to trigger Parquet file creation
        int numDocs = randomIntBetween(10, 50);
        for (int i = 0; i < numDocs; i++) {
            indexDocWithMapping(INDEX_NAME, i);
        }
        logger.info("--> Indexed {} documents", numDocs);

        // Force refresh to ensure files are uploaded
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        IndicesStatsResponse response = client().admin().indices().stats(new IndicesStatsRequest()).get();
        assertEquals(1, response.getShards().length);

        String indexName = response.getShards()[0].getShardRouting().index().getName();

        // Get remote segment store directory
        String dataNode = internalCluster().getDataNodeNames().stream().findFirst().get();
        IndexShard primaryShard = getIndexShard(dataNode, indexName);
        RemoteSegmentStoreDirectory remoteDirectory = primaryShard.getRemoteDirectory();

        // Verify format-aware metadata
        Map<String, UploadedSegmentMetadata> uploadedSegments =
            remoteDirectory.getSegmentsUploadedToRemoteStore();

        logger.info("--> Uploaded segments: {}", uploadedSegments.keySet());

        // Verify both Lucene and Parquet files are uploaded
        Set<String> formats = uploadedSegments.keySet().stream()
            .map(file -> new FileMetadata(file).dataFormat())
            .collect(Collectors.toSet());

        logger.info("--> Data formats found in uploaded segments: {}", formats);

        // Assert that we have Parquet files
        assertTrue("Expected Parquet format files", formats.contains("parquet"));

        // Verify Parquet blob path exists in remote store
        String segmentsPathPrefix = RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX.get(getNodeSettings());
        BlobPath shardBlobPath = getShardLevelBlobPath(
            client(),
            indexName,
            new BlobPath(),
            "0",
            SEGMENTS,
            DATA,
            segmentsPathPrefix
        );

        Path segmentDataRepoPath = segmentRepoPath.resolve(shardBlobPath.buildAsString());

        // Check for Parquet format subdirectory
        Path parquetFormatPath = segmentDataRepoPath.resolve("parquet");

        assertBusy(() -> {
            assertTrue("Parquet format directory should exist", Files.exists(parquetFormatPath));

            Set<String> parquetFiles = getSegmentFiles(parquetFormatPath);

            logger.info("--> Parquet files in remote: {}", parquetFiles);

            assertFalse("Parquet files should be uploaded", parquetFiles.isEmpty());
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Tests that CatalogSnapshot containing both Lucene and Parquet segments
     * is correctly serialized and uploaded to remote metadata.
     */
    public void testCatalogSnapshotWithMultiFormatUpload() throws Exception {
        prepareCluster(1, 1, Settings.EMPTY);
        createIndexWithMapping(INDEX_NAME, 0, 1);

        // Index documents
        int numDocs = randomIntBetween(20, 100);
        for (int i = 0; i < numDocs; i++) {
            indexDocWithMapping(INDEX_NAME, i);
        }
        logger.info("--> Indexed {} documents", numDocs);

        // Force refresh to trigger metadata upload
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        IndicesStatsResponse response = client().admin().indices().stats(new IndicesStatsRequest()).get();
        String indexName = response.getShards()[0].getShardRouting().index().getName();

        String dataNode = internalCluster().getDataNodeNames().stream().findFirst().get();
        IndexShard primaryShard = getIndexShard(dataNode, indexName);
        RemoteSegmentStoreDirectory remoteDirectory = primaryShard.getRemoteDirectory();

        // Read latest metadata file
        assertBusy(() -> {
            RemoteSegmentMetadata metadata = remoteDirectory.readLatestMetadataFile();
            assertNotNull("Metadata should be uploaded", metadata);

            // Verify CatalogSnapshot bytes are present
            byte[] catalogSnapshotBytes = metadata.getSegmentInfosBytes();
            assertNotNull("CatalogSnapshot bytes should be present", catalogSnapshotBytes);
            assertTrue("CatalogSnapshot should not be empty", catalogSnapshotBytes.length > 0);

            // Verify metadata contains files from both formats
            Map<String, UploadedSegmentMetadata> metadataMap = metadata.getMetadata();
            Set<String> formats = metadataMap.keySet().stream()
                .map(file -> new FileMetadata(file).dataFormat())
                .collect(Collectors.toSet());

            logger.info("--> Formats in metadata: {}", formats);
            // TODO update this assertion when we start adding TermDictionaries to lucene
//            assertTrue("Metadata should contain Lucene files", formats.contains("lucene"));
            assertTrue("Metadata should contain Parquet files", formats.contains("parquet"));

            // Verify ReplicationCheckpoint is present
            assertNotNull("ReplicationCheckpoint should be present", metadata.getReplicationCheckpoint());

        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Tests that CatalogSnapshot version is incremented on each refresh
     * and properly tracked in ReplicationCheckpoint.
     */
    public void testCatalogSnapshotVersionTracking() throws Exception {
        prepareCluster(1, 1, Settings.EMPTY);
        createIndexWithMapping(INDEX_NAME, 0, 1);

        IndicesStatsResponse response = client().admin().indices().stats(new IndicesStatsRequest()).get();
        String indexName = response.getShards()[0].getShardRouting().index().getName();

        String dataNode = internalCluster().getDataNodeNames().stream().findFirst().get();
        IndexShard primaryShard = getIndexShard(dataNode, indexName);

        long initialVersion = primaryShard.getLatestReplicationCheckpoint().getSegmentInfosVersion();
        logger.info("--> Initial checkpoint version: {}", initialVersion);

        // Index and refresh multiple times
        for (int i = 0; i < 5; i++) {
            indexDocWithMapping(INDEX_NAME, i);
            client().admin().indices().prepareRefresh(INDEX_NAME).get();

            long currentVersion = primaryShard.getLatestReplicationCheckpoint().getSegmentInfosVersion();
            logger.info("--> After refresh {}: version = {}", i + 1, currentVersion);

            assertTrue("Version should increment after refresh", currentVersion > initialVersion);
            initialVersion = currentVersion;
        }
    }

    /**
     * Tests that format-aware files are correctly routed to format-specific
     * blob containers in remote store.
     */
    public void testFormatSpecificBlobContainerRouting() throws Exception {
        prepareCluster(1, 1, Settings.EMPTY);
        createIndexWithMapping(INDEX_NAME, 0, 1);

        for (int i = 0; i < randomIntBetween(20, 50); i++) {
            indexDocWithMapping(INDEX_NAME, i);
        }
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        IndicesStatsResponse response = client().admin().indices().stats(new IndicesStatsRequest()).get();
        String indexName = response.getShards()[0].getShardRouting().index().getName();

        String segmentsPathPrefix = RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX.get(getNodeSettings());
        BlobPath shardBlobPath = getShardLevelBlobPath(
            client(),
            indexName,
            new BlobPath(),
            "0",
            SEGMENTS,
            DATA,
            segmentsPathPrefix
        );
        Path segmentDataRepoPath = segmentRepoPath.resolve(shardBlobPath.buildAsString());

        assertBusy(() -> {
            // Verify Parquet directory exists
            Path parquetDir = segmentDataRepoPath.resolve("parquet");

            assertTrue("Parquet directory should exist in remote store", Files.isDirectory(parquetDir));

            // Verify files are in correct directory
            Set<String> parquetFiles = getSegmentFiles(parquetDir);

            // Parquet files should contain .parquet in filename
            boolean hasParquetFiles = parquetFiles.stream()
                .anyMatch(f -> f.contains(".parquet"));
            assertTrue("Parquet directory should contain .parquet files", hasParquetFiles);

        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Tests that metadata file contains format-aware FileMetadata keys
     * and can be correctly deserialized.
     */
    public void testMetadataFileContainsFormatAwareKeys() throws Exception {
        prepareCluster(1, 1, Settings.EMPTY);
        createIndexWithMapping(INDEX_NAME, 0, 1);

        for (int i = 0; i < randomIntBetween(10, 30); i++) {
            indexDocWithMapping(INDEX_NAME, i);
        }
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        IndicesStatsResponse response = client().admin().indices().stats(new IndicesStatsRequest()).get();
        String indexName = response.getShards()[0].getShardRouting().index().getName();

        String dataNode = internalCluster().getDataNodeNames().stream().findFirst().get();
        IndexShard primaryShard = getIndexShard(dataNode, indexName);
        RemoteSegmentStoreDirectory remoteDirectory = primaryShard.getRemoteDirectory();

        assertBusy(() -> {
            RemoteSegmentMetadata metadata = remoteDirectory.readLatestMetadataFile();
            assertNotNull("Metadata file should exist", metadata);

            Map<String, UploadedSegmentMetadata> metadataMap = metadata.getMetadata();

            // Verify FileMetadata keys have correct format information
            for (String file : metadataMap.keySet()) {
                FileMetadata fileMetadata = new FileMetadata(file);
                assertNotNull("FileMetadata should have dataFormat", fileMetadata.dataFormat());
                assertFalse("DataFormat should not be empty", fileMetadata.dataFormat().isEmpty());

                UploadedSegmentMetadata uploadedMeta = metadataMap.get(file);
                assertEquals("UploadedSegmentMetadata format should match FileMetadata",
                    fileMetadata.dataFormat(), uploadedMeta.getDataFormat());

                logger.debug("--> File: {}, Format: {}, RemoteFile: {}",
                    fileMetadata.file(), fileMetadata.dataFormat(), uploadedMeta.getUploadedFilename());
            }

            // Verify we have Parquet entries
            long parquetCount = metadataMap.keySet().stream()
                .filter(fm -> "parquet".equals(new FileMetadata(fm).dataFormat()))
                .count();

            assertTrue("Should have Parquet files in metadata", parquetCount > 0);

        }, 60, TimeUnit.SECONDS);
    }
}
