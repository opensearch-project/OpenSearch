/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.FileMetadata;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ITs for DataFormatAwareEngine remote upload. Single primary, no replica; {@code Scope.TEST}
 * per test. Requires {@code -Dsandbox.enabled=true}.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataFormatAwareUploadIT extends RemoteStoreBaseIntegTestCase {

    protected static final String INDEX_NAME = "dfa-upload-test-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(ParquetDataFormatPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class, DataFusionPlugin.class)
        ).collect(Collectors.toList());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    /** Index settings for a 1-shard composite(parquet) index with remote store + segment replication. */
    protected Settings dfaIndexSettings(int replicaCount) {
        return Settings.builder()
            .put(remoteStoreIndexSettings(replicaCount, 1))
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", List.of())
            .build();
    }

    /** Creates {@link #INDEX_NAME} with the DFA settings and waits for green. */
    protected void createDfaIndex(int replicaCount) throws Exception {
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings(replicaCount)).get();
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);
    }

    /** Indexes {@code count} docs with predictable fields. */
    protected void indexDocs(int count) {
        for (int i = 0; i < count; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("field_text", randomAlphaOfLength(10), "field_keyword", randomAlphaOfLength(10), "field_number", (long) i)
                .get();
        }
    }

    /** Data node hosting the primary (replicaCount=0 so the only data node is it). */
    protected String primaryNodeName() {
        return internalCluster().getDataNodeNames().iterator().next();
    }

    /** Extract the set of distinct data formats from an uploaded-segments map. */
    protected static Set<String> formatsOf(Map<String, UploadedSegmentMetadata> segments) {
        return segments.keySet().stream().map(file -> new FileMetadata(file).dataFormat()).collect(Collectors.toSet());
    }

    /**
     * After a single flush, the primary's remote upload is consistent on every observable front:
     * non-empty upload map, parquet present, non-empty SegmentInfos bytes, a ReplicationCheckpoint,
     * every upload-map key has a non-empty {@code dataFormat}, and the catalog↔upload-map↔blob-store
     * agreement holds. Consolidates what were individual single-invariant tests.
     */
    public void testPrimaryUploadIsConsistentAfterFlush() throws Exception {
        prepareCluster(1, 1, Settings.EMPTY);
        createDfaIndex(0);
        indexDocs(randomIntBetween(10, 30));
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        final IndexShard primary = getIndexShard(primaryNodeName(), INDEX_NAME);
        assertBusy(() -> {
            // Upload map populated + includes parquet.
            Map<String, UploadedSegmentMetadata> uploaded = primary.getRemoteDirectory().getSegmentsUploadedToRemoteStore();
            assertFalse("expected uploaded segments, got empty map", uploaded.isEmpty());
            assertTrue("expected parquet in uploaded formats, got " + formatsOf(uploaded), formatsOf(uploaded).contains("parquet"));

            // Remote metadata file round-trip: non-null, non-empty SegmentInfos bytes, has ReplicationCheckpoint.
            RemoteSegmentMetadata metadata = primary.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull("expected metadata file", metadata);
            assertTrue(
                "expected non-empty SegmentInfos bytes",
                metadata.getSegmentInfosBytes() != null && metadata.getSegmentInfosBytes().length > 0
            );
            assertNotNull("expected non-null ReplicationCheckpoint", metadata.getReplicationCheckpoint());

            // Every file key in the metadata map has a non-empty dataFormat.
            Map<String, UploadedSegmentMetadata> metadataMap = metadata.getMetadata();
            assertFalse("expected non-empty metadata map", metadataMap.isEmpty());
            for (String fileKey : metadataMap.keySet()) {
                FileMetadata fm = new FileMetadata(fileKey);
                assertNotNull("FileMetadata.dataFormat() must be non-null for " + fileKey, fm.dataFormat());
                assertFalse("FileMetadata.dataFormat() must be non-empty for " + fileKey, fm.dataFormat().isEmpty());
            }

            // Catalog↔upload-map↔blob-store tier agreement.
            DataFormatAwareITUtils.assertCatalogMatchesUploadedBlobs(primary, client(), getNodeSettings(), segmentRepoPath);
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Across multiple flush cycles the checkpoint's {@code segmentInfosVersion} strictly advances,
     * SegmentInfos bytes stay non-empty, and the catalog↔upload tier agreement holds each cycle.
     */
    public void testUploadVersionAdvancesAcrossFlushCycles() throws Exception {
        prepareCluster(1, 1, Settings.EMPTY);
        createDfaIndex(0);

        final IndexShard primary = getIndexShard(primaryNodeName(), INDEX_NAME);
        long previousVersion = -1L;
        for (int i = 0; i < 5; i++) {
            indexDocs(5);
            client().admin().indices().prepareRefresh(INDEX_NAME).get();
            client().admin().indices().prepareFlush(INDEX_NAME).get();

            final long capturedPrev = previousVersion;
            final int iteration = i;
            assertBusy(() -> {
                RemoteSegmentMetadata metadata = primary.getRemoteDirectory().readLatestMetadataFile();
                assertNotNull("expected metadata at cycle " + iteration, metadata);
                assertTrue(
                    "expected non-empty SegmentInfos bytes at cycle " + iteration,
                    metadata.getSegmentInfosBytes() != null && metadata.getSegmentInfosBytes().length > 0
                );
                long version = metadata.getReplicationCheckpoint().getSegmentInfosVersion();
                assertTrue(
                    "version should strictly advance at cycle " + iteration + ": previous=" + capturedPrev + ", current=" + version,
                    version > capturedPrev
                );
                DataFormatAwareITUtils.assertCatalogMatchesUploadedBlobs(primary, client(), getNodeSettings(), segmentRepoPath);
            }, 60, TimeUnit.SECONDS);

            previousVersion = primary.getRemoteDirectory().readLatestMetadataFile().getReplicationCheckpoint().getSegmentInfosVersion();
        }
    }

    /**
     * Refresh without flush: the DFA engine publishes segments to the remote upload map without
     * requiring a flush. Validates the refresh-only path end-to-end.
     */
    public void testUploadAfterRefreshOnly() throws Exception {
        prepareCluster(1, 1, Settings.EMPTY);
        createDfaIndex(0);
        indexDocs(randomIntBetween(10, 30));
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        final IndexShard primary = getIndexShard(primaryNodeName(), INDEX_NAME);
        assertBusy(() -> {
            Map<String, UploadedSegmentMetadata> uploaded = primary.getRemoteDirectory().getSegmentsUploadedToRemoteStore();
            assertFalse("refresh alone must populate the upload map", uploaded.isEmpty());
            assertTrue("expected parquet in uploaded formats, got " + formatsOf(uploaded), formatsOf(uploaded).contains("parquet"));
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Mixed refresh + flush sequence. Every flush publishes a metadata file whose
     * {@code segmentInfosVersion} strictly advances over the previous flush, even when
     * interleaved with refresh-only rounds.
     */
    public void testMixedRefreshAndFlushCycles() throws Exception {
        prepareCluster(1, 1, Settings.EMPTY);
        createDfaIndex(0);

        final IndexShard primary = getIndexShard(primaryNodeName(), INDEX_NAME);
        long previousFlushVersion = -1L;

        // Pattern: refresh, refresh, flush, refresh, flush, flush.
        boolean[] isFlush = { false, false, true, false, true, true };
        for (int i = 0; i < isFlush.length; i++) {
            indexDocs(5);
            if (isFlush[i]) {
                client().admin().indices().prepareFlush(INDEX_NAME).get();
            } else {
                client().admin().indices().prepareRefresh(INDEX_NAME).get();
            }

            if (isFlush[i]) {
                final long capturedPrev = previousFlushVersion;
                final int iteration = i;
                assertBusy(() -> {
                    RemoteSegmentMetadata metadata = primary.getRemoteDirectory().readLatestMetadataFile();
                    assertNotNull("expected metadata after flush at step " + iteration, metadata);
                    long version = metadata.getReplicationCheckpoint().getSegmentInfosVersion();
                    assertTrue(
                        "version must strictly advance at step " + iteration + ": previous=" + capturedPrev + ", current=" + version,
                        version > capturedPrev
                    );
                    DataFormatAwareITUtils.assertCatalogMatchesUploadedBlobs(primary, client(), getNodeSettings(), segmentRepoPath);
                }, 60, TimeUnit.SECONDS);
                previousFlushVersion = primary.getRemoteDirectory()
                    .readLatestMetadataFile()
                    .getReplicationCheckpoint()
                    .getSegmentInfosVersion();
            }
        }
    }
}
