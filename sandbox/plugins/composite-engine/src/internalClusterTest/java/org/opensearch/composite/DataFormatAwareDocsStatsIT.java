/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.admin.indices.segments.IndexSegments;
import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.admin.indices.segments.ShardSegments;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.List;

/**
 * Doc-count coverage for deletes and updates on a composite (parquet primary + lucene secondary)
 * index.
 *
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class DataFormatAwareDocsStatsIT extends AbstractCompositeEngineIT {

    private static final String INDEX = "dfae_docs_stats";

    /**
     * Composite index with a Lucene secondary (needed for {@code _id} resolution, and the only format
     * that tracks liveness) and auto-refresh off, so every measurement below happens at a point the
     * test chose rather than wherever a background refresh landed.
     */
    private void createManualRefreshIndex() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            // Enabling the pluggable data format switches append-only on by default, which would
            // reject both a custom _id and a delete. It is a final setting, so it must be set here.
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), false)
            .put("index.refresh_interval", -1)
            .build();
        client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(settings)
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(INDEX);
    }

    private void indexDoc(String id, String name, int value) {
        client().prepareIndex(INDEX).setId(id).setSource("name", name, "value", value).get();
    }

    private DocsStats docsStats() {
        IndicesStatsResponse response = client().admin().indices().prepareStats(INDEX).clear().setDocs(true).get();
        return response.getIndex(INDEX).getShards()[0].getStats().getDocs();
    }

    private List<Segment> segments() {
        IndicesSegmentResponse response = client().admin().indices().prepareSegments(INDEX).get();
        IndexSegments indexSegments = response.getIndices().get(INDEX);
        assertNotNull("index segments must exist", indexSegments);
        ShardSegments[] shardSegments = indexSegments.getShards().values().iterator().next().getShards();
        assertTrue("must have shard segments", shardSegments.length > 0);
        return shardSegments[0].getSegments();
    }

    private void assertDocsStats(String context, long expectedCount, long expectedDeleted) {
        DocsStats stats = docsStats();
        assertEquals(context + ": docs.count", expectedCount, stats.getCount());
        assertEquals(context + ": docs.deleted", expectedDeleted, stats.getDeleted());
    }

    private void assertLiveCountAndAccountedRows(String context, long expectedCount) throws IOException {
        DocsStats stats = docsStats();
        assertEquals(context + ": docs.count", expectedCount, stats.getCount());
        long rowsInCatalog = acquireAndGetSnapshot(INDEX).getNumDocs();
        assertEquals(
            context + ": docs.count + docs.deleted must account for every row the catalog holds",
            rowsInCatalog,
            stats.getCount() + stats.getDeleted()
        );
    }

    private void assertSegmentCountsAgreeWithDocsStats(String context) {
        long segmentDocs = 0;
        long segmentDeleted = 0;
        for (Segment segment : segments()) {
            segmentDocs += segment.getNumDocs();
            segmentDeleted += segment.getDeletedDocs();
        }
        DocsStats stats = docsStats();
        assertEquals(context + ": summed segment docs must equal docs.count", stats.getCount(), segmentDocs);
        assertEquals(context + ": summed segment deleted docs must equal docs.deleted", stats.getDeleted(), segmentDeleted);
    }

    public void testDeleteAcrossRefreshMovesDocsToDeleted() {
        createManualRefreshIndex();

        for (int i = 1; i <= 10; i++) {
            indexDoc("k" + i, "v" + i, i);
        }
        refreshIndex(INDEX);
        assertDocsStats("after indexing 10 docs", 10L, 0L);
        assertSegmentCountsAgreeWithDocsStats("after indexing 10 docs");

        client().prepareDelete(INDEX, "k1").get();
        client().prepareDelete(INDEX, "k2").get();
        client().prepareDelete(INDEX, "k3").get();
        refreshIndex(INDEX);

        assertDocsStats("after deleting 3 of 10", 7L, 3L);
        assertSegmentCountsAgreeWithDocsStats("after deleting 3 of 10");
    }

    public void testDeleteWithinRefreshWindowMovesDocsToDeleted() {
        createManualRefreshIndex();

        for (int i = 1; i <= 6; i++) {
            indexDoc("k" + i, "v" + i, i);
        }
        // No refresh yet — these deletes hit rows the active writer has not flushed.
        client().prepareDelete(INDEX, "k1").get();
        client().prepareDelete(INDEX, "k2").get();
        refreshIndex(INDEX);

        assertDocsStats("after deleting 2 of 6 before any refresh", 4L, 2L);
        assertSegmentCountsAgreeWithDocsStats("after deleting 2 of 6 before any refresh");
    }

    public void testUpdateKeepsCountAndRaisesDeleted() {
        createManualRefreshIndex();

        for (int i = 1; i <= 5; i++) {
            indexDoc("k" + i, "v" + i, i);
        }
        refreshIndex(INDEX);
        assertDocsStats("after indexing 5 docs", 5L, 0L);

        // Update two documents. Each appends a row and hides its predecessor: 7 rows on disk, still
        // 5 reachable documents.
        indexDoc("k1", "v1_updated", 101);
        indexDoc("k2", "v2_updated", 102);
        refreshIndex(INDEX);

        assertDocsStats("after updating 2 of 5", 5L, 2L);
        assertSegmentCountsAgreeWithDocsStats("after updating 2 of 5");

        // The counted documents must be the ones a reader resolves to, and to their new content. Read
        // by id rather than by search: this engine rejects the Lucene search path.
        for (int i = 1; i <= 5; i++) {
            assertTrue("k" + i + " must still be reachable", client().prepareGet(INDEX, "k" + i).setRealtime(false).get().isExists());
        }
        assertEquals("v1_updated", client().prepareGet(INDEX, "k1").setRealtime(false).get().getSourceAsMap().get("name"));
    }

    public void testRepeatedUpdatesThenDelete() throws IOException {
        createManualRefreshIndex();

        indexDoc("k1", "v0", 0);
        indexDoc("anchor", "anchor", 0);
        refreshIndex(INDEX);
        assertDocsStats("after indexing 2 docs", 2L, 0L);

        // Each update appends a row and hides its predecessor, so the two documents stay reachable no
        // matter how many copies of k1 accumulate — and no matter whether a merge reclaims some of them.
        for (int i = 1; i <= 4; i++) {
            indexDoc("k1", "v" + i, i);
            refreshIndex(INDEX);
            assertLiveCountAndAccountedRows("after update " + i, 2L);
        }
        assertEquals("v4", client().prepareGet(INDEX, "k1").setRealtime(false).get().getSourceAsMap().get("name"));

        client().prepareDelete(INDEX, "k1").get();
        refreshIndex(INDEX);
        // Every copy of k1 is hidden now; only the anchor stays reachable.
        assertLiveCountAndAccountedRows("after deleting the updated doc", 1L);
        assertTrue(
            "the deleted doc's rows must be reported as deleted, not as live",
            docsStats().getDeleted() >= 1L
        );
        assertFalse("k1 must no longer resolve", client().prepareGet(INDEX, "k1").setRealtime(false).get().isExists());
        assertSegmentCountsAgreeWithDocsStats("after deleting the updated doc");
    }

    public void testCountsSurviveFlushAndForceMerge() {
        createManualRefreshIndex();

        for (int i = 1; i <= 8; i++) {
            indexDoc("k" + i, "v" + i, i);
        }
        refreshIndex(INDEX);

        indexDoc("k1", "v1_updated", 101);
        client().prepareDelete(INDEX, "k2").get();
        refreshIndex(INDEX);
        assertDocsStats("after 1 update and 1 delete of 8", 7L, 2L);

        flushIndex(INDEX);
        assertDocsStats("after flush", 7L, 2L);

        client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();
        DocsStats afterMerge = docsStats();
        assertEquals("force merge must not change the reachable count", 7L, afterMerge.getCount());
        assertTrue(
            "force merge must not invent deleted docs, but reported " + afterMerge.getDeleted(),
            afterMerge.getDeleted() <= 2L
        );
        assertSegmentCountsAgreeWithDocsStats("after force merge");
    }
}
