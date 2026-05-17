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
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.Map;

/**
 * Integration tests for segments and segments stats APIs on DataFormatAwareEngine.
 *
 * Validates:
 * - segments() returns correct committed/searchable state before and after flush
 * - segments() returns correct docCount, sizeInBytes, generation ordering
 * - segmentsStats() returns correct segment count and file sizes
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class CompositeSegmentsStatsIT extends AbstractCompositeEngineIT {

    /**
     * Tests that after refresh (no flush), segments are searchable but NOT committed.
     * After flush, segments become committed.
     */
    public void testSegmentsCommittedAndSearchableState() {
        String indexName = "test-seg-committed-searchable";
        createCompositeIndex(indexName);
        indexDocs(indexName, 5, 0);
        refreshIndex(indexName);

        // Before flush: segments should be searchable but NOT committed
        List<Segment> segmentsBeforeFlush = getSegmentsFromApi(indexName);
        assertFalse("Should have segments after refresh", segmentsBeforeFlush.isEmpty());
        for (Segment seg : segmentsBeforeFlush) {
            assertTrue("Segment should be searchable before flush", seg.search);
            assertFalse("Segment should NOT be committed before flush", seg.committed);
        }

        // After flush: segments should be both searchable AND committed
        flushIndex(indexName);
        List<Segment> segmentsAfterFlush = getSegmentsFromApi(indexName);
        assertFalse("Should have segments after flush", segmentsAfterFlush.isEmpty());
        for (Segment seg : segmentsAfterFlush) {
            assertTrue("Segment should be searchable after flush", seg.search);
            assertTrue("Segment should be committed after flush", seg.committed);
        }
    }

    /**
     * Tests that segments report correct docCount and sizeInBytes.
     */
    public void testSegmentsDocCountAndSize() {
        String indexName = "test-seg-doccount-size";
        createCompositeIndex(indexName);
        indexDocs(indexName, 10, 0);
        refreshIndex(indexName);

        List<Segment> segments = getSegmentsFromApi(indexName);
        assertFalse("Should have segments", segments.isEmpty());

        int totalDocs = 0;
        long totalSize = 0;
        for (Segment seg : segments) {
            assertTrue("docCount should be > 0", seg.docCount > 0);
            assertEquals("delDocCount should be 0", 0, seg.delDocCount);
            assertTrue("sizeInBytes should be > 0", seg.sizeInBytes > 0);
            assertNotNull("Segment name should not be null", seg.getName());
            totalDocs += seg.docCount;
            totalSize += seg.sizeInBytes;
        }
        assertEquals("Total docs across segments should equal indexed docs", 10, totalDocs);
        assertTrue("Total size should be > 0", totalSize > 0);
    }

    /**
     * Tests that segments are sorted by generation when multiple refreshes create multiple segments.
     */
    public void testSegmentsSortedByGeneration() {
        String indexName = "test-seg-generation-order";
        createCompositeIndex(indexName);

        // Create multiple segments via multiple refresh cycles
        indexDocs(indexName, 5, 0);
        refreshIndex(indexName);
        indexDocs(indexName, 5, 5);
        refreshIndex(indexName);

        List<Segment> segments = getSegmentsFromApi(indexName);
        assertTrue("Should have multiple segments", segments.size() >= 2);

        for (int i = 1; i < segments.size(); i++) {
            assertTrue(
                "Segments should be sorted by generation: gen["
                    + (i - 1)
                    + "]="
                    + segments.get(i - 1).getGeneration()
                    + " should be <= gen["
                    + i
                    + "]="
                    + segments.get(i).getGeneration(),
                segments.get(i).getGeneration() >= segments.get(i - 1).getGeneration()
            );
        }
    }

    /**
     * Tests that segmentsStats returns correct segment count matching the segments API.
     */
    public void testSegmentsStatsCount() {
        String indexName = "test-segstats-count";
        createCompositeIndex(indexName);
        indexDocs(indexName, 10, 0);
        refreshIndex(indexName);

        List<Segment> segments = getSegmentsFromApi(indexName);
        SegmentsStats stats = getSegmentsStats(indexName, false);

        assertNotNull("SegmentsStats should not be null", stats);
        assertEquals("Segment count from stats should match segments API", segments.size(), stats.getCount());
    }

    /**
     * Tests that segmentsStats includes file sizes when requested.
     */
    public void testSegmentsStatsWithFileSizes() {
        String indexName = "test-segstats-filesizes";
        createCompositeIndex(indexName);
        indexDocs(indexName, 10, 0);
        refreshIndex(indexName);
        flushIndex(indexName);

        SegmentsStats stats = getSegmentsStats(indexName, true);
        assertNotNull("SegmentsStats should not be null", stats);

        Map<String, Long> fileSizes = stats.getFileSizes();
        assertNotNull("File sizes map should not be null", fileSizes);
        assertFalse("File sizes should not be empty when includeSegmentFileSizes=true", fileSizes.isEmpty());

        for (Map.Entry<String, Long> entry : fileSizes.entrySet()) {
            assertTrue("File size for '" + entry.getKey() + "' should be >= 0", entry.getValue() >= 0);
        }
    }

    /**
     * Tests that segmentsStats without file sizes does not populate the file sizes map.
     */
    public void testSegmentsStatsWithoutFileSizes() {
        String indexName = "test-segstats-no-filesizes";
        createCompositeIndex(indexName);
        indexDocs(indexName, 5, 0);
        refreshIndex(indexName);

        SegmentsStats stats = getSegmentsStats(indexName, false);
        assertNotNull("SegmentsStats should not be null", stats);
        assertTrue("Segment count should be > 0", stats.getCount() > 0);

        Map<String, Long> fileSizes = stats.getFileSizes();
        assertTrue("File sizes should be empty when includeSegmentFileSizes=false", fileSizes == null || fileSizes.isEmpty());
    }

    /**
     * Tests committed state transitions: new docs after flush are not committed until next flush.
     */
    public void testCommittedStateTransitionsAcrossFlushes() {
        String indexName = "test-seg-commit-transitions";
        createCompositeIndex(indexName);

        // Index + refresh + flush → committed
        indexDocs(indexName, 5, 0);
        refreshIndex(indexName);
        flushIndex(indexName);

        List<Segment> afterFirstFlush = getSegmentsFromApi(indexName);
        for (Segment seg : afterFirstFlush) {
            assertTrue("All segments should be committed after flush", seg.committed);
        }

        // Index more + refresh (no flush) → new snapshot not committed
        indexDocs(indexName, 5, 5);
        refreshIndex(indexName);

        List<Segment> afterSecondRefresh = getSegmentsFromApi(indexName);
        assertTrue("Should have segments", afterSecondRefresh.size() >= 1);
        // After a new refresh without flush, the current snapshot differs from committed snapshot
        // so all segments in the current snapshot should show committed=false
        for (Segment seg : afterSecondRefresh) {
            assertFalse("Segments should NOT be committed after refresh without flush", seg.committed);
            assertTrue("Segments should still be searchable", seg.search);
        }

        // Flush again → all committed
        flushIndex(indexName);
        List<Segment> afterSecondFlush = getSegmentsFromApi(indexName);
        for (Segment seg : afterSecondFlush) {
            assertTrue("All segments should be committed after second flush", seg.committed);
        }
    }

    // --- Helper methods ---

    private List<Segment> getSegmentsFromApi(String indexName) {
        IndicesSegmentResponse response = client().admin().indices().prepareSegments(indexName).get();
        IndexSegments indexSegments = response.getIndices().get(indexName);
        assertNotNull("Index segments should exist", indexSegments);
        ShardSegments[] shardSegments = indexSegments.getShards().values().iterator().next().getShards();
        assertTrue("Should have shard segments", shardSegments.length > 0);
        return shardSegments[0].getSegments();
    }

    private SegmentsStats getSegmentsStats(String indexName, boolean includeFileSizes) {
        IndicesStatsResponse statsResponse = client().admin()
            .indices()
            .prepareStats(indexName)
            .clear()
            .setSegments(true)
            .setIncludeSegmentFileSizes(includeFileSizes)
            .get();
        ShardStats shardStats = statsResponse.getIndex(indexName).getShards()[0];
        return shardStats.getStats().getSegments();
    }
}
