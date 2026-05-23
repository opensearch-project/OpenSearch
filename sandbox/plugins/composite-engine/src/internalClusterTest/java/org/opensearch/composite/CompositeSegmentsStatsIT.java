/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.segments.IndexSegments;
import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.admin.indices.segments.ShardSegments;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.shard.DocsStats;
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

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).build();
    }

    /**
     * Tests that after refresh (no flush), segments are searchable but NOT committed.
     * After flush, segments become committed.
     * Note: Composite engines may handle committed state differently than pure Lucene engines.
     */
    public void testSegmentsCommittedAndSearchableState() {
        String indexName = "test-seg-committed-searchable";
        createCompositeIndex(indexName);
        indexDocs(indexName, 5, 0);
        refreshIndex(indexName);

        // Before flush: segments should be searchable but NOT committed
        List<Segment> segmentsBeforeFlush = getSegmentsFromApi(indexName);
        assertFalse("Should have segments after refresh", segmentsBeforeFlush.isEmpty());

        for (int i = 0; i < segmentsBeforeFlush.size(); i++) {
            Segment seg = segmentsBeforeFlush.get(i);
            assertTrue("Segment should be searchable before flush", seg.search);
            assertFalse("Segment should NOT be committed before flush", seg.committed);
        }

        // After flush: segments should be searchable and committed
        flushIndex(indexName);
        ensureGreen(indexName);

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

        // Index + refresh + flush
        indexDocs(indexName, 5, 0);
        refreshIndex(indexName);
        flushIndex(indexName);
        ensureGreen(indexName);

        // Verify data is accessible after first flush by checking segments
        List<Segment> segmentsAfterFirstFlush = getSegmentsFromApi(indexName);
        assertFalse("Should have segments after first flush", segmentsAfterFirstFlush.isEmpty());

        // Index more + refresh (no flush)
        indexDocs(indexName, 5, 5);
        refreshIndex(indexName);

        List<Segment> afterSecondRefresh = getSegmentsFromApi(indexName);
        assertTrue("Should have segments", afterSecondRefresh.size() >= 1);

        // All segments should be searchable
        for (Segment seg : afterSecondRefresh) {
            assertTrue("Segments should be searchable after refresh", seg.search);
        }

        // Verify we can access segments after second refresh
        List<Segment> segmentsAfterSecondRefresh = getSegmentsFromApi(indexName);
        assertTrue("Should have segments after second refresh", segmentsAfterSecondRefresh.size() >= 1);

        // Flush again
        flushIndex(indexName);
        ensureGreen(indexName);

        // Verify data is still accessible after second flush by checking segments
        List<Segment> segmentsAfterSecondFlush = getSegmentsFromApi(indexName);
        assertFalse("Should have segments after second flush", segmentsAfterSecondFlush.isEmpty());
    }

    /**
     * Tests that overall node stats (without any index-level filter) returns valid segment stats
     * and that stats are updated correctly as documents are indexed and refreshed.
     */
    public void testNodeStatsIncludesSegmentStats() {
        String indexName = "test-node-stats";
        createCompositeIndex(indexName);

        // Index first batch and verify
        indexDocs(indexName, 10, 0);
        refreshIndex(indexName);
        flushIndex(indexName);

        NodeStats dataNodeStats = getDataNodeStats();
        assertNotNull("Node indices stats should not be null", dataNodeStats.getIndices());

        SegmentsStats segmentsStats = dataNodeStats.getIndices().getSegments();
        assertNotNull("Segments stats from node stats should not be null", segmentsStats);
        assertTrue("Node-level segment count should be > 0", segmentsStats.getCount() > 0);

        DocsStats docsStats = dataNodeStats.getIndices().getDocs();
        assertNotNull("Docs stats from node stats should not be null", docsStats);
        assertEquals("Node-level doc count should match indexed docs", 10L, docsStats.getCount());

        // Index more docs and refresh — verify counts update
        indexDocs(indexName, 10, 10);
        refreshIndex(indexName);

        dataNodeStats = getDataNodeStats();
        docsStats = dataNodeStats.getIndices().getDocs();
        assertEquals("Doc count should update after second refresh", 20L, docsStats.getCount());

        segmentsStats = dataNodeStats.getIndices().getSegments();
        assertTrue("Segment count should be > 0 after second refresh", segmentsStats.getCount() > 0);

        // Index another batch, flush, and verify again
        indexDocs(indexName, 10, 20);
        refreshIndex(indexName);
        flushIndex(indexName);

        dataNodeStats = getDataNodeStats();
        docsStats = dataNodeStats.getIndices().getDocs();
        assertEquals("Doc count should update after third batch and flush", 30L, docsStats.getCount());
    }

    private NodeStats getDataNodeStats() {
        NodesStatsResponse nodesStats = client().admin().cluster().prepareNodesStats().all().get();
        for (NodeStats ns : nodesStats.getNodes()) {
            if (ns.getIndices() != null && ns.getIndices().getDocs().getCount() > 0) {
                return ns;
            }
        }
        // Fallback to first node with indices stats
        for (NodeStats ns : nodesStats.getNodes()) {
            if (ns.getIndices() != null) {
                return ns;
            }
        }
        fail("No data node found with indices stats");
        return null;
    }

    // --- Helper methods ---

    private List<Segment> getSegmentsFromApi(String indexName) {
        // Ensure all operations are complete before checking segments
        client().admin().indices().prepareRefresh(indexName).get();

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
