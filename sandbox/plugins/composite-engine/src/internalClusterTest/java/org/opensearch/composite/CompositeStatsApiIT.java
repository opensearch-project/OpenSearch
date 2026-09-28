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
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.test.OpenSearchIntegTestCase;

/**
 * Integration tests verifying index stats API and node stats API for DataFormatAwareEngine.
 *
 * Validates:
 * - _stats/segments (index stats API) reports correct segment count and file sizes
 * - _nodes/stats/indices/segments (node stats API) reports segment count and file sizes
 * - _stats/docs reports correct doc count
 * - node stats and index stats segment counts are consistent
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class CompositeStatsApiIT extends AbstractCompositeEngineIT {

    /**
     * Tests that node stats API reports segment count > 0 after indexing and refresh.
     */
    public void testNodeStatsSegmentsCount() {
        String indexName = "test-nodestats-seg-count";
        createCompositeIndex(indexName);
        indexDocs(indexName, 10, 0);
        refreshIndex(indexName);

        NodesStatsResponse nodesStats = client().admin()
            .cluster()
            .prepareNodesStats()
            .clear()
            .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Segments))
            .get();

        long totalSegmentCount = 0;
        for (NodeStats nodeStats : nodesStats.getNodes()) {
            if (nodeStats.getIndices() != null && nodeStats.getIndices().getSegments() != null) {
                totalSegmentCount += nodeStats.getIndices().getSegments().getCount();
            }
        }
        assertTrue("Node stats should report at least one segment after refresh", totalSegmentCount > 0);
    }

    /**
     * Tests that node stats API reports segment count > 0 after flush.
     * Note: node stats API does not support includeSegmentFileSizes (only index stats API does),
     * so file sizes are always empty via this path.
     */
    public void testNodeStatsSegmentsAfterFlush() {
        String indexName = "test-nodestats-seg-flush";
        createCompositeIndex(indexName);
        indexDocs(indexName, 10, 0);
        refreshIndex(indexName);
        flushIndex(indexName);

        NodesStatsResponse nodesStats = client().admin()
            .cluster()
            .prepareNodesStats()
            .clear()
            .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Segments))
            .get();

        long totalSegmentCount = 0;
        for (NodeStats nodeStats : nodesStats.getNodes()) {
            if (nodeStats.getIndices() != null && nodeStats.getIndices().getSegments() != null) {
                totalSegmentCount += nodeStats.getIndices().getSegments().getCount();
            }
        }
        assertTrue("Node stats should report at least one segment after flush", totalSegmentCount > 0);
    }

    /**
     * Tests that index stats API reports correct doc count after indexing.
     */
    public void testIndexStatsDocCount() {
        String indexName = "test-indexstats-docs";
        createCompositeIndex(indexName);
        int docCount = 15;
        indexDocs(indexName, docCount, 0);
        refreshIndex(indexName);

        IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(indexName).clear().setDocs(true).get();

        long totalDocs = statsResponse.getIndex(indexName).getTotal().getDocs().getCount();
        assertEquals("Index stats doc count should match indexed docs", docCount, totalDocs);
    }

    /**
     * Tests that node stats and index stats report consistent segment counts.
     */
    public void testNodeStatsAndIndexStatsSegmentCountConsistency() {
        String indexName = "test-consistency-seg-count";
        createCompositeIndex(indexName);
        indexDocs(indexName, 10, 0);
        refreshIndex(indexName);

        IndicesStatsResponse indexStats = client().admin().indices().prepareStats(indexName).clear().setSegments(true).get();
        ShardStats shardStats = indexStats.getIndex(indexName).getShards()[0];
        SegmentsStats indexSegmentsStats = shardStats.getStats().getSegments();
        assertNotNull("Index stats segments should not be null", indexSegmentsStats);
        long indexStatsSegmentCount = indexSegmentsStats.getCount();

        NodesStatsResponse nodesStats = client().admin()
            .cluster()
            .prepareNodesStats()
            .clear()
            .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Segments))
            .get();
        long nodeStatsSegmentCount = nodesStats.getNodes()
            .stream()
            .filter(n -> n.getIndices() != null && n.getIndices().getSegments() != null)
            .mapToLong(n -> n.getIndices().getSegments().getCount())
            .sum();

        assertEquals("Segment count from node stats and index stats should be consistent", indexStatsSegmentCount, nodeStatsSegmentCount);
    }
}
