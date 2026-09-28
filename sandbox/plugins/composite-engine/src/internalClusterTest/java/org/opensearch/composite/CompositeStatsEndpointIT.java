/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.util.Map;

/**
 * Integration tests for the composite-engine per-format stats endpoint
 * ({@code /_plugins/composite/...}) and the parquet {@code native_write_rejections} counter.
 *
 * @opensearch.experimental
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1)
public class CompositeStatsEndpointIT extends BaseStatsIT {

    /** Fresh composite index: endpoint responds and all counters are zero. */
    public void testCompositeStatsZeroOnFreshIndex() throws Exception {
        String idx = "composite-zero-idx";
        createCompositeIndex(idx, true);

        Map<String, Object> c = compositeIndexStats(idx);
        assertCounter("fresh refresh_total", c, "indices." + idx + ".refresh.refresh_total", 0L);
        assertCounter("fresh refresh_merge_total", c, "indices." + idx + ".refresh.refresh_merge_total", 0L);
        assertCounter("fresh refresh_merge_failures", c, "indices." + idx + ".refresh.refresh_merge_failures", 0L);
        assertCounter("fresh merge_total", c, "indices." + idx + ".merge.merge_total", 0L);
        assertCounter("fresh merge_failures", c, "indices." + idx + ".merge.merge_failures", 0L);
        assertCounter("fresh write_total", c, "indices." + idx + ".write.write_total", 0L);
        assertCounter("fresh write_primary_failures", c, "indices." + idx + ".write.write_primary_failures", 0L);
        assertCounter("fresh write_secondary_failures", c, "indices." + idx + ".write.write_secondary_failures", 0L);
        assertCounter("fresh mapping_update_executed_total", c, "indices." + idx + ".mapping.mapping_update_executed_total", 0L);
    }

    /** After indexing + refresh, composite refresh counters increment and failures stay zero. */
    public void testCompositeRefreshAndMergeCountersIncrement() throws Exception {
        String idx = "composite-refresh-idx";
        createCompositeIndex(idx, true);
        indexDocs(idx, 100, 0);
        refreshIndex(idx);

        Map<String, Object> c = compositeIndexStats(idx);
        assertCounterAtLeast("refresh_total", c, "indices." + idx + ".refresh.refresh_total", 1L);
        assertCounterAtLeast("refresh_time_millis", c, "indices." + idx + ".refresh.refresh_time_millis", 0L);
        // write_total counts every indexed doc attempt; 100 docs were indexed.
        assertCounterAtLeast("write_total", c, "indices." + idx + ".write.write_total", 100L);
        // Happy path: no write or merge failures.
        assertCounter("write_primary_failures", c, "indices." + idx + ".write.write_primary_failures", 0L);
        assertCounter("write_secondary_failures", c, "indices." + idx + ".write.write_secondary_failures", 0L);
        assertCounter("merge_failures", c, "indices." + idx + ".merge.merge_failures", 0L);
        assertCounter("refresh_merge_failures", c, "indices." + idx + ".refresh.refresh_merge_failures", 0L);

        // refresh_merge_total is the merge-on-refresh subset of merge_total — it must never exceed it.
        long refreshMerge = StatsITHelpers.getCounter(c, "indices." + idx + ".refresh.refresh_merge_total");
        long mergeTotal = StatsITHelpers.getCounter(c, "indices." + idx + ".merge.merge_total");
        assertTrue(
            "refresh_merge_total (" + refreshMerge + ") must not exceed merge_total (" + mergeTotal + ")",
            refreshMerge <= mergeTotal
        );
    }

    /** The composite per-node endpoint aggregates and responds after indexing. */
    public void testCompositeNodeStatsEndpoint() throws Exception {
        String idx = "composite-node-idx";
        createCompositeIndex(idx, true);
        indexDocs(idx, 50, 0);
        refreshIndex(idx);

        Map<String, Object> n = compositeNodeStats("");
        // A successful per-node response carries a "nodes" object (one entry per responding node).
        assertTrue("per-node response must contain a 'nodes' object", n.get("nodes") instanceof Map);
        assertFalse("per-node response 'nodes' object must not be empty", ((Map<?, ?>) n.get("nodes")).isEmpty());
    }

    /** Parquet native_write_rejections is wired and reads zero on the happy path. */
    public void testParquetNativeWriteRejectionsZeroOnHappyPath() throws Exception {
        String idx = "parquet-rejection-idx";
        createCompositeIndex(idx, true);
        indexDocs(idx, 100, 0);
        refreshIndex(idx);

        Map<String, Object> p = parquetIndexStats(idx);
        assertCounter("native_write_rejections zero", p, "indices." + idx + ".native_write.native_write_rejections", 0L);
    }

    /** The parquet per-node endpoint exposes the live native_ingest_pool block (queue/active/rejected). */
    public void testParquetNodeStatsExposesIngestPool() throws Exception {
        String idx = "parquet-ingest-pool-idx";
        createCompositeIndex(idx, true);
        indexDocs(idx, 50, 0);
        refreshIndex(idx);

        Map<String, Object> n = parquetNodeStats("");
        // Each responding node must carry a native_ingest_pool block with the pool fields.
        Map<?, ?> nodes = (Map<?, ?>) n.get("nodes");
        assertNotNull("per-node response must contain 'nodes'", nodes);
        assertFalse("'nodes' must not be empty", nodes.isEmpty());
        boolean sawPool = false;
        for (Object node : nodes.values()) {
            Object pool = ((Map<?, ?>) node).get("native_ingest_pool");
            if (pool instanceof Map) {
                assertTrue("native_ingest_pool must report queue_depth", ((Map<?, ?>) pool).containsKey("queue_depth"));
                assertTrue("native_ingest_pool must report rejected", ((Map<?, ?>) pool).containsKey("rejected"));
                sawPool = true;
            }
        }
        assertTrue("at least one node must expose native_ingest_pool", sawPool);
    }
}
