/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.util.List;
import java.util.Map;

/**
 * IT for per-shard bucket oversampling using ClickBench dataset.
 * Exercises real TopK query shapes (stats+sort+head) on multi-shard index.
 */
public class ShardBucketOversamplingIT extends AnalyticsRestTestCase {

    private static volatile boolean provisioned = false;
    private static final String INDEX = "parquet_hits";

    private void ensureProvisioned() throws Exception {
        if (!provisioned) {
            DatasetProvisioner.provision(client(), ClickBenchTestHelper.DATASET, 2);
            Request req = new Request("PUT", "/_cluster/settings");
            req.setJsonEntity("{\"persistent\":{\"analytics.shard_bucket_oversampling_factor\": 2.0}}");
            client().performRequest(req);
            provisioned = true;
        }
    }

    /** Q13 shape: count by keyword, sort desc, head 10. */
    public void testCountByGroup_sortDesc_head10() throws Exception {
        ensureProvisioned();
        Map<String, Object> result = executePPL(
            "source = " + INDEX + " | stats count() as c by RegionID | sort - c | head 10"
        );
        assertRowCount(result, 10);
    }

    /** Q10 shape: sum + count + avg + dc by group, sort, head. */
    public void testMultiAgg_sortByCount_head10() throws Exception {
        ensureProvisioned();
        Map<String, Object> result = executePPL(
            "source = " + INDEX + " | stats sum(AdvEngineID), count() as c, avg(ResolutionWidth) by RegionID | sort - c | head 10"
        );
        assertRowCount(result, 10);
    }

    /** Sum by group, sort desc. */
    public void testSumByGroup_sortDesc_head10() throws Exception {
        ensureProvisioned();
        Map<String, Object> result = executePPL(
            "source = " + INDEX + " | stats sum(ResolutionWidth) as s by RegionID | sort - s | head 10"
        );
        assertRowCount(result, 10);
    }

    /** Avg by group, sort desc. */
    public void testAvgByGroup_sortDesc_head10() throws Exception {
        ensureProvisioned();
        Map<String, Object> result = executePPL(
            "source = " + INDEX + " | stats avg(ResolutionWidth) as a by RegionID | sort - a | head 10"
        );
        assertRowCount(result, 10);
    }

    /** Min/Max by group. */
    public void testMinMaxByGroup() throws Exception {
        ensureProvisioned();
        Map<String, Object> result = executePPL(
            "source = " + INDEX + " | stats min(ResolutionWidth) as mi, max(ResolutionWidth) as ma by RegionID | sort - ma | head 10"
        );
        assertRowCount(result, 10);
    }

    /** dc by group, sort desc. */
    public void testDcByGroup_sortDesc_head10() throws Exception {
        ensureProvisioned();
        Map<String, Object> result = executePPL(
            "source = " + INDEX + " | stats dc(UserID) as u by RegionID | sort - u | head 10"
        );
        assertRowCount(result, 10);
    }

    /** Mixed aggregation with TopK: dc + sum, sorted by dc. */
    public void testMultiAgg_sortByDc_head10() throws Exception {
        ensureProvisioned();
        Map<String, Object> result = executePPL(
            "source = " + INDEX + " | stats dc(UserID) as u, sum(AdvEngineID) as s by RegionID | sort - u | head 10"
        );
        List<List<Object>> rows = rowsOf(result);
        assertFalse("Should return rows", rows.isEmpty());
        assertTrue("Should return at most 10 rows", rows.size() <= 10);
        // Verify all expected columns present (row is a positional list; schema has 3 columns: u, s, RegionID)
        for (List<Object> row : rows) {
            assertEquals("each row should have 3 columns [u, s, RegionID]", 3, row.size());
        }
    }

    // ── Multi-shard TopK across sort placements / group-by / having ─────────────────────
    // 84 distinct RegionID groups over 100 docs on 2 shards, so `head 10` (10 << 84) genuinely
    // exercises the per-shard TopK oversampling path (a per-partition Sort+Limit below the ER).
    //
    // These assert only the DETERMINISTIC contract — row count, schema, no error — across the
    // shape variations. They intentionally do NOT assert exact group membership/values: shard-bucket
    // oversampling is approximate by design (see AnalyticsApproximationSettings); a globally-top-N
    // group that ranks below every shard's local cutoff can legitimately be dropped, and tied
    // sort-keys have no defined tie-break, so value-equality vs. an unsampled run is not a guaranteed
    // property and would be flaky. The exactness of the fix (per-shard fetch sized off the inner
    // limit, not the outer system cap) is pinned deterministically in TopKRewriterPlanShapeTests.

    /** count by group, sort DESC on the agg, head 10. */
    public void testCountByGroup_sortDescAgg_head10() throws Exception {
        ensureProvisioned();
        assertCountByQueryReturns("source = " + INDEX + " | stats count() as c by RegionID | sort - c | head 10", 10);
    }

    /** sort ASC on the agg, head 10 — opposite collation direction through the per-shard sort. */
    public void testCountByGroup_sortAscAgg_head10() throws Exception {
        ensureProvisioned();
        assertCountByQueryReturns("source = " + INDEX + " | stats count() as c by RegionID | sort c | head 10", 10);
    }

    /** sort on the GROUP KEY (not the agg), head 10 — sort field is the by-column. */
    public void testCountByGroup_sortOnGroupKey_head10() throws Exception {
        ensureProvisioned();
        assertCountByQueryReturns("source = " + INDEX + " | stats count() as c by RegionID | sort - RegionID | head 10", 10);
    }

    /** HAVING (post-stats where) between the agg and the limit, then sort + head. */
    public void testCountByGroup_having_sortDesc_head10() throws Exception {
        ensureProvisioned();
        // `where c > 1` is PPL's HAVING: filters groups after aggregation, before sort/limit.
        // 14 RegionID groups have count > 1 (12 at 2 + 2 at 3), so head 10 still bounds the result.
        assertCountByQueryReturns(
            "source = " + INDEX + " | stats count() as c by RegionID | where c > 1 | sort - c | head 10",
            10
        );
    }

    /** No head: grouped + sorted, every surviving group returned (redundant outer system limit
     *  must not trim, and TopK must not fire when there is no user limit). */
    public void testCountByGroup_sortDesc_noHead_returnsAllGroups() throws Exception {
        ensureProvisioned();
        // 84 distinct RegionID groups in the fixture.
        assertCountByQueryReturns("source = " + INDEX + " | stats count() as c by RegionID | sort - c", 84);
    }

    /** Asserts the query returns exactly {@code expectedRows} rows with the expected [c, RegionID]
     *  schema — the deterministic contract that holds regardless of oversampling factor. */
    private void assertCountByQueryReturns(String ppl, int expectedRows) throws Exception {
        Map<String, Object> result = executePPL(ppl);
        List<List<Object>> rows = rowsOf(result);
        assertEquals("row count", expectedRows, rows.size());
        for (List<Object> row : rows) {
            assertEquals("each row is [count, RegionID]", 2, row.size());
            assertTrue("count column must be numeric, got " + row.get(0), row.get(0) instanceof Number);
        }
    }

    /** No oversampling (factor=0): query still works. */
    public void testFactorZero_queryWorks() throws Exception {
        ensureProvisioned();
        // Temporarily set factor to 0
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"persistent\":{\"analytics.shard_bucket_oversampling_factor\": 0.0}}");
        client().performRequest(req);

        Map<String, Object> result = executePPL(
            "source = " + INDEX + " | stats count() as c by RegionID | sort - c | head 10"
        );
        assertRowCount(result, 10);

        // Restore
        Request restore = new Request("PUT", "/_cluster/settings");
        restore.setJsonEntity("{\"persistent\":{\"analytics.shard_bucket_oversampling_factor\": 2.0}}");
        client().performRequest(restore);
    }

    @SuppressWarnings("unchecked")
    private void assertRowCount(Map<String, Object> result, int expected) {
        List<?> rows = (List<?>) result.get("rows");
        assertNotNull("response must have rows, got: " + result.keySet(), rows);
        assertEquals(expected, rows.size());
    }

    @SuppressWarnings("unchecked")
    private static List<List<Object>> rowsOf(Map<String, Object> result) {
        List<?> rows = (List<?>) result.get("rows");
        assertNotNull("response must have rows, got: " + result.keySet(), rows);
        return (List<List<Object>>) rows;
    }

    private void setOversamplingFactor(double factor) throws Exception {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"persistent\":{\"analytics.shard_bucket_oversampling_factor\": " + factor + "}}");
        client().performRequest(req);
    }

    private Map<String, Object> executePPL(String ppl) throws Exception {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + ppl + "\"}");
        Response response = client().performRequest(request);
        return entityAsMap(response);
    }
}
