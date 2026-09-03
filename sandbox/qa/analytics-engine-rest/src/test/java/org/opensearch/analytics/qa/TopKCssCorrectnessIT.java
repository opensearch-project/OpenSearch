/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.util.List;
import java.util.Map;

/**
 * Regression tests for TopK correctness when concurrent segment search (CSS) is active.
 *
 * <p>Before the PartialReduce fix, CSS caused each intra-shard partition to independently
 * truncate to the TopK fetch limit before the coordinator merge, producing wrong counts.
 * Each test runs the same query with CSS off (reference) and CSS on (subject) and asserts
 * the results are identical.
 *
 * <p>Covers 13 aggregate shapes identified by Aniketh Jain across count, sum, avg, min/max,
 * distinct_count, stddev/variance, percentile, offset, scalar agg, and permutation variants.
 */
@SuppressWarnings("unchecked")
public class TopKCssCorrectnessIT extends AnalyticsRestTestCase {

    private static volatile boolean provisioned = false;
    private static final String INDEX = "parquet_hits";

    private void ensureProvisioned() throws Exception {
        if (!provisioned) {
            // MULTI_SEGMENT (2 segments/shard) + low oversampling makes the CSS truncation
            // bug reproducible on the local test cluster — each CSS partition independently
            // truncates to a very small fetch limit, producing wrong results without the fix.
            DatasetProvisioner.provision(client(), ClickBenchTestHelper.DATASET, 2, DatasetProvisioner.SegmentLayout.MULTI_SEGMENT);
            Request req = new Request("PUT", "/_cluster/settings");
            req.setJsonEntity(
                "{\"persistent\":{\"analytics.shard_bucket_oversampling_factor\": 0.1}}"
            );
            client().performRequest(req);
            provisioned = true;
        }
    }

    // ── case-01: multi-key, count/sum/avg, != filter ──────────────────────────

    public void testCase01_multiKeyCountSumAvg_cssMatchesNoCss() throws Exception {
        ensureProvisioned();
        assertCssMatchesNoCss(
            "source = " + INDEX
                + " | where SearchPhrase != ''"
                + " | stats count() as c, sum(IsRefresh), avg(ResolutionWidth)"
                + " by SearchEngineID, ClientIP"
                + " | sort - c, SearchEngineID, ClientIP | head 10"
        );
    }

    // ── case-02: single-key count ────────────────────────────────────────────

    public void testCase02_singleKeyCount_cssMatchesNoCss() throws Exception {
        ensureProvisioned();
        assertCssMatchesNoCss(
            "source = " + INDEX
                + " | stats count() as c by SearchEngineID"
                + " | sort - c, SearchEngineID | head 2"
        );
    }

    // ── case-03: distinct_count (HLL) ────────────────────────────────────────

    public void testCase03_distinctCount_cssMatchesNoCss() throws Exception {
        ensureProvisioned();
        assertCssMatchesNoCss(
            "source = " + INDEX
                + " | stats distinct_count(ClientIP) as dc by SearchEngineID"
                + " | sort - dc, SearchEngineID | head 2"
        );
    }

    // ── case-04: stddev / variance ───────────────────────────────────────────

    public void testCase04_stddevVariance_cssMatchesNoCss() throws Exception {
        ensureProvisioned();
        assertCssMatchesNoCss(
            "source = " + INDEX
                + " | stats stddev_samp(ResolutionWidth) as sd,"
                + " var_samp(ResolutionWidth) as vs,"
                + " var_pop(ResolutionWidth) as vp"
                + " by SearchEngineID | sort SearchEngineID | head 10"
        );
    }

    // ── case-05: scalar aggregate (no group-by, no TopK) ─────────────────────

    public void testCase05_scalarSums_cssMatchesNoCss() throws Exception {
        ensureProvisioned();
        assertCssMatchesNoCss(
            "source = " + INDEX
                + " | stats sum(ResolutionWidth),"
                + " sum(ResolutionWidth+1),"
                + " sum(ResolutionWidth+2),"
                + " count()"
        );
    }

    // ── case-06: offset + limit ───────────────────────────────────────────────

    public void testCase06_offsetLimit_cssMatchesNoCss() throws Exception {
        ensureProvisioned();
        assertCssMatchesNoCss(
            "source = " + INDEX
                + " | stats count() as c by SearchEngineID"
                + " | sort - c, SearchEngineID | head 2 from 1"
        );
    }

    // ── case-07: min / max ────────────────────────────────────────────────────

    public void testCase07_minMax_cssMatchesNoCss() throws Exception {
        ensureProvisioned();
        assertCssMatchesNoCss(
            "source = " + INDEX
                + " | stats min(ResolutionWidth) as mn,"
                + " max(ResolutionWidth) as mx,"
                + " count() as c by SearchEngineID"
                + " | sort - c, SearchEngineID | head 2"
        );
    }

    // ── case-08: avg + sum ────────────────────────────────────────────────────

    public void testCase08_avgSum_cssMatchesNoCss() throws Exception {
        ensureProvisioned();
        // Sort by SearchEngineID (deterministic key, not count) to avoid tie-breaking flakiness.
        assertCssMatchesNoCss(
            "source = " + INDEX
                + " | stats avg(ResolutionWidth) as a,"
                + " sum(ResolutionWidth) as s,"
                + " count() as c by SearchEngineID"
                + " | sort SearchEngineID | head 5"
        );
    }

    // ── case-09a: agg permutation (count, sum, avg, min, max) ────────────────

    public void testCase09a_permutation1_cssMatchesNoCss() throws Exception {
        ensureProvisioned();
        assertCssMatchesNoCss(
            "source = " + INDEX
                + " | stats count() as c,"
                + " sum(IsRefresh) as si,"
                + " avg(ResolutionWidth) as a,"
                + " min(ResolutionWidth) as mn,"
                + " max(ResolutionWidth) as mx by SearchEngineID"
                + " | sort - c, SearchEngineID | head 2"
        );
    }

    // ── case-09b: agg permutation (max, avg, count, min, sum) ────────────────

    public void testCase09b_permutation2_cssMatchesNoCss() throws Exception {
        ensureProvisioned();
        assertCssMatchesNoCss(
            "source = " + INDEX
                + " | stats max(ResolutionWidth) as mx,"
                + " avg(ResolutionWidth) as a,"
                + " count() as c,"
                + " min(ResolutionWidth) as mn,"
                + " sum(IsRefresh) as si by SearchEngineID"
                + " | sort - c, SearchEngineID | head 2"
        );
    }

    // ── case-09c: agg permutation (avg, min, sum, max, count) ────────────────

    public void testCase09c_permutation3_cssMatchesNoCss() throws Exception {
        ensureProvisioned();
        assertCssMatchesNoCss(
            "source = " + INDEX
                + " | stats avg(ResolutionWidth) as a,"
                + " min(ResolutionWidth) as mn,"
                + " sum(IsRefresh) as si,"
                + " max(ResolutionWidth) as mx,"
                + " count() as c by SearchEngineID"
                + " | sort - c, SearchEngineID | head 2"
        );
    }

    // ── case-10: no aliases ───────────────────────────────────────────────────

    public void testCase10_noAliases_cssMatchesNoCss() throws Exception {
        ensureProvisioned();
        assertCssMatchesNoCss(
            "source = " + INDEX
                + " | stats count(), sum(ResolutionWidth),"
                + " avg(ResolutionWidth),"
                + " min(ResolutionWidth),"
                + " max(ResolutionWidth) by SearchEngineID"
                + " | sort SearchEngineID | head 5"
        );
    }

    // ── case-11: many aggs on same column ────────────────────────────────────

    public void testCase11_manyAggsOnSameColumn_cssMatchesNoCss() throws Exception {
        ensureProvisioned();
        assertCssMatchesNoCss(
            "source = " + INDEX
                + " | stats sum(ResolutionWidth),"
                + " avg(ResolutionWidth),"
                + " min(ResolutionWidth),"
                + " max(ResolutionWidth),"
                + " count(ResolutionWidth) by SearchEngineID"
                + " | sort SearchEngineID | head 5"
        );
    }

    // ── case-12: percentile ───────────────────────────────────────────────────

    public void testCase12_percentile_cssMatchesNoCss() throws Exception {
        ensureProvisioned();
        assertCssMatchesNoCss(
            "source = " + INDEX
                + " | stats percentile(ResolutionWidth, 50) as p50,"
                + " percentile(ResolutionWidth, 95) as p95 by SearchEngineID"
                + " | sort SearchEngineID | head 5"
        );
    }

    // ── case-13: mixed split + non-split (count/sum + percentile) ────────────

    public void testCase13_mixedSplitAndNonSplit_cssMatchesNoCss() throws Exception {
        ensureProvisioned();
        assertCssMatchesNoCss(
            "source = " + INDEX
                + " | stats count() as c,"
                + " sum(ResolutionWidth) as s,"
                + " percentile(ResolutionWidth, 50) as p50 by SearchEngineID"
                + " | sort - c, SearchEngineID | head 2"
        );
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Runs {@code ppl} with CSS off, then with CSS on (4 slices), and asserts the
     * result rows are identical. Restores CSS-off after the check.
     */
    private void assertCssMatchesNoCss(String ppl) throws Exception {
        setCss("none", 0);
        List<List<Object>> reference = rowsOf(executePPL(ppl));

        setCss("all", 4);
        List<List<Object>> withCss = rowsOf(executePPL(ppl));

        assertEquals(
            "CSS result differs from no-CSS reference for query: " + ppl,
            reference,
            withCss
        );

        setCss("none", 0);
    }

    private void setCss(String mode, int sliceCount) throws Exception {
        Request req = new Request("PUT", "/_cluster/settings");
        if (sliceCount > 0) {
            req.setJsonEntity(
                "{\"transient\":{\"search.concurrent_segment_search.mode\":\""
                    + mode
                    + "\",\"search.concurrent.max_slice_count\":"
                    + sliceCount
                    + "}}"
            );
        } else {
            req.setJsonEntity(
                "{\"transient\":{\"search.concurrent_segment_search.mode\":\"" + mode + "\"}}"
            );
        }
        client().performRequest(req);
    }

    private Map<String, Object> executePPL(String ppl) throws Exception {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + ppl + "\"}");
        Response response = client().performRequest(request);
        return entityAsMap(response);
    }

    private List<List<Object>> rowsOf(Map<String, Object> result) {
        List<?> rows = (List<?>) result.get("rows");
        assertNotNull("response must have rows, got: " + result.keySet(), rows);
        return (List<List<Object>>) rows;
    }
}
