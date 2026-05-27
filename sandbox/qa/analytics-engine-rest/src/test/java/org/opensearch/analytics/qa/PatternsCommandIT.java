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

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Self-contained integration test for PPL {@code patterns} on the analytics-engine route.
 *
 * <p>Pins the two pieces of pattern wiring that are unique to this PR:
 * <ul>
 *   <li><b>SIMPLE patterns label mode</b> — regex-based tokenization runs in the DataFusion
 *       backend's {@code regexp_replace}. Requires the {@code RegexpReplaceAdapter} to append
 *       the {@code "g"} flag for 3-arg calls (DataFusion defaults to first-match-only).</li>
 *   <li><b>SIMPLE patterns aggregation mode with group-by on a multi-shard index</b> —
 *       exercises {@link org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule}'s
 *       SINGLE-on-SINGLETON fallback. The rule skips the PARTIAL/FINAL split when STATE_EXPANDING
 *       aggregates (e.g. {@code take()}) are present and gathers to the coordinator instead. The
 *       multi-shard variant is what actually drives that fallback path; the single-shard variant
 *       would never split anyway.</li>
 * </ul>
 *
 * <p>BRAIN method coverage stays on the SQL-side {@code CalcitePPLDashboardPatternsIT} for now —
 * BRAIN aggregation has open planner work (LogicalCorrelate post-aggregate) that's tracked
 * separately.
 */
public class PatternsCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("app_logs", "app_logs");
    private static final Dataset DATASET_MULTI = new Dataset("app_logs", "app_logs_patterns_multi");

    private static boolean dataProvisioned = false;
    private static boolean multiProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    /** Provision a 3-shard {@code app_logs} index so the patterns aggregation tests exercise
     *  the {@code OpenSearchAggregateSplitRule} SINGLE-on-SINGLETON fallback. */
    private void ensureMultiShardProvisioned() throws IOException {
        if (multiProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET_MULTI, 3);
            multiProvisioned = true;
            // Self-check: the QA integTest cluster runs with 2 nodes (per build.gradle), so a
            // 3-shard index distributes shards across nodes and gathering is genuinely
            // cross-node. Verify the shard count came through DatasetProvisioner override.
            Request settingsRequest = new Request("GET", "/" + DATASET_MULTI.indexName + "/_settings");
            Response settingsResponse = client().performRequest(settingsRequest);
            Map<String, Object> settings = assertOkAndParse(settingsResponse, "settings: " + DATASET_MULTI.indexName);
            String shardCount = extractShardCount(settings, DATASET_MULTI.indexName);
            assertEquals("Multi-shard index must report number_of_shards=3", "3", shardCount);
        }
    }

    @SuppressWarnings("unchecked")
    private static String extractShardCount(Map<String, Object> settings, String indexName) {
        Map<String, Object> indexSection = (Map<String, Object>) settings.get(indexName);
        assertNotNull("Settings response missing index entry", indexSection);
        Map<String, Object> settingsSection = (Map<String, Object>) indexSection.get("settings");
        Map<String, Object> indexSettings = (Map<String, Object>) settingsSection.get("index");
        return String.valueOf(indexSettings.get("number_of_shards"));
    }

    // ── SIMPLE patterns label mode (single-shard) ──────────────────────────────

    /**
     * {@code patterns <field> mode=label} appends a {@code patterns_field} column with the
     * tokenized form. Asserts every row got tokenized (no plain-letter messages survived) and
     * the document count is preserved — label mode doesn't aggregate.
     */
    public void testSimplePatternLabelMode() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName
                + " | patterns message method=simple_pattern mode=label"
                + " | fields patterns_field"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertEquals("label mode preserves row count", 200, rows.size());
        // Every value should be a string containing the wildcard token "<*>" — proving the
        // global regexp_replace ran on every match (no first-match-only artifacts).
        for (int i = 0; i < rows.size(); i++) {
            Object v = rows.get(i).get(0);
            assertNotNull("patterns_field at row " + i + " should not be null", v);
            assertTrue("patterns_field at row " + i + " should contain '<*>': " + v,
                v.toString().contains("<*>"));
        }
    }

    // ── SIMPLE patterns aggregation mode on a 3-shard index ────────────────────

    /**
     * {@code patterns ... mode=aggregation | stats count() as c by patterns_field} on a 3-shard
     * index. The {@code stats} contains {@code take()} (a STATE_EXPANDING aggregate registered
     * by the patterns lowering) so {@link
     * org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule#shouldSkipPartialFinalSplit}
     * skips the PARTIAL/FINAL split and falls back to SINGLE-on-SINGLETON — coordinator-side
     * aggregation after a gather. Asserts row counts sum to 200 (matches dataset total) and
     * every group has a non-empty {@code sample_logs} array.
     */
    public void testSimplePatternAggregationModeMultiShard() throws IOException {
        ensureMultiShardProvisioned();
        Map<String, Object> response = executePpl(
            "source=" + DATASET_MULTI.indexName
                + " | patterns message method=simple_pattern mode=aggregation"
                + " | fields patterns_field, pattern_count, sample_logs"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertTrue("Expected at least one cluster", rows.size() >= 1);
        long total = 0;
        for (int i = 0; i < rows.size(); i++) {
            List<Object> r = rows.get(i);
            assertEquals("Row " + i + " has 3 columns", 3, r.size());
            // patterns_field is a string with at least one <*> token.
            assertNotNull("patterns_field at row " + i, r.get(0));
            assertTrue("patterns_field at row " + i + " contains <*>", r.get(0).toString().contains("<*>"));
            // pattern_count is a positive number.
            assertTrue("pattern_count > 0 at row " + i, ((Number) r.get(1)).longValue() > 0);
            total += ((Number) r.get(1)).longValue();
            // sample_logs is a non-empty list (take(message, 10) returns up to 10 representatives).
            assertTrue("sample_logs is a List at row " + i, r.get(2) instanceof List);
            @SuppressWarnings("unchecked")
            List<Object> samples = (List<Object>) r.get(2);
            assertFalse("sample_logs non-empty at row " + i, samples.isEmpty());
        }
        assertEquals(
            "pattern_count totals must equal the dataset document count (200 docs distributed"
                + " across 3 shards)",
            200L,
            total
        );
    }

    /**
     * Same multi-shard pattern aggregation, but with an explicit user group key
     * ({@code by patterns_field, service_name}) — exercises the SINGLE-on-SINGLETON fallback
     * when group-set cardinality > 1.
     */
    public void testSimplePatternAggregationGroupByServiceMultiShard() throws IOException {
        ensureMultiShardProvisioned();
        Map<String, Object> response = executePpl(
            "source=" + DATASET_MULTI.indexName
                + " | patterns message method=simple_pattern mode=label"
                + " | stats count() as c, take(message, 1) as sample_logs"
                + "     by patterns_field, service_name"
                + " | fields patterns_field, service_name, c, sample_logs"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertTrue("Expected at least one (pattern, service) group", rows.size() >= 1);
        long total = 0;
        for (int i = 0; i < rows.size(); i++) {
            List<Object> r = rows.get(i);
            assertEquals("Row " + i + " has 4 columns", 4, r.size());
            // patterns_field has the wildcard token.
            assertTrue("patterns_field at row " + i + " contains <*>", r.get(0).toString().contains("<*>"));
            // service_name is one of the four expected services.
            String svc = (String) r.get(1);
            assertTrue("Unexpected service at row " + i + ": " + svc,
                svc.equals("api-service") || svc.equals("auth-service")
                    || svc.equals("db-service") || svc.equals("web-service"));
            assertTrue("group count > 0 at row " + i, ((Number) r.get(2)).longValue() > 0);
            total += ((Number) r.get(2)).longValue();
            @SuppressWarnings("unchecked")
            List<Object> sample = (List<Object>) r.get(3);
            assertEquals("take(message, 1) returns exactly 1 sample", 1, sample.size());
        }
        assertEquals(
            "group counts must sum to the dataset total (200 docs distributed across 3 shards)",
            200L,
            total
        );
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }
}
