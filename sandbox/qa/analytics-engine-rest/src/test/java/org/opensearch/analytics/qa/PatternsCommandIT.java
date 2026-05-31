/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Integration tests for PPL {@code patterns} on the analytics-engine route. */
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

    /** Provision a 3-shard {@code app_logs} index to drive the SINGLE-on-SINGLETON fallback. */
    private void ensureMultiShardProvisioned() throws IOException {
        if (multiProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET_MULTI, 3);
            multiProvisioned = true;
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

    public void testSimplePatternLabelMode() throws IOException {
        Map<String, Object> response = executePplViaShim(
            "source=" + DATASET.indexName
                + " | patterns message method=simple_pattern mode=label"
                + " | fields patterns_field"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertEquals("label mode preserves row count", 200, rows.size());
        for (int i = 0; i < rows.size(); i++) {
            Object v = rows.get(i).get(0);
            assertNotNull("patterns_field at row " + i + " should not be null", v);
            assertTrue("patterns_field at row " + i + " should contain '<*>': " + v,
                v.toString().contains("<*>"));
        }
    }

    @AwaitsFix(bugUrl = "patterns mode=aggregation auto-generates take(message, 10) but the literal 10 resolves to UNDEFINED in the PPL type checker: 'Aggregation function TAKE expects {[ANY]|[ANY,INTEGER]}, but got [STRING,UNDEFINED]'. Frontend type-resolution bug in the patterns-aggregation lowering (unified-query / CalciteRelNodeVisitor.visitPatterns), surfaced by the upstream BRAIN/SIMPLE patterns merge. Explicit take(message,1) works (see sibling testSimplePatternAggregationGroupByServiceMultiShard); only the auto-generated N is mistyped. Needs an opensearch-sql fix, out of scope for analytics-engine.")
    public void testSimplePatternAggregationModeMultiShard() throws IOException {
        ensureMultiShardProvisioned();
        Map<String, Object> response = executePplViaShim(
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
            assertNotNull("patterns_field at row " + i, r.get(0));
            assertTrue("patterns_field at row " + i + " contains <*>", r.get(0).toString().contains("<*>"));
            assertTrue("pattern_count > 0 at row " + i, ((Number) r.get(1)).longValue() > 0);
            total += ((Number) r.get(1)).longValue();
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

    public void testSimplePatternAggregationGroupByServiceMultiShard() throws IOException {
        ensureMultiShardProvisioned();
        Map<String, Object> response = executePplViaShim(
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
            assertTrue("patterns_field at row " + i + " contains <*>", r.get(0).toString().contains("<*>"));
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

    @Override
    protected void onBeforeQuery() throws IOException {
        ensureDataProvisioned();
    }
}
