/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Integration test for {@code !=} / {@code NOT field=value} predicates on keyword columns that
 * are pushed to the lucene-secondary index via filter delegation.
 *
 * <p>The PPL frontend desugars {@code field != "v"} (and {@code NOT field = "v"}) into a
 * {@code query_string} containing an {@code _exists_:field} clause. {@code _exists_} compiles to a
 * Lucene {@code FieldExistsQuery}, which requires the field to index doc_values / norms / vectors.
 * On a composite parquet-primary index the keyword doc_values live in the parquet primary, NOT the
 * lucene-secondary segment that filter delegation queries — so {@code FieldExistsQuery.rewrite()}
 * throws ("indexes neither doc values, norms nor vectors") and the whole query 500s.
 *
 * <p>{@code LuceneQueryConversionUtils.rewriteFieldExistsForSecondary} fixes this by rewriting the
 * {@code FieldExistsQuery} into a postings-only {@code TermRangeQuery} before execution. These tests
 * assert the queries now return the correct counts.
 *
 * <p>Dataset {@code app_logs_filter_delegation}: keyword {@code log_level} with ERROR=115, INFO=47,
 * WARN=38 (200 docs total). All filtering routes through the delegation path.
 */
public class SearchFieldExistsDelegationIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("app_logs_filter_delegation", "app_logs_filter_delegation");

    private static boolean dataProvisioned = false;

    private static final int TOTAL = 200;
    private static final int ERROR = 115;
    private static final int INFO = 47;
    private static final int WARN = 38;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            // 2 shards so the predicate is delegated + reduced across shards (the failing topology).
            DatasetProvisioner.provision(client(), DATASET, 2);
            dataProvisioned = true;
        }
    }

    // The bug reproducer is the PPL `search` command's {@code field != value} form. Per the SQL
    // plugin's own SearchCommandIT: `!=` means "field EXISTS and is not equal", so the frontend
    // emits a query_string with an `_exists_:field` clause → Lucene FieldExistsQuery → 500 on the
    // doc-values-less lucene-secondary without rewriteFieldExistsForSecondary. (By contrast
    // `NOT field=value` and `OR` do NOT emit `_exists_`, so they don't hit the bug — they're kept
    // below as control cases that must pass with the fix on or off.)

    /** REPRODUCER: {@code search log_level!="INFO"} → exists-and-not-INFO = ERROR + WARN. 500 before fix. */
    public void testSearchNotEqualsKeyword() throws IOException {
        String src = "search source=" + DATASET.indexName + " log_level!=\"INFO\"";
        // Count check.
        assertCount(src + " | stats count() as c", ERROR + WARN);
        // Value check: the surviving log_levels are exactly {ERROR, WARN} with the right per-bucket
        // counts and — crucially — INFO is absent. Guards against a wrong-rows-same-total pass.
        assertCountsByLogLevel(src, Map.of("ERROR", (long) ERROR, "WARN", (long) WARN));
    }

    /** REPRODUCER: {@code !=} on a different keyword column → not field-specific. 500 before fix. */
    public void testSearchNotEqualsOtherKeyword() throws IOException {
        String src = "search source=" + DATASET.indexName + " service_name!=\"__nope__\"";
        // service_name exists on every doc and is never "__nope__" → all 200 match, every level present.
        assertCount(src + " | stats count() as c", TOTAL);
        assertCountsByLogLevel(src, Map.of("ERROR", (long) ERROR, "INFO", (long) INFO, "WARN", (long) WARN));
    }

    /** CONTROL: {@code NOT log_level="INFO"} excludes INFO without an _exists_ clause (no bug path). */
    public void testSearchNotFieldEqualsKeyword() throws IOException {
        assertCount("search source=" + DATASET.indexName + " NOT log_level=\"INFO\" | stats count() as c", ERROR + WARN);
    }

    /** CONTROL: Boolean OR over keyword equality — two terms, no _exists_. */
    public void testSearchOrEqualsKeyword() throws IOException {
        assertCount(
            "search source=" + DATASET.indexName + " log_level=\"ERROR\" OR log_level=\"FATAL\" | stats count() as c",
            ERROR
        );
    }

    /** CONTROL: plain equality via search (no _exists_) still matches the known count. */
    public void testSearchEqualsKeywordBaseline() throws IOException {
        assertCount("search source=" + DATASET.indexName + " log_level=\"INFO\" | stats count() as c", INFO);
    }

    /**
     * REPRODUCER (non-count / value-returning): {@code search field!=v | fields ...} returns actual
     * rows rather than a stats count, so it exercises the Lucene-driver SCAN path
     * (LuceneSearchExecEngine over the filter query) — a different code path from the stats-count
     * fast-path and from filter-delegation. Without the rewrite this 500s too. We assert the
     * surviving log_level distribution (no INFO) by grouping, proving real rows came back.
     */
    public void testSearchNotEqualsReturnsRows() throws IOException {
        String src = "search source=" + DATASET.indexName + " log_level!=\"INFO\"";
        // fields projection (no stats) → value-returning scan; head bounds the payload.
        Map<String, Object> response = executePpl(src + " | fields log_level | head 5");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows'", rows);
        assertEquals("expected 5 rows", 5, rows.size());
        for (List<Object> row : rows) {
            assertNotEquals("INFO must be excluded by !=", "INFO", row.get(0));
        }
        // And the full grouped distribution over the value-returning scan excludes INFO entirely.
        assertCountsByLogLevel(src, Map.of("ERROR", (long) ERROR, "WARN", (long) WARN));
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private void assertCount(String ppl, long expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows' for query: " + ppl, rows);
        assertEquals("Expected a single count row for query: " + ppl, 1, rows.size());
        long actual = ((Number) rows.get(0).get(0)).longValue();
        assertEquals("Count mismatch for query: " + ppl, expected, actual);
    }

    /**
     * Runs {@code <searchPrefix> | stats count() as c by log_level} and asserts the returned
     * level→count map equals {@code expected} exactly — same keys, same counts, no extras. This
     * verifies the actual surviving rows (e.g. that INFO is truly excluded), not just a total.
     */
    private void assertCountsByLogLevel(String searchPrefix, Map<String, Long> expected) throws IOException {
        Map<String, Object> response = executePpl(searchPrefix + " | stats count() as c by log_level");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows'", rows);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> schema = (List<Map<String, Object>>) response.get("schema");
        // Locate the count vs log_level columns by name (column order is not guaranteed).
        int countCol = -1;
        int levelCol = -1;
        for (int i = 0; i < schema.size(); i++) {
            String name = String.valueOf(schema.get(i).get("name"));
            if ("c".equals(name)) {
                countCol = i;
            } else if ("log_level".equals(name)) {
                levelCol = i;
            }
        }
        assertTrue("schema missing 'c'/'log_level' columns: " + schema, countCol >= 0 && levelCol >= 0);
        Map<String, Long> actual = new HashMap<>();
        for (List<Object> row : rows) {
            actual.put(String.valueOf(row.get(levelCol)), ((Number) row.get(countCol)).longValue());
        }
        assertEquals("log_level → count map mismatch", expected, actual);
    }
}
