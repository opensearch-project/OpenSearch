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
 * Regression test for parquet row-group / page statistics pruning under dynamic-mapping
 * <b>schema drift</b> across segments. Automates the manual cluster reproduction that
 * surfaced the bug (24 segments, wide dynamic schemas, merges off).
 *
 * <p><b>The bug.</b> {@code StatisticsConverter} resolves a column name to a parquet leaf
 * <em>positionally</em>: it finds the column's ordinal in the Arrow schema it is handed and
 * uses that ordinal to index into the parquet file (parquet crate {@code parquet_column}).
 * The stats-pruning paths ({@code eval_leaf} / {@code PagePruner::prune_rg} in
 * {@code page_pruner.rs}, and {@code rg_provably_excluded} in {@code dynamic_filter.rs}) used
 * to pass the <em>union table schema</em>. That schema's field order is first-seen across
 * segments, but each individual segment's parquet column order follows the Java mapper's
 * {@code HashMap} iteration order for <em>that segment's own field set</em>. When segments
 * have different-sized field sets (dynamic mapping adding fields over time), {@code severity}
 * lands at a different ordinal in each segment than it has in the union schema. The positional
 * lookup then reads the wrong leaf — or finds none — yielding all-null stats, and the
 * always-true residual {@code severity >= 0} wrongly prunes those row groups / segments,
 * silently dropping matching rows. The fix resolves stats against each segment's <em>own</em>
 * arrow schema (derived from its parquet footer).
 *
 * <p><b>How drift is induced.</b> {@code message} (text) and {@code severity} (byte) are
 * declared up front, but each flush also indexes a <em>growing</em> set of dynamically-mapped
 * numeric fields {@code pad_0..pad_W(s)} (segment {@code s} is wider than segment {@code s-1}).
 * Because every flush becomes its own parquet segment, merge is disabled (see the
 * {@code integTestNoMerge} cluster variant in build.gradle), and {@code row_group_max_rows} is
 * small (multi-RG segments), successive segments carry differently-sized field sets — the
 * HashMap-order divergence that drifts {@code severity}'s per-segment ordinal away from its
 * union ordinal. The high field count (~300 in the widest segment) is what makes the
 * divergence reliable; small fixtures (a handful of fields) do not drift and pass even without
 * the fix.
 *
 * <p>The {@code text} field is declared in the initial mapping on purpose:
 * {@code match()} on a <em>dynamically-added</em> field across segments is a separate, known
 * issue ({@code DynamicMappingSearchIT#testSearchOnDynamicallyAddedFields}, {@code @AwaitsFix}
 * PR #21701), so the full-text field stays stable and only the numeric pad fields drift.
 *
 * <p><b>Query shapes</b> (each asserted against a ground truth computed during ingest):
 * <ol>
 *   <li>Numeric filter only — {@code where severity >= 0 | stats count()}
 *       (hits {@code eval_leaf} / {@code prune_rg} on the parquet-only path).</li>
 *   <li>Sort + limit (TopK) — {@code where severity >= 0 | sort - severity | head k}
 *       (adds the {@code DynamicRgPruner} / {@code rg_provably_excluded} path).</li>
 *   <li>match() + numeric residual — {@code where match(message,'alpha') and severity >= 0 | stats count()}
 *       (the production shape: Lucene-delegated text predicate AND a stats-pruned residual).</li>
 *   <li>match() + filter + sort + limit — all three prune paths at once.</li>
 * </ol>
 *
 * <p>Pre-fix, the always-true {@code severity >= 0} residual prunes drifted segments and the
 * counts come in <em>below</em> ground truth. Post-fix every count matches exactly.
 */
public class SchemaDriftPruneNoMergeIT extends AnalyticsRestTestCase {

    private static final String INDEX = "schema_drift_prune";
    private static final String TARGET = "alpha";

    // The bug is driven by per-segment dynamic-field WIDTH: the Java mapper's HashMap column
    // order scatters `severity` to a different ordinal in each differently-sized segment,
    // diverging from its first-seen ordinal in the union table schema, so the positional
    // StatisticsConverter lookup reads the wrong/no column. Reproducing it reliably needs many
    // segments of widely-varying width (smaller fixtures — e.g. 4 segments up to 150 fields —
    // do not diverge enough and pass even without the fix). These values mirror the manual
    // cluster reproduction (24 segments, dynamic widths up to ~300, multi-RG segments) and were
    // verified to fail without the fix (match()+severity>=0 returned 6600 instead of 7200).

    /** Each flush becomes one parquet segment; widths grow so per-segment field sets differ. */
    private static final int SEGMENTS = 24;
    /** Docs per segment. With row_group_max_rows small this yields multiple RGs per segment. */
    private static final int DOCS_PER_SEGMENT = 500;
    /** Matching (contain TARGET) docs per segment; the rest are non-matching. */
    private static final int MATCHING_PER_SEGMENT = 300;
    /** Widest segment's pad-field count. Segment s carries pad_0..pad_{W(s)-1}. */
    private static final int MAX_PAD_FIELDS = 300;
    /** Parquet row-group size — small so each segment has several row groups (multi-RG like prod). */
    private static final int ROW_GROUP_MAX_ROWS = 100;

    private static final String[] MATCHING_TEMPLATES = new String[] {
        "bravo alpha charlie",
        "delta alpha echo foxtrot",
        "alpha golf hotel india",
        "juliet kilo alpha lima" };

    private static final String[] NON_MATCHING_TEMPLATES = new String[] {
        "bravo charlie delta",
        "echo foxtrot golf",
        "hotel india juliet kilo",
        "papa quebec romeo sierra" };

    // Ground truth accumulated during ingest.
    private long totalDocs = 0L;
    private long totalMatching = 0L;

    public void testStatsPruningCorrectUnderSchemaDrift() throws Exception {
        createCompositeIndex();
        seedDriftingSegments();

        // 1) Numeric-filter-only: severity >= 0 is true for every doc → full count.
        long filterOnly = scalarCount("source = " + INDEX + " | where severity >= 0 | stats count()");
        assertEquals("numeric filter only (severity>=0) must not drop rows under schema drift", totalDocs, filterOnly);

        // 2) Sort + limit (TopK): exercises the dynamic-filter prune path. head k over the
        //    filtered set must return exactly min(k, totalDocs) rows, all with severity >= 0.
        int k = 50;
        List<List<Object>> topk = datarows(
            executePpl("source = " + INDEX + " | where severity >= 0 | sort - severity | head " + k + " | fields severity")
        );
        assertEquals("sort+limit row count under schema drift", Math.min(k, (int) totalDocs), topk.size());
        for (List<Object> r : topk) {
            assertTrue("every returned row satisfies severity >= 0", num(r.get(0)) >= 0);
        }

        // 3) match() + numeric residual: the production shape. Lucene-delegated match() AND a
        //    stats-pruned residual. Must equal the matching-doc ground truth.
        long matchAndResidual = scalarCount(
            "source = " + INDEX + " | where match(message, '" + TARGET + "') and severity >= 0 | stats count()"
        );
        assertEquals("match() AND severity>=0 must not drop rows under schema drift", totalMatching, matchAndResidual);

        // Sanity: match() alone agrees with the residual-combined count (residual is always true).
        long matchOnly = scalarCount("source = " + INDEX + " | where match(message, '" + TARGET + "') | stats count()");
        assertEquals("match() alone equals match()+residual (residual is always true)", matchOnly, matchAndResidual);

        // 4) match() + filter + sort + limit: all three prune paths together.
        List<List<Object>> combined = datarows(
            executePpl(
                "source = " + INDEX + " | where match(message, '" + TARGET + "') and severity >= 0 "
                    + "| sort - severity | head " + k + " | fields severity"
            )
        );
        assertEquals("match()+filter+sort+limit row count under schema drift", Math.min(k, (int) totalMatching), combined.size());
        for (List<Object> r : combined) {
            assertTrue("every combined row satisfies severity >= 0", num(r.get(0)) >= 0);
        }
    }

    // ── Setup / ingest ───────────────────────────────────────────────────────────

    private void createCompositeIndex() throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {
            // index may not exist yet
        }

        // message + severity declared up front; dynamic:true (the default) lets pad_* fields
        // be auto-mapped per segment, producing the schema drift this test depends on.
        // total_fields.limit is raised to admit the ~300 dynamic pad fields; row_group_max_rows
        // is small so each segment has several row groups.
        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.mapping.total_fields.limit\": 2000,"
            + "  \"index.parquet.row_group_max_rows\": " + ROW_GROUP_MAX_ROWS + ","
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": \"lucene\""
            + "},"
            + "\"mappings\": {"
            + "  \"dynamic\": true,"
            + "  \"properties\": {"
            + "    \"message\": { \"type\": \"text\" },"
            + "    \"severity\": { \"type\": \"byte\" }"
            + "  }"
            + "}"
            + "}";

        Request createIndex = new Request("PUT", "/" + INDEX);
        createIndex.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(createIndex), "create index");
        assertEquals(true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    /**
     * Seeds {@link #SEGMENTS} segments of growing width. Segment {@code s} writes documents
     * carrying {@code pad_0..pad_{W(s)-1}} dynamically-mapped numeric fields, where {@code W(s)}
     * grows from a few fields up to {@link #MAX_PAD_FIELDS}. Successive segments therefore have
     * different-sized field sets → divergent per-segment HashMap column order → {@code severity}
     * drifts to a different ordinal than its union-schema ordinal. Every doc has a non-negative
     * {@code severity}, so {@code severity >= 0} is always true and must never prune.
     */
    private void seedDriftingSegments() throws IOException {
        for (int s = 0; s < SEGMENTS; s++) {
            int width = padWidthForSegment(s);
            StringBuilder bulk = new StringBuilder(DOCS_PER_SEGMENT * 64);
            for (int i = 0; i < DOCS_PER_SEGMENT; i++) {
                boolean matching = i < MATCHING_PER_SEGMENT;
                String message = matching
                    ? MATCHING_TEMPLATES[i % MATCHING_TEMPLATES.length]
                    : NON_MATCHING_TEMPLATES[i % NON_MATCHING_TEMPLATES.length];
                // severity in 0..17, always non-negative.
                int severity = (i % 18);

                StringBuilder doc = new StringBuilder();
                doc.append("{\"message\":\"").append(escapeJson(message)).append("\"");
                doc.append(",\"severity\":").append(severity);
                // Pad values are strongly NEGATIVE so that if `severity` misresolves onto a
                // pad column (positional drift), the wrong column's max < 0 makes the always-true
                // `severity >= 0` evaluate to "cannot match" → the RG/segment is wrongly pruned.
                // (With non-negative pads, a misresolution onto a pad would still satisfy
                // severity >= 0 and silently hide the bug.)
                for (int p = 0; p < width; p++) {
                    doc.append(",\"pad_").append(p).append("\":").append(-1000000 - p);
                }
                doc.append("}");

                bulk.append("{\"index\":{}}\n");
                bulk.append(doc).append("\n");
            }

            Request req = new Request("POST", "/" + INDEX + "/_bulk");
            req.setJsonEntity(bulk.toString());
            Map<String, Object> parsed = assertOkAndParse(client().performRequest(req), "bulk segment " + s);
            assertFalse("bulk errors in segment " + s + ": " + parsed, Boolean.TRUE.equals(parsed.get("errors")));

            totalDocs += DOCS_PER_SEGMENT;
            totalMatching += MATCHING_PER_SEGMENT;

            refresh();
            flush(); // force a parquet segment write; merge is disabled on this cluster variant
        }
    }

    /** Growing pad width per segment: a few fields in segment 0 up to MAX_PAD_FIELDS in the last. */
    private static int padWidthForSegment(int s) {
        int w = (int) (((long) (s + 1) * MAX_PAD_FIELDS) / SEGMENTS);
        return Math.max(1, Math.min(MAX_PAD_FIELDS, w));
    }

    // ── REST helpers ───────────────────────────────────────────────────────────────

    private void refresh() throws IOException {
        client().performRequest(new Request("POST", "/" + INDEX + "/_refresh"));
    }

    private void flush() throws IOException {
        Request req = new Request("POST", "/" + INDEX + "/_flush");
        req.addParameter("force", "true");
        client().performRequest(req);
    }

    private long scalarCount(String ppl) throws IOException {
        Request req = new Request("POST", "/_plugins/_ppl");
        req.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response resp = client().performRequest(req);
        Map<String, Object> parsed = assertOkAndParse(resp, "PPL: " + ppl);
        List<List<Object>> rows = datarows(parsed);
        assertNotNull("PPL response missing datarows for: " + ppl, rows);
        assertEquals("scalar count should return exactly 1 row for: " + ppl, 1, rows.size());
        assertEquals("scalar count should return exactly 1 column for: " + ppl, 1, rows.get(0).size());
        Object v = rows.get(0).get(0);
        assertNotNull("scalar count value must not be null for: " + ppl, v);
        return ((Number) v).longValue();
    }

    @SuppressWarnings("unchecked")
    private static List<List<Object>> datarows(Map<String, Object> result) {
        return (List<List<Object>>) result.get("datarows");
    }

    private static double num(Object cell) {
        return ((Number) cell).doubleValue();
    }
}
