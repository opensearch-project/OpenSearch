/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.AfterClass;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

/**
 * Correctness of filter delegation across boolean shapes, on data laid out so the
 * per-row-group owner election for performance (dual-viable) leaves lands on
 * <em>both</em> DataFusion and Lucene.
 *
 * <p>Existing delegation ITs use tiny datasets (10–200 docs), so every parquet file is one
 * row group with one page per column and the election can never pick DataFusion for a
 * surviving row group. The parquet writer cuts pages at its 1024-row write batch at the
 * earliest ({@code index.parquet.page_row_limit} below that has no effect), so this index
 * is large enough that each segment's single row group has ~25 pages:
 * <ul>
 *   <li>{@code zone} (keyword) is clustered by {@code id}: a {@code zone = ...} leaf is
 *       page-prunable to a few percent of a row group, so DataFusion owns it.</li>
 *   <li>{@code color} (keyword) cycles per doc: never page-prunable, so Lucene owns it.</li>
 *   <li>{@code tier} (keyword) is null for ~20% of docs, to exercise three-valued logic
 *       under NOT.</li>
 *   <li>{@code num}/{@code id} (long) are native-only; {@code msg} (text) is
 *       {@code match()}-only (correctness-delegated).</li>
 * </ul>
 *
 * <p><b>Oracle.</b> The test generates every document itself and evaluates each filter in
 * Java with SQL three-valued logic, so expected results are independent of the engine.
 * Each query returns {@code count, sum(id), min(id), max(id)} so a wrong row set with the
 * right size still fails. Every query runs with {@code prefer_metadata_driver} true and
 * false, on a multi-segment index and on a force-merged copy.
 *
 * <p><b>Mechanism.</b> {@link #testOwnerElectionUsesBothBackends} reads the DataFusion
 * node stats ({@code delegation_calls}) to check that a clustered dual leaf is evaluated
 * without consulting Lucene and a scattered one does consult Lucene. Results alone cannot
 * show this: both owners return the same rows.
 */
public class PerformanceDelegationCorrectnessIT extends AnalyticsRestTestCase {

    /** Several segments per shard, never merged. */
    private static final String INDEX = "perf_delegation_correctness";
    /** Same documents, force-merged to one segment per shard. */
    private static final String INDEX_MERGED = "perf_delegation_correctness_merged";
    /** 2 shards × 2 segments × ~25k rows: one 1024-row page is ~4% of a row group. */
    private static final int NUM_DOCS = 100_000;
    private static final int ZONE_WIDTH = 20;
    private static final int BATCHES = 2;
    private static final int RANDOM_QUERIES = 60;

    private static final String[] COLORS = { "red", "orange", "yellow", "green", "blue", "indigo", "violet" };
    private static final String[] TIERS = { "gold", "silver", "bronze" };
    private static final String[] WORDS = { "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel" };

    private static boolean provisioned = false;
    static List<Doc> docs;

    // ── Data ────────────────────────────────────────────────────────────

    record Doc(long id, String zone, String color, String tier, long num, String msg) {}

    private static List<Doc> generateDocs() {
        // Fixed seed: the dataset is shared by all test methods and must not depend on method order.
        Random r = new Random(0x5EED_DE1EL);
        List<Doc> out = new ArrayList<>(NUM_DOCS);
        for (int i = 0; i < NUM_DOCS; i++) {
            String zone = String.format(Locale.ROOT, "z%04d", i / ZONE_WIDTH);
            String color = COLORS[i % COLORS.length];
            String tier = r.nextInt(5) == 0 ? null : TIERS[r.nextInt(TIERS.length)];
            long num = r.nextInt(1000);
            String msg = WORDS[r.nextInt(WORDS.length)] + " " + WORDS[r.nextInt(WORDS.length)] + " " + WORDS[r.nextInt(WORDS.length)];
            out.add(new Doc(i, zone, color, tier, num, msg));
        }
        return out;
    }

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        docs = generateDocs();
        provision(INDEX, false);
        provision(INDEX_MERGED, true);
        provisioned = true;
    }

    private void provision(String index, boolean forceMerge) throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {
            // index may not exist
        }
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\": {"
                + "\"number_of_shards\": 2, \"number_of_replicas\": 0,"
                + "\"index.pluggable.dataformat.enabled\": true,"
                + "\"index.pluggable.dataformat\": \"composite\","
                + "\"index.composite.primary_data_format\": \"parquet\","
                + "\"index.composite.secondary_data_formats\": [\"lucene\"],"
                // Effective page size is the writer's 1024-row batch; the default (20000) gives one page per file.
                + "\"index.parquet.page_row_limit\": 50"
                + "},"
                + "\"mappings\": {\"properties\": {"
                + "\"id\": {\"type\": \"long\"},"
                + "\"zone\": {\"type\": \"keyword\"},"
                + "\"color\": {\"type\": \"keyword\"},"
                + "\"tier\": {\"type\": \"keyword\"},"
                + "\"num\": {\"type\": \"long\"},"
                + "\"msg\": {\"type\": \"text\"}"
                + "}}}"
        );
        assertOkAndParse(client().performRequest(create), "create index " + index);
        Request health = new Request("GET", "/_cluster/health/" + index);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "60s");
        client().performRequest(health);

        // Several bulk+refresh rounds so each shard has multiple segments (parquet files).
        int perBatch = NUM_DOCS / BATCHES;
        for (int b = 0; b < BATCHES; b++) {
            StringBuilder bulk = new StringBuilder();
            for (int i = b * perBatch; i < (b + 1) * perBatch; i++) {
                Doc d = docs.get(i);
                bulk.append("{\"index\":{}}\n{\"id\":").append(d.id());
                bulk.append(",\"zone\":\"").append(d.zone()).append('"');
                bulk.append(",\"color\":\"").append(d.color()).append('"');
                if (d.tier() != null) {
                    bulk.append(",\"tier\":\"").append(d.tier()).append('"');
                }
                bulk.append(",\"num\":").append(d.num());
                bulk.append(",\"msg\":\"").append(d.msg()).append("\"}\n");
            }
            Request req = new Request("POST", "/" + index + "/_bulk");
            req.setJsonEntity(bulk.toString());
            Map<String, Object> resp = assertOkAndParse(client().performRequest(req), "bulk");
            assertFalse("bulk errors: " + resp, Boolean.TRUE.equals(resp.get("errors")));
            client().performRequest(new Request("POST", "/" + index + "/_refresh"));
            Request flush = new Request("POST", "/" + index + "/_flush");
            flush.addParameter("force", "true");
            client().performRequest(flush);
        }
        if (forceMerge) {
            Request merge = new Request("POST", "/" + index + "/_forcemerge");
            merge.addParameter("max_num_segments", "1");
            client().performRequest(merge);
            client().performRequest(new Request("POST", "/" + index + "/_refresh"));
        }
    }

    /**
     * The shared test cluster keeps indices between suites; these two are large enough to trigger
     * shard rebalancing that times out later suites' health checks, so drop them when done.
     */
    @AfterClass
    public static void deleteIndices() throws IOException {
        if (provisioned) {
            for (String index : new String[] { INDEX, INDEX_MERGED }) {
                client().performRequest(new Request("DELETE", "/" + index));
            }
            provisioned = false;
        }
    }

    @Override
    public void tearDown() throws Exception {
        try {
            setPreferMetadataDriver(true);
        } finally {
            super.tearDown();
        }
    }

    // ── Filter model + oracle (SQL three-valued logic) ──────────────────

    /**
     * Filter AST rendered to PPL and evaluated against {@link Doc}.
     * {@link #eval} is SQL three-valued logic ({@code null} = UNKNOWN; a row matches only when
     * TRUE). {@link #eval2} is two-valued logic where a comparison on a missing value is FALSE
     * (Lucene {@code must_not} semantics); it is only used to classify mismatches.
     */
    interface F {
        String ppl();

        Boolean eval(Doc d);

        default boolean eval2(Doc d) {
            return Boolean.TRUE.equals(eval(d));
        }
    }

    record Eq(String field, String value) implements F {
        public String ppl() {
            return field + " = '" + value + "'";
        }

        public Boolean eval(Doc d) {
            String v = keyword(d, field);
            return v == null ? null : v.equals(value);
        }
    }

    record Ne(String field, String value) implements F {
        public String ppl() {
            return field + " != '" + value + "'";
        }

        public Boolean eval(Doc d) {
            String v = keyword(d, field);
            return v == null ? null : v.equals(value) == false;
        }
    }

    record IsNull(String field, boolean negate) implements F {
        public String ppl() {
            return (negate ? "isnotnull(" : "isnull(") + field + ")";
        }

        public Boolean eval(Doc d) {
            return (keyword(d, field) == null) != negate;
        }
    }

    record Lt(String field, long value) implements F {
        public String ppl() {
            return field + " < " + value;
        }

        public Boolean eval(Doc d) {
            return (field.equals("id") ? d.id() : d.num()) < value;
        }
    }

    record Ge(String field, long value) implements F {
        public String ppl() {
            return field + " >= " + value;
        }

        public Boolean eval(Doc d) {
            return (field.equals("id") ? d.id() : d.num()) >= value;
        }
    }

    record Match(String word) implements F {
        public String ppl() {
            return "match(msg, '" + word + "')";
        }

        public Boolean eval(Doc d) {
            for (String t : d.msg().split(" ")) {
                if (t.equals(word)) return true;
            }
            return false;
        }
    }

    record And(List<F> children) implements F {
        public String ppl() {
            return join(children, " AND ");
        }

        public boolean eval2(Doc d) {
            return children.stream().allMatch(c -> c.eval2(d));
        }

        public Boolean eval(Doc d) {
            boolean unknown = false;
            for (F c : children) {
                Boolean v = c.eval(d);
                if (Boolean.FALSE.equals(v)) return false;
                if (v == null) unknown = true;
            }
            return unknown ? null : true;
        }
    }

    record Or(List<F> children) implements F {
        public String ppl() {
            return join(children, " OR ");
        }

        public boolean eval2(Doc d) {
            return children.stream().anyMatch(c -> c.eval2(d));
        }

        public Boolean eval(Doc d) {
            boolean unknown = false;
            for (F c : children) {
                Boolean v = c.eval(d);
                if (Boolean.TRUE.equals(v)) return true;
                if (v == null) unknown = true;
            }
            return unknown ? null : false;
        }
    }

    record Not(F child) implements F {
        public String ppl() {
            return "NOT (" + child.ppl() + ")";
        }

        public boolean eval2(Doc d) {
            return child.eval2(d) == false;
        }

        public Boolean eval(Doc d) {
            Boolean v = child.eval(d);
            return v == null ? null : v == false;
        }
    }

    private static String keyword(Doc d, String field) {
        return switch (field) {
            case "zone" -> d.zone();
            case "color" -> d.color();
            case "tier" -> d.tier();
            default -> throw new IllegalArgumentException(field);
        };
    }

    private static String join(List<F> children, String op) {
        List<String> parts = new ArrayList<>();
        for (F c : children) {
            parts.add("(" + c.ppl() + ")");
        }
        return String.join(op, parts);
    }

    static F and(F... c) {
        return new And(List.of(c));
    }

    static F or(F... c) {
        return new Or(List.of(c));
    }

    static F not(F c) {
        return new Not(c);
    }

    // Leaf vocabulary. dual = keyword equality (performance-delegated under AND),
    // native = long comparison, delegated = match() on text.
    static F zone(int z) {
        return new Eq("zone", String.format(Locale.ROOT, "z%04d", z));
    }

    static final F DUAL_CLUSTERED = zone(123);            // DataFusion-owned where it survives
    static final F DUAL_CLUSTERED_2 = zone(3210);
    static final F DUAL_SCATTERED = new Eq("color", "red"); // Lucene-owned
    static final F DUAL_SCATTERED_2 = new Eq("color", "blue");
    static final F DUAL_ABSENT = new Eq("color", "magenta"); // in stats range, no matches
    static final F DUAL_NULLABLE = new Eq("tier", "gold");
    static final F NATIVE = new Lt("num", 300);
    static final F NATIVE_ID = new Ge("id", 25_000);
    static final F DELEGATED = new Match("alpha");
    static final F DELEGATED_2 = new Match("echo");

    /** Hand-picked shapes: every leaf-kind combination under AND / OR / NOT, plus nesting. */
    private static List<F> curatedFilters() {
        return List.of(
            // single leaves
            DUAL_CLUSTERED,
            DUAL_SCATTERED,
            DUAL_ABSENT,
            DUAL_NULLABLE,
            new Ne("tier", "gold"),
            new IsNull("tier", false),
            new IsNull("tier", true),
            NATIVE,
            DELEGATED,
            // performance-only conjunctions (the per-RG election case)
            and(DUAL_CLUSTERED, DUAL_SCATTERED),
            and(DUAL_SCATTERED, DUAL_SCATTERED_2),
            and(DUAL_SCATTERED, DUAL_NULLABLE),
            and(DUAL_CLUSTERED, DUAL_NULLABLE, DUAL_SCATTERED),
            and(DUAL_ABSENT, DUAL_SCATTERED),
            // performance + native
            and(DUAL_CLUSTERED, NATIVE),
            and(DUAL_SCATTERED, NATIVE),
            and(DUAL_SCATTERED, NATIVE_ID),
            and(DUAL_CLUSTERED, DUAL_SCATTERED, NATIVE, NATIVE_ID),
            // performance + correctness (Fix 8 demotion under a root AND)
            and(DUAL_CLUSTERED, DELEGATED),
            and(DUAL_SCATTERED, DELEGATED),
            and(DUAL_SCATTERED, DUAL_NULLABLE, DELEGATED),
            and(DUAL_SCATTERED, DELEGATED, NATIVE),
            and(DELEGATED, DELEGATED_2, DUAL_SCATTERED),
            // OR / NOT (dual leaves reclassified to correctness)
            or(DUAL_CLUSTERED, DUAL_SCATTERED),
            or(DUAL_SCATTERED, NATIVE),
            or(DUAL_CLUSTERED, DELEGATED),
            or(DUAL_NULLABLE, DELEGATED, NATIVE),
            not(DUAL_SCATTERED),
            not(DUAL_NULLABLE),
            not(and(DUAL_SCATTERED, DUAL_NULLABLE)),
            not(or(DUAL_NULLABLE, DELEGATED)),
            // nesting
            and(or(DUAL_SCATTERED, DUAL_SCATTERED_2), NATIVE),
            and(or(DUAL_CLUSTERED, DUAL_CLUSTERED_2), DUAL_SCATTERED),
            and(DUAL_SCATTERED, or(DELEGATED, NATIVE)),
            and(DUAL_SCATTERED, not(DUAL_NULLABLE)),
            and(NATIVE_ID, not(DELEGATED), DUAL_SCATTERED),
            or(and(DUAL_SCATTERED, NATIVE), and(DUAL_CLUSTERED, DELEGATED)),
            or(and(DUAL_SCATTERED, DUAL_NULLABLE), not(NATIVE))
        );
    }

    private static final F[] LEAVES = {
        DUAL_CLUSTERED,
        DUAL_CLUSTERED_2,
        DUAL_SCATTERED,
        DUAL_SCATTERED_2,
        DUAL_ABSENT,
        DUAL_NULLABLE,
        new Ne("color", "green"),
        new IsNull("tier", false),
        NATIVE,
        NATIVE_ID,
        new Lt("id", 90_000),
        DELEGATED,
        DELEGATED_2 };

    private static F randomFilter(Random r, int depth) {
        if (depth == 0 || r.nextInt(3) == 0) {
            return LEAVES[r.nextInt(LEAVES.length)];
        }
        int kind = r.nextInt(5);
        if (kind == 0) {
            return not(randomFilter(r, depth - 1));
        }
        List<F> children = new ArrayList<>();
        int n = 2 + r.nextInt(2);
        for (int i = 0; i < n; i++) {
            children.add(randomFilter(r, depth - 1));
        }
        // Bias toward AND: the conjunctive path is where performance leaves live.
        return kind <= 2 ? new And(children) : new Or(children);
    }

    // ── Tests ───────────────────────────────────────────────────────────

    public void testCuratedShapes() throws Exception {
        runAll(curatedFilters(), "curated");
    }

    /**
     * OR of a {@code match()} with keyword predicates that Calcite folds into one SEARCH/Sarg
     * (same-field equalities, or {@code != / IS NULL} on one field). DataFusion's SargAdapter
     * expands the SEARCH before serialization, so Lucene can't take the leaf and it stays native;
     * the plan used to be labelled CONJUNCTIVE anyway and returned every row.
     */
    public void testOrOfFoldedKeywordPredicatesWithMatch() throws Exception {
        runAll(
            List.of(
                or(DUAL_CLUSTERED, DUAL_CLUSTERED_2, DELEGATED),
                or(DUAL_SCATTERED, DUAL_SCATTERED_2, DELEGATED),
                and(DUAL_SCATTERED, or(DUAL_CLUSTERED, DUAL_CLUSTERED_2, DELEGATED)),
                or(DELEGATED, not(DUAL_NULLABLE), new IsNull("tier", false))
            ),
            "folded-or"
        );
    }

    public void testRandomShapes() throws Exception {
        long seed = randomLong();
        logger.info("PerformanceDelegationCorrectnessIT random seed={}", seed);
        Random r = new Random(seed);
        List<F> filters = new ArrayList<>();
        for (int i = 0; i < RANDOM_QUERIES; i++) {
            filters.add(randomFilter(r, 3));
        }
        runAll(filters, "random seed=" + seed);
    }

    /**
     * On a real cluster, a clustered dual leaf is evaluated by DataFusion without a Lucene
     * call, and a scattered dual leaf is evaluated by Lucene.
     */
    public void testOwnerElectionUsesBothBackends() throws Exception {
        setPreferMetadataDriver(false);
        // AND with a native leaf keeps DataFusion as the driver with a CONJUNCTIVE tree.
        F clustered = and(DUAL_CLUSTERED, new Ge("num", 0));
        F scattered = and(DUAL_SCATTERED, new Ge("num", 0));

        long[] before = searchStats();
        assertCorrect(INDEX_MERGED, clustered, "owner check, clustered");
        long[] mid = searchStats();
        assertCorrect(INDEX_MERGED, scattered, "owner check, scattered");
        long[] after = searchStats();

        long clusteredCalls = mid[0] - before[0];
        long clusteredScans = mid[1] - before[1];
        long scatteredCalls = after[0] - mid[0];
        long scatteredScans = after[1] - mid[1];
        logger.info(
            "owner check: clustered calls={} scans={}, scattered calls={} scans={}",
            clusteredCalls,
            clusteredScans,
            scatteredCalls,
            scatteredScans
        );
        assertTrue("clustered query must take the indexed single-collector path", clusteredScans > 0);
        assertTrue("scattered query must take the indexed single-collector path", scatteredScans > 0);
        assertEquals("clustered dual leaf must be owned by DataFusion (no Lucene call)", 0, clusteredCalls);
        assertTrue("scattered dual leaf must be owned by Lucene (Lucene called)", scatteredCalls > 0);
    }

    // ── Harness ─────────────────────────────────────────────────────────

    private void runAll(List<F> filters, String label) throws Exception {
        List<String> failures = new ArrayList<>();
        for (String index : new String[] { INDEX, INDEX_MERGED }) {
            for (boolean prefer : new boolean[] { true, false }) {
                setPreferMetadataDriver(prefer);
                for (F f : filters) {
                    String ctx = label + " index=" + index + " prefer_metadata_driver=" + prefer;
                    String err = check(index, f, ctx);
                    if (err != null) {
                        failures.add(err);
                    }
                }
            }
        }
        if (failures.isEmpty() == false) {
            fail(failures.size() + " mismatches:\n" + String.join("\n", failures));
        }
    }

    private void assertCorrect(String index, F f, String ctx) throws Exception {
        String err = check(index, f, ctx);
        if (err != null) {
            fail(err);
        }
    }

    /** {count, sum(id), min(id), max(id)} of the rows the filter selects. */
    static long[] oracle(F f, boolean threeValued) {
        long count = 0, sum = 0, min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        for (Doc d : docs) {
            if (threeValued ? Boolean.TRUE.equals(f.eval(d)) : f.eval2(d)) {
                count++;
                sum += d.id();
                min = Math.min(min, d.id());
                max = Math.max(max, d.id());
            }
        }
        return new long[] { count, sum, min, max };
    }

    private static boolean matches(long[] expected, List<Object> row) {
        if (((Number) row.get(0)).longValue() != expected[0]) return false;
        if (expected[0] == 0) return true;
        for (int i = 1; i < 4; i++) {
            if (row.get(i) == null || ((Number) row.get(i)).longValue() != expected[i]) return false;
        }
        return true;
    }

    /**
     * Returns a failure description, or {@code null} when the engine agrees with the
     * three-valued oracle. A mismatch that agrees with the two-valued oracle is tagged
     * {@code NULL-SEMANTICS}; anything else is {@code WRONG-ROWS}.
     */
    String check(String index, F f, String ctx) throws Exception {
        long[] expected = oracle(f, true);
        String ppl = "source = " + index + " | where " + f.ppl() + " | stats count() as c, sum(id) as s, min(id) as mn, max(id) as mx";
        List<Object> row;
        try {
            row = singleRow(executePpl(ppl));
        } catch (ResponseException e) {
            String body = EntityUtils.toString(e.getResponse().getEntity());
            return ctx + " ERROR HTTP " + e.getResponse().getStatusLine().getStatusCode() + " " + body + "\n  ppl: " + ppl;
        }
        if (matches(expected, row)) {
            return null;
        }
        long[] twoValued = oracle(f, false);
        String kind = matches(twoValued, row) ? "NULL-SEMANTICS" : "WRONG-ROWS";
        return String.format(
            Locale.ROOT,
            "%s %s expected(3VL)=%s expected(2VL)=%s got=%s\n  ppl: %s",
            ctx,
            kind,
            Arrays.toString(expected),
            Arrays.toString(twoValued),
            row,
            ppl
        );
    }

    @SuppressWarnings("unchecked")
    private static List<Object> singleRow(Map<String, Object> response) {
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("no datarows: " + response, rows);
        assertEquals("expected one row: " + response, 1, rows.size());
        return rows.get(0);
    }

    /** Cluster-wide {delegation_calls, single_collector_scan} from the DataFusion node stats. */
    @SuppressWarnings("unchecked")
    private long[] searchStats() throws IOException {
        Map<String, Object> resp = assertOkAndParse(
            client().performRequest(new Request("GET", "/_plugins/_analytics_backend_datafusion/stats")),
            "datafusion stats"
        );
        Map<String, Object> nodes = (Map<String, Object>) resp.get("nodes");
        assertNotNull("no nodes in stats: " + resp, nodes);
        long calls = 0, scans = 0;
        for (Object n : nodes.values()) {
            Map<String, Object> ss = (Map<String, Object>) ((Map<String, Object>) n).get("search_stats");
            assertNotNull("no search_stats: " + n, ss);
            calls += ((Number) ss.get("delegation_calls")).longValue();
            scans += ((Number) ss.get("single_collector_scan")).longValue();
        }
        return new long[] { calls, scans };
    }

    private void setPreferMetadataDriver(boolean value) throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"persistent\":{\"analytics.planner.prefer_metadata_driver\": " + value + "}}");
        client().performRequest(req);
    }
}
