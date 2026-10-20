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
 * End-to-end tests for the count fast path covering the four shape buckets the planner
 * must navigate:
 *
 * <ol>
 *   <li><b>Lucene-only viable</b> — predicate references a keyword/text field that lives in
 *       Lucene's secondary inverted index and isn't reproducible by the parquet/DataFusion
 *       column scan. Lucene drives end-to-end via {@code LuceneSearchExecEngine}.</li>
 *   <li><b>DataFusion-only viable</b> — predicate references a numeric field which only
 *       parquet has doc values for; Lucene is not viable for the operator and must NOT be
 *       picked. The DataFusion engine path runs.</li>
 *   <li><b>Mixed AND/OR</b> — leaves split between backends. {@code FilterTreeShape} drives
 *       the conjunctive vs disjunctive delegation, and (when the count detector matches) the
 *       combiner fuses both sides into one Lucene query.</li>
 *   <li><b>Full-text on a text field</b> — {@code MATCH(message, ...)} resolves only on
 *       Lucene; correctness delegation puts the predicate on Lucene's side and the count
 *       fast path runs.</li>
 * </ol>
 *
 * <p>Multi-segment ingest exercises Lucene's per-leaf {@code Weight.count(LeafReaderContext)}
 * summation. Each test method computes its own oracle from {@link #DOCS} so adding rows in
 * one place doesn't silently break a different assertion.
 *
 * <p>Run with:
 * {@code ./gradlew :sandbox:qa:analytics-engine-rest:integTest --tests "*.CountFastPathIT" -Dsandbox.enabled=true}
 */
public class CountFastPathIT extends AnalyticsRestTestCase {

    private static final String INDEX = "count_fast_path_e2e";

    /**
     * Single source of truth — every assertion's oracle is computed from this list.
     * Layout (segment splits in {@link #ingestThreeSegments}):
     * <pre>
     *   segment 0: u_a/click/10/us/"alpha beta", u_a/view/20/eu/"beta gamma",
     *              u_a/click/30/us/"alpha", u_b/view/40/eu/"gamma",
     *              u_b/click/50/us/"alpha beta gamma", u_c/view/60/apac/"alpha gamma",
     *              u_c/click/70/apac/"beta"
     *   segment 1: u_a/view/11/us/"alpha alpha", u_a/click/21/eu/"beta",
     *              u_a/view/31/us/"gamma", u_b/click/41/eu/"alpha",
     *              u_b/view/51/us/"alpha beta", u_c/click/61/apac/"gamma alpha",
     *              u_c/view/71/apac/"beta beta", u_c/click/81/us/"alpha"
     *   segment 2: u_a/view/12/us/"alpha", u_seg3_only/click/22/eu/"beta gamma",
     *              u_seg3_only/view/32/eu/"alpha", u_seg3_only/click/42/apac/"gamma"
     * </pre>
     */
    private static final List<Doc> DOCS = List.of(
        // segment 0
        new Doc("u_a", "click", 10, "us", "alpha beta"),
        new Doc("u_a", "view", 20, "eu", "beta gamma"),
        new Doc("u_a", "click", 30, "us", "alpha"),
        new Doc("u_b", "view", 40, "eu", "gamma"),
        new Doc("u_b", "click", 50, "us", "alpha beta gamma"),
        new Doc("u_c", "view", 60, "apac", "alpha gamma"),
        new Doc("u_c", "click", 70, "apac", "beta"),
        // segment 1
        new Doc("u_a", "view", 11, "us", "alpha alpha"),
        new Doc("u_a", "click", 21, "eu", "beta"),
        new Doc("u_a", "view", 31, "us", "gamma"),
        new Doc("u_b", "click", 41, "eu", "alpha"),
        new Doc("u_b", "view", 51, "us", "alpha beta"),
        new Doc("u_c", "click", 61, "apac", "gamma alpha"),
        new Doc("u_c", "view", 71, "apac", "beta beta"),
        new Doc("u_c", "click", 81, "us", "alpha"),
        // segment 2
        new Doc("u_a", "view", 12, "us", "alpha"),
        new Doc("u_seg3_only", "click", 22, "eu", "beta gamma"),
        new Doc("u_seg3_only", "view", 32, "eu", "alpha"),
        new Doc("u_seg3_only", "click", 42, "apac", "gamma")
    );

    /** Range of doc indices that go into each segment (start inclusive, end exclusive). */
    private static final int[] SEGMENT_BOUNDS = { 0, 7, 15, DOCS.size() };

    public void testCountAcrossMultipleSegments() throws Exception {
        createIndex();
        ingestThreeSegments();

        long total = DOCS.size();

        // Unfiltered count: no predicate, Lucene driver still picks up via metadata-only scan.
        // Validates the count(*) fast path through Lucene's IndexReader.numDocs.
        assertCount("stats count() as cnt", total);

        // Single keyword equality (Lucene-only viable: keyword has indexFormats=[lucene],
        // operator EQ supported on Lucene). 'u_a' appears 7 times across all three segments.
        assertCount("where userID = 'u_a' | stats count() as cnt", oracleWhere(d -> d.userID.equals("u_a")));

        // Zero-match: 'dave' isn't in any segment → Weight.count returns 0 per leaf.
        assertCount("where userID = 'dave' | stats count() as cnt", 0);

        // Single-segment-only term: 'u_seg3_only' only in segment 2. Per-leaf summation
        // covers segments where most leaves contribute zero.
        assertCount(
            "where userID = 'u_seg3_only' | stats count() as cnt",
            oracleWhere(d -> d.userID.equals("u_seg3_only"))
        );

        // Coverage parity: every doc has exactly one event_type, so click_count + view_count = total.
        long clicks = countOf("where event_type = 'click' | stats count() as cnt");
        long views = countOf("where event_type = 'view' | stats count() as cnt");
        assertEquals("clicks + views must equal total docs (every event has one type)", total, clicks + views);
    }

    /**
     * Predicates that only DataFusion can answer — numeric range on the parquet-backed
     * {@code amount} field. Lucene has no doc values for it (long fields aren't in the
     * STANDARD_TYPES set), so the planner must drop Lucene from the alternative list and
     * the DataFusion engine path runs end-to-end. Asserted via correctness of the count.
     */
    public void testNumericFilter_dataFusionOnly() throws Exception {
        createIndex();
        ingestThreeSegments();

        assertCount("where amount > 50 | stats count() as cnt", oracleWhere(d -> d.amount > 50));
        assertCount("where amount >= 50 | stats count() as cnt", oracleWhere(d -> d.amount >= 50));
        assertCount("where amount = 22 | stats count() as cnt", oracleWhere(d -> d.amount == 22));
        // Empty range — every doc's amount > 0, so amount<0 is empty.
        assertCount("where amount < 0 | stats count() as cnt", 0);
    }

    /**
     * Mixed-backend AND: keyword-side leaf (Lucene viable) AND numeric-side leaf
     * (DataFusion-only). The numeric leaf forces DataFusion as the operator backend on the
     * Filter, so the keyword leaf either correctness-delegates to Lucene (when DataFusion
     * doesn't have it natively) or stays as a {@code delegation_possible} performance hint.
     * Either way, the count must match the oracle.
     */
    public void testMixedAndFilter_keywordPlusNumeric() throws Exception {
        createIndex();
        ingestThreeSegments();

        assertCount(
            "where userID = 'u_a' AND amount > 20 | stats count() as cnt",
            oracleWhere(d -> d.userID.equals("u_a") && d.amount > 20)
        );

        // OR across backends — the combiner declines to fuse under disjunction unless
        // dual-viable is in effect (count fast path forces it on).
        assertCount(
            "where userID = 'u_b' OR amount = 42 | stats count() as cnt",
            oracleWhere(d -> d.userID.equals("u_b") || d.amount == 42)
        );
    }

    /**
     * Pure Lucene-side AND/OR over keyword fields. Both leaves are correctness-delegated to
     * Lucene; the combiner emits a single fused {@code BoolQueryBuilder} which Lucene's
     * {@code IndexSearcher.count} resolves via the term dictionary.
     */
    public void testKeywordBooleanFilter_luceneFused() throws Exception {
        createIndex();
        ingestThreeSegments();

        // AND of two keyword EQUALS — fully Lucene-driven, combiner not invoked.
        String andPpl = "where userID = 'u_a' AND event_type = 'click' | stats count() as cnt";
        assertCount(andPpl, oracleWhere(d -> d.userID.equals("u_a") && d.eventType.equals("click")));
        Map<String, Object> andExplain = executeExplain("source = " + INDEX + " | " + andPpl);
        assertShardFragmentChoseBackend(andExplain, "lucene");

        // EQUALS + MATCH on keyword + text — both Lucene-resolvable; combiner ships them as
        // one fused BoolQueryBuilder. Picks up perf-delegation on the keyword leaf and
        // correctness-delegation on the MATCH leaf in the same shape.
        assertCount(
            "where userID = 'u_a' AND match(message, 'alpha') | stats count() as cnt",
            oracleWhere(d -> d.userID.equals("u_a") && tokenizedContains(d.message, "alpha"))
        );

        // OR mixing keyword EQUALS with MATCH — the dual-viable EQUALS fuses with the MATCH
        // correctness sibling into one Lucene shipment under OR; the count is unaffected.
        assertCount(
            "where userID = 'u_b' OR match(message, 'gamma') | stats count() as cnt",
            oracleWhere(d -> d.userID.equals("u_b") || tokenizedContains(d.message, "gamma"))
        );

        assertCount(
            "where region = 'us' AND event_type = 'view' | stats count() as cnt",
            oracleWhere(d -> d.region.equals("us") && d.eventType.equals("view"))
        );

        assertCount(
            "where userID = 'u_b' OR userID = 'u_seg3_only' | stats count() as cnt",
            oracleWhere(d -> d.userID.equals("u_b") || d.userID.equals("u_seg3_only"))
        );

        // Three-way AND across keyword fields exercises BoolQueryBuilder with multiple MUST clauses.
        assertCount(
            "where userID = 'u_a' AND event_type = 'view' AND region = 'us' | stats count() as cnt",
            oracleWhere(d -> d.userID.equals("u_a") && d.eventType.equals("view") && d.region.equals("us"))
        );
    }

    /**
     * NOT and not-equals on keyword. Both lower to {@code BoolQueryBuilder.mustNot} —
     * still Lucene-viable, still the fast path.
     */
    public void testNegationFilter() throws Exception {
        createIndex();
        ingestThreeSegments();

        assertCount(
            "where userID != 'u_a' | stats count() as cnt",
            oracleWhere(d -> !d.userID.equals("u_a"))
        );

        assertCount(
            "where NOT(event_type = 'click') | stats count() as cnt",
            oracleWhere(d -> !d.eventType.equals("click"))
        );
    }

    /**
     * IN-list lowers to {@code BoolQueryBuilder.should} (term-set query) on Lucene.
     */
    public void testInListFilter() throws Exception {
        createIndex();
        ingestThreeSegments();

        assertCount(
            "where userID IN ('u_a', 'u_c') | stats count() as cnt",
            oracleWhere(d -> d.userID.equals("u_a") || d.userID.equals("u_c"))
        );

        // Single-element IN reduces to equality.
        assertCount(
            "where region IN ('apac') | stats count() as cnt",
            oracleWhere(d -> d.region.equals("apac"))
        );
    }

    /**
     * Full-text {@code MATCH} on a text field. Only Lucene supports MATCH for any field
     * type, so the planner chooses Lucene and ships the predicate as a NamedWriteable
     * {@code MatchQueryBuilder}.
     */
    public void testFullTextMatchFilter() throws Exception {
        createIndex();
        ingestThreeSegments();

        // 'alpha' appears in many docs; oracle counts whitespace-tokenised occurrences.
        String alphaPpl = "where match(message, 'alpha') | stats count() as cnt";
        assertCount(alphaPpl, oracleWhere(d -> tokenizedContains(d.message, "alpha")));
        Map<String, Object> alphaExplain = executeExplain("source = " + INDEX + " | " + alphaPpl);
        assertShardFragmentChoseBackend(alphaExplain, "lucene");

        // Term that's present in fewer docs.
        assertCount(
            "where match(message, 'gamma') | stats count() as cnt",
            oracleWhere(d -> tokenizedContains(d.message, "gamma"))
        );

        // Combined with a keyword filter — both delegate to Lucene, fused into one BoolQueryBuilder.
        assertCount(
            "where userID = 'u_a' AND match(message, 'alpha') | stats count() as cnt",
            oracleWhere(d -> d.userID.equals("u_a") && tokenizedContains(d.message, "alpha"))
        );
    }

    /**
     * {@code count(field)} on a keyword field — DataFusion drives by design. PPL's
     * {@code count(field)} desugars to {@code COUNT(field) → Project(field) →
     * Filter(IS NOT NULL(field)) → Scan} (count of non-null values), and the {@code IS NOT NULL}
     * filter on a keyword field has no Lucene cap declared, so the operator narrows to
     * DataFusion only. Asserting on row correctness (DF computes the right count) is the
     * useful invariant; backend choice is forced by the planner.
     *
     * <p>{@code count()} (no field) is the actual Lucene-driven path —
     * {@link #testCountAcrossMultipleSegments} and
     * {@link #testCountAcrossMultipleShards_luceneShardsToDataFusionReduce} cover that.
     */
    public void testCountByKeywordField_drivenByDataFusion() throws Exception {
        createIndex();
        ingestThreeSegments();

        long total = DOCS.size();
        assertCount("stats count(userID) as cnt", total);

        Map<String, Object> explain = executeExplain("source = " + INDEX + " | stats count(userID) as cnt");
        assertShardFragmentChoseBackend(explain, "datafusion");

        // count(field) + correctness-delegated MATCH filter: Project/Filter narrow to DF, but
        // the MATCH leaf delegates to Lucene per row group. Asserts both correctness and that
        // the SHARD_FRAGMENT remains DataFusion-driven (delegation, not driver collapse).
        assertCount(
            "where match(message, 'alpha') | stats count(userID) as cnt",
            oracleWhere(d -> tokenizedContains(d.message, "alpha"))
        );
        Map<String, Object> mixedExplain = executeExplain(
            "source = " + INDEX + " | where match(message, 'alpha') | stats count(userID) as cnt"
        );
        assertShardFragmentChoseBackend(mixedExplain, "datafusion");
    }

    /**
     * {@code count(field)} on a numeric field where {@code amount} only has parquet doc values.
     * Lucene's {@code Index(supportedFieldTypes={KEYWORD, TEXT, MATCH_ONLY_TEXT})}
     * doesn't cover LONG, so the planner never marks Lucene viable — the SHARD_FRAGMENT must
     * be DataFusion. Catches the planner-level regression where a numeric scan accidentally
     * picks Lucene because the field has {@code indexFormats=[lucene]} (BKD points).
     */
    public void testCountByNumericField_drivenByDataFusion() throws Exception {
        createIndex();
        ingestThreeSegments();

        long total = DOCS.size();
        assertCount("stats count(amount) as cnt", total);

        Map<String, Object> explain = executeExplain("source = " + INDEX + " | stats count(amount) as cnt");
        assertShardFragmentChoseBackend(explain, "datafusion");
    }

    /**
     * Multi-shard count: exercises the cross-backend partition boundary that single-shard tests
     * skip entirely. SHARD_FRAGMENT runs Lucene, COORDINATOR_REDUCE runs DataFusion summing
     * per-shard counts. The reducer registers each shard's partition stream against the
     * Substrait stub from {@code LuceneFragmentConvertor.convertSchemaOnlyRead}; if Lucene's
     * non-Substrait wire bytes leak into {@code derive_schema_from_partial_plan}, this test
     * fails with "decode failed: invalid tag value".
     *
     * <p>Three shards is enough to make the bug deterministic — fewer shards risk Lucene's
     * stage being elided into the coord stage by the planner.
     */
    public void testCountAcrossMultipleShards_luceneShardsToDataFusionReduce() throws Exception {
        createIndex(3);
        ingestThreeSegments();

        long total = DOCS.size();
        assertCount("stats count() as cnt", total);
        assertCount("where userID = 'u_a' | stats count() as cnt", oracleWhere(d -> d.userID.equals("u_a")));

        Map<String, Object> explain = executeExplain("source = " + INDEX + " | stats count() as cnt");
        assertShardFragmentChoseBackend(explain, "lucene");
        assertCoordinatorReduceChoseBackend(explain, "datafusion");
        // No WHERE clause → no delegation instruction → tree_shape must be absent on both
        // stages. Pins that the multi-shard reducer path doesn't accidentally synthesize a
        // FilterDelegationInstructionNode where there's no filter to delegate.
        assertStageHasNoTreeShape(explain, "SHARD_FRAGMENT");
        assertStageHasNoTreeShape(explain, "COORDINATOR_REDUCE");
    }

    // ── Explain helpers ─────────────────────────────────────────────────────

    private Map<String, Object> executeExplain(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl/_explain");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "EXPLAIN: " + ppl);
    }

    /** Asserts the SHARD_FRAGMENT stage in {@code explain.profile.stages} chose the given backend. */
    private static void assertShardFragmentChoseBackend(Map<String, Object> explain, String expectedBackend) {
        assertStageChoseBackend(explain, "SHARD_FRAGMENT", expectedBackend);
    }

    /** Asserts the COORDINATOR_REDUCE stage in {@code explain.profile.stages} chose the given backend. */
    private static void assertCoordinatorReduceChoseBackend(Map<String, Object> explain, String expectedBackend) {
        assertStageChoseBackend(explain, "COORDINATOR_REDUCE", expectedBackend);
    }

    @SuppressWarnings("unchecked")
    private static void assertStageChoseBackend(Map<String, Object> explain, String executionType, String expectedBackend) {
        Map<String, Object> profile = (Map<String, Object>) explain.get("profile");
        assertNotNull("profile present", profile);
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");
        assertNotNull("stages present", stages);
        for (Map<String, Object> stage : stages) {
            if (executionType.equals(stage.get("execution_type"))) {
                assertEquals(
                    executionType + " chose unexpected backend (full stage: " + stage + ")",
                    expectedBackend,
                    stage.get("chosen_backend")
                );
                return;
            }
        }
        fail("No " + executionType + " stage in profile: " + stages);
    }

    /** Asserts the named stage has no {@code tree_shape} field — i.e. no delegation instruction. */
    @SuppressWarnings("unchecked")
    private static void assertStageHasNoTreeShape(Map<String, Object> explain, String executionType) {
        Map<String, Object> profile = (Map<String, Object>) explain.get("profile");
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");
        for (Map<String, Object> stage : stages) {
            if (executionType.equals(stage.get("execution_type"))) {
                assertNull(executionType + " unexpectedly carries tree_shape (full stage: " + stage + ")", stage.get("tree_shape"));
                return;
            }
        }
        fail("No " + executionType + " stage in profile: " + stages);
    }

    // ── Oracle helpers ──────────────────────────────────────────────────────

    private static long oracleWhere(java.util.function.Predicate<Doc> p) {
        return DOCS.stream().filter(p).count();
    }

    /** Standard-analyzer-equivalent: token equality on whitespace splits. */
    private static boolean tokenizedContains(String text, String token) {
        for (String t : text.toLowerCase(java.util.Locale.ROOT).split("\\s+")) {
            if (t.equals(token)) return true;
        }
        return false;
    }

    // ── Setup ───────────────────────────────────────────────────────────────

    private void createIndex() throws Exception {
        createIndex(1);
    }

    private void createIndex(int numberOfShards) throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": "
            + numberOfShards
            + ","
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": \"lucene\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"userID\": { \"type\": \"keyword\" },"
            + "    \"event_type\": { \"type\": \"keyword\" },"
            + "    \"region\": { \"type\": \"keyword\" },"
            + "    \"message\": { \"type\": \"text\" },"
            + "    \"amount\": { \"type\": \"long\" }"
            + "  }"
            + "}"
            + "}";
        Request req = new Request("PUT", "/" + INDEX);
        req.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(req), "Create index");
        assertEquals(true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    /**
     * Ingest in three waves (defined by {@link #SEGMENT_BOUNDS}) with force-flush between
     * each so each wave becomes its own Lucene segment. Multi-segment ingest exercises the
     * per-leaf {@code Weight.count} summation inside {@code IndexSearcher.count}.
     */
    private void ingestThreeSegments() throws Exception {
        for (int seg = 0; seg < SEGMENT_BOUNDS.length - 1; seg++) {
            int from = SEGMENT_BOUNDS[seg];
            int to = SEGMENT_BOUNDS[seg + 1];
            String[] batch = new String[to - from];
            for (int i = from; i < to; i++) {
                batch[i - from] = DOCS.get(i).toJson();
            }
            bulkIndex(docs(batch));
            flush();
        }
    }

    // ── Document model ──────────────────────────────────────────────────────

    private record Doc(String userID, String eventType, long amount, String region, String message) {
        String toJson() {
            return "{\"userID\": \""
                + userID
                + "\", \"event_type\": \""
                + eventType
                + "\", \"region\": \""
                + region
                + "\", \"message\": \""
                + message
                + "\", \"amount\": "
                + amount
                + "}";
        }
    }

    private static String docs(String... documents) {
        StringBuilder sb = new StringBuilder();
        for (String d : documents) {
            sb.append("{\"index\": {}}\n").append(d).append("\n");
        }
        return sb.toString();
    }

    private void bulkIndex(String ndjson) throws Exception {
        Request req = new Request("POST", "/" + INDEX + "/_bulk");
        req.setJsonEntity(ndjson);
        req.addParameter("refresh", "true");
        req.setOptions(req.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Map<String, Object> response = assertOkAndParse(client().performRequest(req), "Bulk index");
        assertEquals("Bulk indexing should have no errors", false, response.get("errors"));
    }

    private void flush() throws Exception {
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
    }

    // ── PPL helpers ─────────────────────────────────────────────────────────

    private Map<String, Object> executePPL(String ppl) throws IOException {
        Request req = new Request("POST", "/_analytics/ppl");
        req.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(req);
        return assertOkAndParse(response, "PPL: " + ppl);
    }

    private long countOf(String pplSuffix) throws IOException {
        String ppl = "source = " + INDEX + " | " + pplSuffix;
        Map<String, Object> result = executePPL(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("Response missing 'rows' for: " + ppl, rows);
        assertEquals("Expected 1 row for count query: " + ppl, 1, rows.size());
        return ((Number) rows.get(0).get(0)).longValue();
    }

    private void assertCount(String pplSuffix, long expected) throws IOException {
        String ppl = "source = " + INDEX + " | " + pplSuffix;
        long actual = countOf(pplSuffix);
        assertEquals("Count mismatch for: " + ppl, expected, actual);
    }
}
