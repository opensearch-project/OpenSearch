/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.After;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST integration test for the DSL sub-plan fan-out on a live cluster: it drives
 * {@code dsl.query.max_parallel_sub_plans} over
 * {@code _cluster/settings} and asserts that <b>widening the fan-out never changes the answer</b>.
 */
public class DslFanOutIT extends AnalyticsRestTestCase {

    private static final String INDEX = "dsl_fanout_it";

    /** The only setting the fan-out reads; there is one execution shape and no setting for it. */
    private static final String WIDTH_SETTING = "dsl.query.max_parallel_sub_plans";

    /** The width setting's upper bound, mirroring {@code SubPlanParallelism.MAX_K_SETTING}. */
    private static final int CEILING = 5;

    /**
     * Plain search: no aggregations, {@code size > 0}. The {@code term} filter is load-bearing — it makes the
     * expected total 7 rather than the index's 10, so a dropped filter cannot pass as a correct answer.
     */
    private static final String PLAIN_SEARCH = "{\"size\": 5, \"query\": {\"term\": {\"region\": \"east\"}}}";

    /**
     * {region, service, latency}. Every doc_count that a bucket ordering depends on is distinct (7/3 at the
     * root, 4/3 and 2/1 within the two parents), so the default {@code doc_count desc} order is fully
     * determined. {@code latency} is an {@code integer} field whose per-bucket sums exceed
     * {@code Integer.MAX_VALUE}: the engine accumulates a widened sum in Int64, and an accumulator that
     * stayed 32-bit wraps to a negative number here instead of returning a wrong-but-plausible total.
     */
    private static final String[][] DOCS = {
        { "east", "api", "600000000" },
        { "east", "api", "600000000" },
        { "east", "api", "600000000" },
        { "east", "api", "600000000" },
        { "east", "db", "100000000" },
        { "east", "db", "100000000" },
        { "east", "db", "100000000" },
        { "west", "api", "700000000" },
        { "west", "api", "700000000" },
        { "west", "cache", "50000000" } };

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            createFanOutIndex();
            ingestDocs();
            dataProvisioned = true;
        }
    }

    /**
     * Clears the width knob. {@link #preserveClusterUponCompletion()} is true for this suite, so nothing in
     * the framework clears cluster settings for us and a leaked value would reach whatever runs next. Kept as
     * a loop over keys so adding a second knob cannot silently skip clearing it, and so a mid-test failure —
     * a failed assertion, a 500 out of a search — still leaves the cluster at its defaults.
     */
    @After
    public void clearFanOutSettings() {
        List<String> failures = new ArrayList<>();
        for (String key : List.of(WIDTH_SETTING)) {
            try {
                Response response = putTransientSetting(key, "null");
                int status = response.getStatusLine().getStatusCode();
                if (status != 200) {
                    failures.add(key + " -> HTTP " + status);
                }
            } catch (Exception e) {
                failures.add(key + " -> " + e);
            }
        }
        if (failures.isEmpty() == false) {
            fail("Failed to clear the DSL fan-out cluster settings; a later test would inherit them: " + failures);
        }
    }

    // ── Fan-out parity ─────────────────────────────────────────────────────

    /**
     * A 2-plan nested aggregation — the common production shape, and where a width of 2 first becomes
     * reachable. Repeated at width 2 because a fan-out
     * race — a torn collector slot, one plan's rows landing in another plan's slot — need not reproduce on
     * the first attempt.
     */
    public void testNestedAggregationWidth2MatchesWidth1() throws Exception {
        assertNestedAggregationWidthParity(nestedAggregationBody(0), 3, "2 plans");
    }



    /** The widest arrangement available: all 3 plans gated, so nothing runs alone first. */
    public void testHitsAndNestedAggregationWidth2MatchesWidth1() throws Exception {
        assertNestedAggregationWidthParity(nestedAggregationBody(5), 2, "3 plans");
    }

    // ── The aggregation-only gate ──────────────────────────────────────────

    /**
     * An ordinary search still answers correctly with the knob turned up. Its main batch is one HITS plan,
     * so it never reaches the width decision at all — which is exactly the claim: an operator who widens the
     * fan-out for the aggregations must not change what a plain search returns, and must not turn its engine
     * call into a concurrent one.
     */
    public void testPlainSearchIsUnaffectedByTheFanOutWidth() throws Exception {
        String baseline = "plain search at width=1";
        Map<String, Object> sequential = dslSearch(PLAIN_SEARCH, 1, baseline);
        assertEquals(baseline + ": hits.total.value", 7L, totalHits(sequential, baseline));
        assertNull(baseline + ": a request with no aggs must carry no aggregations", sequential.get("aggregations"));

        String widened = "plain search at width=2";
        Map<String, Object> fannedOut = dslSearch(PLAIN_SEARCH, 2, widened);
        assertEquals(widened + ": hits.total.value", 7L, totalHits(fannedOut, widened));
        assertNull(widened + ": a request with no aggs must carry no aggregations", fannedOut.get("aggregations"));

        assertEquals(
            "the widest fan-out setting must answer a plain search exactly as the default does",
            stableView(sequential),
            stableView(fannedOut)
        );
    }

    // ── The knobs' own contracts ───────────────────────────────────────────

    /** A width above the ceiling is a 400, never a silent clamp to something inside it. */
    public void testWidthAboveTheMaximumIsRejected() throws Exception {
        ResponseException e = expectThrows(ResponseException.class, () -> putTransientSetting(WIDTH_SETTING, String.valueOf(CEILING + 1)));
        assertEquals("a width above the ceiling must be a client error", 400, e.getResponse().getStatusLine().getStatusCode());
        String body = EntityUtils.toString(e.getResponse().getEntity());
        assertTrue("rejection should name the setting, got: " + body, body.contains(WIDTH_SETTING));
        assertTrue("rejection should name the maximum, got: " + body, body.contains("must be <= " + CEILING));
    }


    // ── Parity harness ─────────────────────────────────────────────────────

    /**
     * Runs {@code body} once at width 1, then {@code repeats} times at width 2, and asserts every response
     * carries the expected buckets and matches the width-1 response field for field. Width is the only
     * variable: same body, same index, same cluster.
     *
     * @param body the search body, unchanged across widths
     * @param repeats how many times to re-run the fanned-out width
     * @param context short description of the shape, used in failure messages
     */
    private void assertNestedAggregationWidthParity(String body, int repeats, String context) throws IOException {
        String baseline = context + " at width=1";
        Map<String, Object> sequential = dslSearch(body, 1, baseline);
        assertExpectedNestedAggregation(sequential, baseline);

        for (int attempt = 1; attempt <= repeats; attempt++) {
            String widened = context + " at width=2 (attempt " + attempt + ")";
            Map<String, Object> fannedOut = dslSearch(body, 2, widened);
            // Both halves matter: the absolute values catch a width that corrupted the result the same way
            // every time, the comparison catches a width that answered differently from the sequential path.
            assertExpectedNestedAggregation(fannedOut, widened);
            assertEquals(widened + ": must match the width=1 response", stableView(sequential), stableView(fannedOut));
        }
    }

    /**
     * Every bucket value {@link #DOCS} implies, asserted absolutely rather than only against another run —
     * two runs that are wrong in the same way still agree with each other.
     */
    private void assertExpectedNestedAggregation(Map<String, Object> response, String context) {
        Map<String, Object> aggregations = aggregations(response, context);
        List<Map<String, Object>> regions = buckets(aggregations, "by_region", context);
        assertEquals(context + ": by_region bucket count", 2, regions.size());
        // The terms size (10) is above the distinct region count, so nothing was truncated: the COUNT plan's
        // eligible-doc total must exactly cover the buckets returned. A non-zero value means docs went
        assertEquals(context + ": by_region sum_other_doc_count", 0L, sumOtherDocCount(aggregations, "by_region"));

        Map<String, Object> east = regions.get(0);
        assertEquals(context + ": first region key", "east", east.get("key"));
        assertEquals(context + ": east doc_count", 7L, docCount(east));

        List<Map<String, Object>> eastServices = buckets(east, "by_service", context);
        assertEquals(context + ": east by_service bucket count", 2, eastServices.size());
        assertEquals(context + ": east first service key", "api", eastServices.get(0).get("key"));
        assertEquals(context + ": east/api doc_count", 4L, docCount(eastServices.get(0)));
        assertEquals(context + ": east/api total_latency", 2_400_000_000.0, metricValue(eastServices.get(0), "total_latency"), 0.0);
        assertEquals(context + ": east second service key", "db", eastServices.get(1).get("key"));
        assertEquals(context + ": east/db doc_count", 3L, docCount(eastServices.get(1)));
        assertEquals(context + ": east/db total_latency", 300_000_000.0, metricValue(eastServices.get(1), "total_latency"), 0.0);

        Map<String, Object> west = regions.get(1);
        assertEquals(context + ": second region key", "west", west.get("key"));
        assertEquals(context + ": west doc_count", 3L, docCount(west));

        List<Map<String, Object>> westServices = buckets(west, "by_service", context);
        assertEquals(context + ": west by_service bucket count", 2, westServices.size());
        assertEquals(context + ": west first service key", "api", westServices.get(0).get("key"));
        assertEquals(context + ": west/api doc_count", 2L, docCount(westServices.get(0)));
        assertEquals(context + ": west/api total_latency", 1_400_000_000.0, metricValue(westServices.get(0), "total_latency"), 0.0);
        assertEquals(context + ": west second service key", "cache", westServices.get(1).get("key"));
        assertEquals(context + ": west/cache doc_count", 1L, docCount(westServices.get(1)));
        assertEquals(context + ": west/cache total_latency", 50_000_000.0, metricValue(westServices.get(1), "total_latency"), 0.0);

        // Every doc is accounted for regardless of the request's `size`, so the hits total is comparable
        // across the size=0 and size>0 shapes of this same aggregation.
        assertEquals(context + ": hits.total.value", (long) DOCS.length, totalHits(response, context));
    }

    /**
     * The comparable part of a search response. {@code took} is a wall-clock measurement and is the one field
     * two runs of the same request are expected to disagree on; everything else — hits total, shard counts,
     * every bucket and every metric — must match, so it is compared rather than enumerated.
     */
    private static Map<String, Object> stableView(Map<String, Object> response) {
        Map<String, Object> comparable = new HashMap<>(response);
        comparable.remove("took");
        return comparable;
    }

    // ── Request helpers ────────────────────────────────────────────────────

    /**
     * The 2-level terms aggregation, parameterised only by the request's {@code size} so the HITS plan is the
     * single difference between the 2-plan and 3-plan shapes.
     *
     * @param size the request size; 0 emits no HITS plan
     * @return the search body as JSON
     */
    private static String nestedAggregationBody(int size) {
        return "{"
            + "\"size\": " + size + ","
            + "\"aggs\": {"
            + "  \"by_region\": {"
            + "    \"terms\": { \"field\": \"region\", \"size\": 10 },"
            + "    \"aggs\": {"
            + "      \"by_service\": {"
            + "        \"terms\": { \"field\": \"service\", \"size\": 10 },"
            + "        \"aggs\": { \"total_latency\": { \"sum\": { \"field\": \"latency\" } } }"
            + "      }"
            + "    }"
            + "  }"
            + "}"
            + "}";
    }

    /** Sets both knobs, then runs {@code body} against the index and asserts HTTP 200. */
    private Map<String, Object> dslSearch(String body, int width, String context) throws IOException {
        setFanOutWidth(width);
        Request request = new Request("POST", "/" + INDEX + "/_search");
        request.setJsonEntity(body);
        return assertOkAndParse(client().performRequest(request), context);
    }

    /**
     * One PUT per key, mirroring {@link #clearFanOutSettings()}. No polling afterwards is needed and none is
     * used: a {@code _cluster/settings} update is acked only once every node has applied the new cluster
     * state, and each node's settings-update consumer runs during that application — so a search issued
     * after this returns is guaranteed to read the values just set.
     */
    private void setFanOutWidth(int width) throws IOException {
        assertOkAndParse(putTransientSetting(WIDTH_SETTING, String.valueOf(width)), "PUT width=" + width);
    }

    /**
     * @param key the setting key
     * @param jsonValue the value as a JSON token — a bare number, a quoted string, or {@code null} to clear
     * @return the raw response; a rejected value arrives as a {@link ResponseException}, not as a status code
     */
    private Response putTransientSetting(String key, String jsonValue) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\": {\"" + key + "\": " + jsonValue + "}}");
        return client().performRequest(request);
    }

    // ── Response readers ───────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static Map<String, Object> aggregations(Map<String, Object> response, String context) {
        Map<String, Object> aggregations = (Map<String, Object>) response.get("aggregations");
        assertNotNull(context + ": response carries no aggregations", aggregations);
        return aggregations;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> buckets(Map<String, Object> owner, String aggName, String context) {
        Map<String, Object> agg = (Map<String, Object>) owner.get(aggName);
        assertNotNull(context + ": aggregation [" + aggName + "] missing, present keys " + owner.keySet(), agg);
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) agg.get("buckets");
        assertNotNull(context + ": aggregation [" + aggName + "] carries no buckets", buckets);
        return buckets;
    }

    @SuppressWarnings("unchecked")
    private static long sumOtherDocCount(Map<String, Object> owner, String aggName) {
        Map<String, Object> agg = (Map<String, Object>) owner.get(aggName);
        return ((Number) agg.get("sum_other_doc_count")).longValue();
    }

    private static long docCount(Map<String, Object> bucket) {
        return ((Number) bucket.get("doc_count")).longValue();
    }

    @SuppressWarnings("unchecked")
    private static double metricValue(Map<String, Object> bucket, String metricName) {
        Map<String, Object> metric = (Map<String, Object>) bucket.get(metricName);
        assertNotNull("metric [" + metricName + "] missing from bucket [" + bucket.get("key") + "]", metric);
        return ((Number) metric.get("value")).doubleValue();
    }

    /**
     * {@code hits.total.value}, asserting the relation is exact on the way past. Hit <em>documents</em> are
     * still stubbed on this branch ({@code SearchResponseBuilder.buildHits} returns an empty
     * {@code SearchHit[]}), so the total is the only part of the hits section there is anything true to
     * assert about; asserting on {@code hits.hits} would be asserting a behaviour that does not exist yet.
     */
    @SuppressWarnings("unchecked")
    private static long totalHits(Map<String, Object> response, String context) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        assertNotNull(context + ": response carries no hits section", hits);
        Map<String, Object> total = (Map<String, Object>) hits.get("total");
        assertNotNull(context + ": hits section carries no total", total);
        assertEquals(context + ": hits.total.relation", "eq", total.get("relation"));
        return ((Number) total.get("value")).longValue();
    }

    // ── Index setup ────────────────────────────────────────────────────────

    /**
     * Two shards on purpose: a multi-shard aggregation is the shape that goes through the engine's
     * PARTIAL/FINAL exchange, so the fan-out is measured against the distributed path rather than a
     * single-shard shortcut.
     */
    private void createFanOutIndex() throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {
            // index may not exist
        }

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 2,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": \"lucene\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"region\": { \"type\": \"keyword\" },"
            + "    \"service\": { \"type\": \"keyword\" },"
            + "    \"latency\": { \"type\": \"integer\" }"
            + "  }"
            + "}"
            + "}";

        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(create), "Create index");
        assertEquals("Index creation should be acknowledged", true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
        logger.info("Created fan-out index [{}] with 2 shards", INDEX);
    }

    private void ingestDocs() throws IOException {
        StringBuilder ndjson = new StringBuilder();
        for (String[] doc : DOCS) {
            // No explicit _id: index.append_only.enabled rejects one per item on this index (see DOCS).
            ndjson.append("{\"index\": {}}\n");
            ndjson.append("{\"region\": \"").append(doc[0]).append("\"")
                .append(", \"service\": \"").append(doc[1]).append("\"")
                .append(", \"latency\": ").append(doc[2])
                .append("}\n");
        }

        Request bulk = new Request("POST", "/" + INDEX + "/_bulk");
        bulk.setJsonEntity(ndjson.toString());
        bulk.addParameter("refresh", "true");
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Map<String, Object> response = assertOkAndParse(client().performRequest(bulk), "Bulk index");
        // A _bulk that reports errors answers HTTP 200 with per-item statuses, so the only way to learn what
        // was rejected is to print the items. Failing on a bare boolean tells a reader nothing.
        assertEquals("Bulk indexing reported item errors: " + response.get("items"), false, response.get("errors"));
        logger.info("Indexed {} documents into [{}]", DOCS.length, INDEX);
    }
}
