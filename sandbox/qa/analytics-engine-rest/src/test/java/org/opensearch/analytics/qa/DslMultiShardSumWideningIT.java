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
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * End-to-end guard for the DSL type system's SUM widening on a <b>2-shard</b> index:
 * {@code DslTypeSystems.NANO_TIMESTAMP.deriveSumType} maps TINYINT/SMALLINT/INTEGER/BIGINT to
 * {@code BIGINT} and REAL/FLOAT/DOUBLE to {@code DOUBLE}, because the DataFusion backend accumulates
 * every signed-integer sum in {@code Int64} and every floating-point sum in {@code Float64}.
 */
public class DslMultiShardSumWideningIT extends AnalyticsRestTestCase {

    private static final String INDEX = "dsl_sum_widening_2shard";

    /**
     * A 1-shard control index holding the same 8 documents. Its only job is to say whether an answer that
     * is wrong at 2 shards is wrong because of the shard count: a single shard keeps the aggregate SINGLE,
     * so no PARTIAL/FINAL split and no exchange are involved, and anything still wrong here is wrong
     * everywhere rather than only on a multi-shard index.
     */
    private static final String ONE_SHARD_INDEX = "dsl_sum_widening_1shard";

    private static final int DOCS_PER_TENANT = 4;
    private static final int TOTAL_DOCS = 2 * DOCS_PER_TENANT;

    // integer-mapped column. Both subtotals and the grand total exceed Integer.MAX_VALUE (2147483647),
    // so no asserted value below is reachable by a 32-bit accumulator. Every individual document value
    private static final long ALPHA_INT_TOTAL = 4_000_000_000L; // 1.3e9 + 1.1e9 + 0.9e9 + 0.7e9
    private static final long BETA_INT_TOTAL = 2_400_000_000L;  // 1.0e9 + 0.8e9 + 0.4e9 + 0.2e9
    private static final long INT_TOTAL = ALPHA_INT_TOTAL + BETA_INT_TOTAL; // 6_400_000_000
    private static final long INT_MEAN = INT_TOTAL / TOTAL_DOCS; // 800_000_000
    private static final long INT_LOWEST = 200_000_000L;
    private static final long INT_HIGHEST = 1_300_000_000L;

    // float-mapped column. Every document value is a power of two, so it is exact in f32 and the value
    // DataFusion reads back is the literal that was indexed; every total is a sum of powers of two well
    private static final double ALPHA_FLOAT_TOTAL = 2_684_354_560.0d; // 2^30 + 3 * 2^29
    private static final double BETA_FLOAT_TOTAL = 3_221_225_472.0d;  // 2 * 2^30 + 2 * 2^29
    private static final double FLOAT_TOTAL = ALPHA_FLOAT_TOTAL + BETA_FLOAT_TOTAL; // 5_905_580_032
    private static final double ALPHA_FLOAT_MEAN = ALPHA_FLOAT_TOTAL / DOCS_PER_TENANT; // 671_088_640
    private static final double BETA_FLOAT_MEAN = BETA_FLOAT_TOTAL / DOCS_PER_TENANT;   // 805_306_368
    private static final double FLOAT_MEAN = FLOAT_TOTAL / TOTAL_DOCS;                  // 738_197_504

    // A second integer-mapped column whose totals stay far below Integer.MAX_VALUE, so nothing about it
    // can be explained by an accumulator width. Its means are deliberately NOT whole numbers: an avg
    private static final double SMALL_TOTAL = 111.0d;                 // 1+2+3+4 + 10+20+30+41
    private static final double SMALL_MEAN = SMALL_TOTAL / TOTAL_DOCS; // 13.875, exact in IEEE-754
    private static final double ALPHA_SMALL_MEAN = 2.5d;               // (1+2+3+4) / 4
    private static final double BETA_SMALL_MEAN = 25.25d;              // (10+20+30+41) / 4

    /**
     * 8 documents, 4 per tenant, with auto-generated ids so they spread across both shards by the
     * default {@code _id} hash. The per-document values are spelled out rather than generated so the
     * expected totals above can be added up by hand.
     */
    private static final String BULK_BODY = "{\"index\":{}}\n"
        + "{\"tenant\":\"alpha\",\"int_amount\":1300000000,\"float_amount\":1073741824.0,\"small_amount\":1}\n"
        + "{\"index\":{}}\n"
        + "{\"tenant\":\"alpha\",\"int_amount\":1100000000,\"float_amount\":536870912.0,\"small_amount\":2}\n"
        + "{\"index\":{}}\n"
        + "{\"tenant\":\"alpha\",\"int_amount\":900000000,\"float_amount\":536870912.0,\"small_amount\":3}\n"
        + "{\"index\":{}}\n"
        + "{\"tenant\":\"alpha\",\"int_amount\":700000000,\"float_amount\":536870912.0,\"small_amount\":4}\n"
        + "{\"index\":{}}\n"
        + "{\"tenant\":\"beta\",\"int_amount\":1000000000,\"float_amount\":1073741824.0,\"small_amount\":10}\n"
        + "{\"index\":{}}\n"
        + "{\"tenant\":\"beta\",\"int_amount\":800000000,\"float_amount\":1073741824.0,\"small_amount\":20}\n"
        + "{\"index\":{}}\n"
        + "{\"tenant\":\"beta\",\"int_amount\":400000000,\"float_amount\":536870912.0,\"small_amount\":30}\n"
        + "{\"index\":{}}\n"
        + "{\"tenant\":\"beta\",\"int_amount\":200000000,\"float_amount\":536870912.0,\"small_amount\":41}\n";

    private static volatile boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned == false) {
            provision();
            provisioned = true;
        }
    }

    // ── Preconditions the rest of the class depends on ──────────────────────────────

    /**
     * The two properties that make every other test in this class meaningful: the index really has 2
     * shards (1 shard keeps the aggregate SINGLE and the defect is unreachable) with parquet as the
     * primary data format, and all 8 documents are visible to the query.
     */
    public void testTwoShardParquetIndexHoldsEveryDocument() throws Exception {
        Response settingsResponse = client().performRequest(new Request("GET", "/" + INDEX + "/_settings"));
        Map<String, Object> settingsBody = assertOkAndParse(settingsResponse, "GET /" + INDEX + "/_settings");
        Map<String, Object> index = asObject(asObject(asObject(settingsBody.get(INDEX)).get("settings")).get("index"));

        assertEquals(
            "number_of_shards must stay 2 — OpenSearchAggregateSplitRule only splits the aggregate into "
                + "PARTIAL/FINAL at 2+ shards, and without that split there is no exchange to reject a narrow sum",
            "2",
            index.get("number_of_shards")
        );
        assertEquals(
            "parquet must be the primary data format — the Int64/Float64 accumulator widths this class pins are "
                + "DataFusion's, and the DataFusion path is what a parquet-primary composite index takes",
            "parquet",
            asObject(index.get("composite")).get("primary_data_format")
        );

        Request search = new Request("POST", "/" + INDEX + "/_search");
        search.setJsonEntity("{\"size\": 0, \"track_total_hits\": true}");
        Map<String, Object> searchBody = assertOkAndParse(client().performRequest(search), "hits.total on " + INDEX);
        Map<String, Object> total = asObject(asObject(searchBody.get("hits")).get("total"));
        assertEquals(
            "every indexed document must be visible, otherwise the exact totals asserted by the other tests are unreachable",
            TOTAL_DOCS,
            ((Number) total.get("value")).intValue()
        );
    }

    // ── Root-level metrics: one no-GROUP-BY plan, one exchange ─────────────────────

    /**
     * The written {@code sum} over the {@code integer}-mapped column, on its own. This is the aggregation
     * whose declared type {@code AggregationMetadataBuilder} reconciles, and the total exceeds
     * {@code Integer.MAX_VALUE}, so a 32-bit declaration cannot produce it.
     */
    public void testIntegerSumExceedsIntMaxAcrossTwoShards() throws Exception {
        Map<String, Object> aggs = aggregations(searchAggregations("{\"int_total\": {\"sum\": {\"field\": \"int_amount\"}}}"));

        assertEquals(
            "sum(int_amount) over " + TOTAL_DOCS + " docs on a 2-shard index must be the exact 64-bit total; an "
                + "Int32-declared sum that survives the PARTIAL/FINAL exchange wraps it to " + (int) INT_TOTAL,
            (double) INT_TOTAL,
            metricValue(aggs, "int_total"),
            0.0d
        );
    }

    /**
     * {@code avg} over the {@code integer}-mapped column, whose <em>intermediate</em> sum
     * (6,400,000,000) exceeds {@code Integer.MAX_VALUE} while the mean it produces (800,000,000) fits an
     * {@code int} comfortably. That gap is the point: only the rule-generated intermediate is oversized,
     * so this test can only fail on how the intermediate is typed.
     */
    public void testIntegerAvgWhoseIntermediateSumExceedsIntMax() throws Exception {
        Map<String, Object> aggs = aggregations(searchAggregations("{\"int_mean\": {\"avg\": {\"field\": \"int_amount\"}}}"));

        assertEquals(
            "avg(int_amount) must be the exact mean. Its own result fits an int, so a failure here is about the "
                + "rule-generated intermediate SUM (" + INT_TOTAL + "), not about the mean",
            (double) INT_MEAN,
            metricValue(aggs, "int_mean"),
            0.0d
        );
    }

    /**
     * {@code avg} over an integer column whose every total stays far below {@code Integer.MAX_VALUE}, so
     * no accumulator width is involved and the only thing under test is the <em>width of the quotient</em>:
     * the true mean of {@code small_amount} is 13.875, which is not an integer.
     */
    public void testIntegerAvgIsNotTruncatedToAnIntegerAcrossTwoShards() throws Exception {
        Map<String, Object> aggs = aggregations(searchAggregations("{\"small_mean\": {\"avg\": {\"field\": \"small_amount\"}}}"));

        assertEquals(
            "avg(small_amount) over " + TOTAL_DOCS + " docs must be the true fractional mean " + SMALL_MEAN + " (total "
                + SMALL_TOTAL + "); " + (long) SMALL_MEAN + " would mean the quotient was declared at the column's own "
                + "integer width and truncated",
            SMALL_MEAN,
            metricValue(aggs, "small_mean"),
            0.0d
        );
    }

    /**
     * The same pair over the {@code float}-mapped column, which Calcite types {@code REAL} (Float32)
     * against DataFusion's Float64 accumulator. Its failure mode is the exchange-schema rejection rather
     * than a wrap, so the value assertion here is what proves the total survived the exchange intact.
     */
    public void testFloatSumAndAvgAcrossTwoShards() throws Exception {
        Map<String, Object> aggs = aggregations(
            searchAggregations(
                "{\"float_total\": {\"sum\": {\"field\": \"float_amount\"}},"
                    + " \"float_mean\": {\"avg\": {\"field\": \"float_amount\"}}}"
            )
        );

        assertEquals(
            "sum(float_amount): a float-mapped column is Calcite REAL (Float32) while DataFusion accumulates in "
                + "Float64, so deriveSumType must widen REAL to DOUBLE or the exchange input is rejected",
            FLOAT_TOTAL,
            metricValue(aggs, "float_total"),
            0.0d
        );
        assertEquals(
            "avg(float_amount) reduces to a rule-generated SUM/COUNT pair over the same Float32 column",
            FLOAT_MEAN,
            metricValue(aggs, "float_mean"),
            0.0d
        );
    }

    /**
     * Root-level metrics share one no-GROUP-BY plan, so this single request puts a widened BIGINT sum, a
     * widened DOUBLE sum and two <em>un</em>widened metrics in the same aggregate and the same exchange.
     */
    public void testWidenedAndUnwidenedMetricsShareOneExchangeAcrossTwoShards() throws Exception {
        Map<String, Object> aggs = aggregations(
            searchAggregations(
                "{\"int_total\": {\"sum\": {\"field\": \"int_amount\"}},"
                    + " \"int_low\": {\"min\": {\"field\": \"int_amount\"}},"
                    + " \"int_high\": {\"max\": {\"field\": \"int_amount\"}},"
                    + " \"float_total\": {\"sum\": {\"field\": \"float_amount\"}}}"
            )
        );

        assertEquals(
            "sum(int_amount) alongside unwidened metrics must still be the exact 64-bit total, not " + (int) INT_TOTAL,
            (double) INT_TOTAL,
            metricValue(aggs, "int_total"),
            0.0d
        );
        assertEquals(
            "min(int_amount) keeps the column's own width — deriveSumType must not touch it",
            (double) INT_LOWEST,
            metricValue(aggs, "int_low"),
            0.0d
        );
        assertEquals(
            "max(int_amount) keeps the column's own width — deriveSumType must not touch it",
            (double) INT_HIGHEST,
            metricValue(aggs, "int_high"),
            0.0d
        );
        assertEquals(
            "sum(float_amount) sharing an exchange with an Int64 sum and two Int32 extrema",
            FLOAT_TOTAL,
            metricValue(aggs, "float_total"),
            0.0d
        );
    }

    // ── 1-shard controls: is the shard count what makes an answer wrong? ───────────

    /**
     * {@code sum} over the {@code integer}-mapped column on the <b>1-shard</b> control index. This is the
     * shape the widening is <em>not</em> needed for — a single shard keeps the aggregate SINGLE, so nothing
     * crosses an exchange — and asserting the same total here is what turns
     * {@link #testIntegerSumExceedsIntMaxAcrossTwoShards} into a statement about the split rule: the shard
     * count must not change the answer.
     */
    public void testOneShardIntegerSumMatchesTheTwoShardTotal() throws Exception {
        Map<String, Object> aggs = aggregations(
            searchAggregationsOn(ONE_SHARD_INDEX, "{\"int_total\": {\"sum\": {\"field\": \"int_amount\"}}}")
        );

        assertEquals(
            "sum(int_amount) must be the same exact total at 1 shard as at 2 — the PARTIAL/FINAL split is an "
                + "execution detail, not an arithmetic one",
            (double) INT_TOTAL,
            metricValue(aggs, "int_total"),
            0.0d
        );
    }

    /**
     * {@code avg} over the small integer column on the <b>1-shard</b> control index. This is the assertion
     * that decides the blast radius of a truncated mean: if it is wrong here too, then the shard count and
     * the exchange have nothing to do with it and every {@code avg} over an integer-mapped field is
     * affected, not only the ones on a multi-shard index.
     */
    public void testOneShardIntegerAvgIsNotTruncated() throws Exception {
        Map<String, Object> aggs = aggregations(
            searchAggregationsOn(ONE_SHARD_INDEX, "{\"small_mean\": {\"avg\": {\"field\": \"small_amount\"}}}")
        );

        assertEquals(
            "avg(small_amount) on a 1-shard index must be the true fractional mean " + SMALL_MEAN + "; the same wrong "
                + "value here and at 2 shards means the defect is in AVG's declared type, not in the split",
            SMALL_MEAN,
            metricValue(aggs, "small_mean"),
            0.0d
        );
    }

    // ── Grouped plan: the sum crosses the exchange inside a GROUP BY ───────────────

    /**
     * {@code terms(tenant)} with sub-{@code sum}, so the widened sums cross the exchange inside a grouped
     * plan (a non-empty groupSet, PARTIAL keyed by the term) rather than the flat single-row one. Both
     * per-tenant integer subtotals also exceed {@code Integer.MAX_VALUE}, so the wrap is caught per bucket
     * and not only in aggregate. {@code avg(float_amount)} rides along because the float column's avg does
     * survive the reduce, so it pins the grouped avg path that works.
     */
    public void testTermsSubSumsCrossTheExchangeInAGroupedPlan() throws Exception {
        Map<String, Object> body = searchAggregations(
            "{\"by_tenant\": {\"terms\": {\"field\": \"tenant\"},"
                + " \"aggregations\": {"
                + "   \"int_total\": {\"sum\": {\"field\": \"int_amount\"}},"
                + "   \"float_total\": {\"sum\": {\"field\": \"float_amount\"}},"
                + "   \"float_mean\": {\"avg\": {\"field\": \"float_amount\"}}}}}"
        );

        // Keyed by term rather than read positionally: both tenants hold 4 docs, so pinning the default
        // count-descending order would pin a tie-break instead of the arithmetic under test.
        Map<String, Map<String, Object>> buckets = bucketsByKey(body, "by_tenant");
        assertEquals("terms(tenant) must return exactly the two indexed tenants, got " + buckets.keySet(), 2, buckets.size());
        assertTenantBucket(buckets, "alpha", ALPHA_INT_TOTAL, ALPHA_FLOAT_TOTAL, ALPHA_FLOAT_MEAN);
        assertTenantBucket(buckets, "beta", BETA_INT_TOTAL, BETA_FLOAT_TOTAL, BETA_FLOAT_MEAN);
    }

    /**
     * {@code terms(tenant)} with a sub-{@code avg} over the small integer column, so the quotient's width
     * is pinned inside a grouped plan too. Both per-tenant means are fractional (2.5 and 25.25), and
     * neither is reachable by truncation of the other, so a bucket that came back whole is a truncation
     * and not a mislabelled bucket.
     */
    public void testTermsSubAvgIsNotTruncatedInAGroupedPlan() throws Exception {
        Map<String, Object> body = searchAggregations(
            "{\"by_tenant\": {\"terms\": {\"field\": \"tenant\"},"
                + " \"aggregations\": { \"small_mean\": {\"avg\": {\"field\": \"small_amount\"}}}}}"
        );

        Map<String, Map<String, Object>> buckets = bucketsByKey(body, "by_tenant");
        assertEquals("terms(tenant) must return exactly the two indexed tenants, got " + buckets.keySet(), 2, buckets.size());
        assertEquals(
            "bucket [alpha] avg(small_amount) must be the true fractional mean, not a truncated 2",
            ALPHA_SMALL_MEAN,
            metricValue(bucketOf(buckets, "alpha"), "small_mean"),
            0.0d
        );
        assertEquals(
            "bucket [beta] avg(small_amount) must be the true fractional mean, not a truncated 25",
            BETA_SMALL_MEAN,
            metricValue(bucketOf(buckets, "beta"), "small_mean"),
            0.0d
        );
    }

    private static void assertTenantBucket(
        Map<String, Map<String, Object>> buckets,
        String tenant,
        long intTotal,
        double floatTotal,
        double floatMean
    ) {
        Map<String, Object> bucket = bucketOf(buckets, tenant);
        assertEquals(
            "bucket [" + tenant + "] must hold every document of its tenant",
            DOCS_PER_TENANT,
            ((Number) bucket.get("doc_count")).intValue()
        );
        assertEquals(
            "bucket [" + tenant + "] sum(int_amount) crosses the exchange inside a GROUP BY plan and still exceeds "
                + "Integer.MAX_VALUE; an Int32-declared sum wraps it to " + (int) intTotal,
            (double) intTotal,
            metricValue(bucket, "int_total"),
            0.0d
        );
        assertEquals("bucket [" + tenant + "] sum(float_amount)", floatTotal, metricValue(bucket, "float_total"), 0.0d);
        assertEquals("bucket [" + tenant + "] avg(float_amount)", floatMean, metricValue(bucket, "float_mean"), 0.0d);
    }

    private static Map<String, Object> bucketOf(Map<String, Map<String, Object>> buckets, String tenant) {
        Map<String, Object> bucket = buckets.get(tenant);
        assertNotNull("terms(tenant) is missing bucket [" + tenant + "], got " + buckets.keySet(), bucket);
        return bucket;
    }

    // ── Request / response helpers ─────────────────────────────────────────────────

    /**
     * Runs {@code POST /{index}/_search} with {@code size: 0} and the given aggregation tree, asserting
     * HTTP 200. A non-2xx status arrives as a {@link ResponseException}, so it is turned into a failure
     * that names the two 500s this index is known to be able to produce — a future reader who sees this
     * test go red should not have to rediscover what a 500 here means. The node's own log carries the real
     * cause; the REST body is only {@code Internal error [task_id=N]}.
     */
    private Map<String, Object> searchAggregations(String aggregationsJson) throws IOException {
        return searchAggregationsOn(INDEX, aggregationsJson);
    }

    /** {@link #searchAggregations} against a named index, so the 1-shard control can share the diagnostics. */
    private Map<String, Object> searchAggregationsOn(String index, String aggregationsJson) throws IOException {
        Request request = new Request("POST", "/" + index + "/_search");
        request.setJsonEntity("{\"size\": 0, \"aggregations\": " + aggregationsJson + "}");
        Response response;
        try {
            response = client().performRequest(request);
        } catch (ResponseException e) {
            throw new AssertionError(
                "DSL _search on the index "
                    + index
                    + " did not answer HTTP 200 for aggregations "
                    + aggregationsJson
                    + ".\nThe REST body only carries [Internal error [task_id=N]] — grep the node log for that task id. "
                    + "Two causes are known:\n"
                    + "(1) [Arrow error: Cast error: Can't cast value <total> to type Int32] out of "
                    + "DatafusionReduceSink.reduce, for an aggregations tree containing avg over an integer-mapped "
                    + "column. DslTypeSystems.NANO_TIMESTAMP overrides deriveSumType but not deriveAvgAggType, so AVG "
                    + "is declared at the column's own Int32 while OpenSearchAggregateReduceRule rewrites it into "
                    + "SUM/COUNT/DIVIDE/CAST and casts the correctly widened Int64 intermediate back to that Int32.\n"
                    + "(2) [Failed to create exchange sink for stageId=1] or [Substrait schema has a different type "
                    + "(Int32) than the corresponding field in the table schema (Int64)] — then SUM is no longer "
                    + "declared at the width the engine accumulates in: DslTypeSystems.deriveSumType must widen "
                    + "TINYINT/SMALLINT/INTEGER/BIGINT to BIGINT and REAL/FLOAT/DOUBLE to DOUBLE. At 2+ shards "
                    + "OpenSearchAggregateSplitRule splits the aggregate into PARTIAL/FINAL and the coordinator "
                    + "registers the exchange input with the PARTIAL's lowered (Int64/Float64) schema, so a FINAL whose "
                    + "Substrait declares Calcite's narrower view is rejected.\n"
                    + e.getMessage(),
                e
            );
        }
        return assertOkAndParse(response, "DSL _search on " + index + " with aggregations " + aggregationsJson);
    }

    /** The response's {@code aggregations} section, which must be present for every request here. */
    private static Map<String, Object> aggregations(Map<String, Object> body) {
        Object aggs = body.get("aggregations");
        assertNotNull("response must carry an aggregations section, got keys " + body.keySet(), aggs);
        return asObject(aggs);
    }

    /** A single-value metric's {@code value}, from either the response root or a bucket. */
    private static double metricValue(Map<String, Object> holder, String name) {
        Object metric = holder.get(name);
        assertNotNull("missing aggregation [" + name + "], present: " + holder.keySet(), metric);
        Object value = asObject(metric).get("value");
        assertNotNull("aggregation [" + name + "] returned a null value — no matching docs reached the reduce", value);
        return ((Number) value).doubleValue();
    }

    /** A terms aggregation's buckets keyed by term, with the no-dropped-bucket invariant asserted. */
    private static Map<String, Map<String, Object>> bucketsByKey(Map<String, Object> body, String termsAggName) {
        Object terms = aggregations(body).get(termsAggName);
        assertNotNull("missing terms aggregation [" + termsAggName + "]", terms);
        Map<String, Object> termsAgg = asObject(terms);
        assertEquals(
            "terms [" + termsAggName + "] must not discard a tail — a non-zero sum_other_doc_count means the asserted "
                + "subtotals cover only part of the data",
            0,
            ((Number) termsAgg.get("sum_other_doc_count")).intValue()
        );

        Object rawBuckets = termsAgg.get("buckets");
        assertTrue("terms [" + termsAggName + "] must carry a buckets array, got " + rawBuckets, rawBuckets instanceof List);
        Map<String, Map<String, Object>> byKey = new HashMap<>();
        for (Object rawBucket : (List<?>) rawBuckets) {
            Map<String, Object> bucket = asObject(rawBucket);
            byKey.put((String) bucket.get("key"), bucket);
        }
        return byKey;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asObject(Object value) {
        assertTrue("expected a JSON object, got " + value, value instanceof Map);
        return (Map<String, Object>) value;
    }

    // ── Provisioning ──────────────────────────────────────────────────────────────

    /**
     * Creates the 2-shard composite/parquet-primary index and bulk-indexes the fixture inline — the
     * dataset is 8 documents, so it does not justify a {@link DatasetProvisioner} resource. Runs once per
     * JVM via {@link #onBeforeQuery()}; the leading DELETE only matters when a preserved cluster still
     * holds the index from an earlier run.
     */
    private void provision() throws IOException {
        provisionIndex(INDEX, 2);
        provisionIndex(ONE_SHARD_INDEX, 1);
    }

    /**
     * Creates one composite/parquet-primary index at the given shard count and loads {@link #BULK_BODY}
     * into it. The two indices differ in nothing but {@code number_of_shards}, which is what lets the
     * 1-shard control tests attribute a difference to the shard count and nothing else.
     *
     * @param index the index name
     * @param shards the primary shard count
     */
    private void provisionIndex(String index, int shards) throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (ResponseException ignored) {
            // 404 on a fresh cluster — nothing to clean up.
        }

        // int_amount is mapped integer, not long or double: that is what makes Calcite declare Int32
        // while DataFusion accumulates the sum in Int64. float_amount is mapped float, not double, for
        // the same reason one width up: Calcite REAL (Float32) against DataFusion's Float64.
        String mapping = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": " + shards + ","
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"tenant\":       { \"type\": \"keyword\" },"
            + "    \"int_amount\":   { \"type\": \"integer\" },"
            + "    \"float_amount\": { \"type\": \"float\" },"
            + "    \"small_amount\": { \"type\": \"integer\" }"
            + "  }"
            + "}"
            + "}";

        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(mapping);
        Map<String, Object> createResponse = assertOkAndParse(client().performRequest(create), "create " + index);
        assertEquals("index creation must be acknowledged", true, createResponse.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + index);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);

        Request bulk = new Request("POST", "/" + index + "/_bulk");
        bulk.setJsonEntity(BULK_BODY);
        bulk.addParameter("refresh", "true");
        Map<String, Object> bulkResponse = assertOkAndParse(client().performRequest(bulk), "_bulk " + index);
        // The item statuses are the only place a rejection says why, so print them rather than a boolean.
        assertEquals("bulk ingest reported item errors: " + bulkResponse.get("items"), Boolean.FALSE, bulkResponse.get("errors"));
        client().performRequest(new Request("POST", "/" + index + "/_flush?force=true"));

        // Logged, not asserted: the _id hash decides the split and every total asserted here is
        // spread-invariant, so an assertion on the split would pin the hash rather than the arithmetic.
        Request shards2 = new Request("GET", "/_cat/shards/" + index);
        shards2.addParameter("h", "shard,docs,node");
        Response shardsResponse = client().performRequest(shards2);
        try (InputStream is = shardsResponse.getEntity().getContent()) {
            String spread = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            logger.info("[{}] shard doc distribution (shard/docs/node):\n{}", index, spread);
        }
    }
}
