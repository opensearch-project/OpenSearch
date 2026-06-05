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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;

/**
 * TPC-H PPL integration test, migrated from the SQL plugin's
 * {@code org.opensearch.sql.calcite.tpch.CalcitePPLTpchIT}.
 *
 * <p>The eight TPC-H tables are provisioned from the {@code tpch} dataset; the 22
 * canonical TPC-H queries live under {@code datasets/tpch/ppl/q{N}.ppl} and are validated
 * against {@code datasets/tpch/ppl/expected/q{N}.json} by {@link ResponseValidator}
 * (row-multiset comparison, 1e-9 float tolerance).
 *
 * <p><b>Backend difference vs. the source IT.</b> The source runs Calcite over the
 * <em>Lucene</em> backend with a pushdown-enabled/disabled value fork. This test runs
 * through {@link DatasetProvisioner}, which forces {@code primary_data_format=parquet}, and
 * provisions each index with <b>2 shards</b> (see {@link #onBeforeQuery}) so multi-table joins
 * actually exercise the MPP path (broadcast / hash-shuffle / coordinator-centric) rather than a
 * trivial single-shard reduce. Expected values were captured from a real 2-shard parquet run on
 * this backend (see {@link #testCaptureExpected}), not transcribed from the source's Lucene
 * assertions.
 *
 * <p><b>Coverage on this backend (2 shards).</b> Of the 22 canonical TPC-H queries, 17 run and
 * match captured expected files (q1, q2, q3, q5, q6, q9, q10, q11, q12, q13, q14, q16, q17, q18,
 * q19, q21, q22 — most are multi-table joins, so the MPP join path, including multi-broadcast
 * queries, is genuinely covered). q1, q6, q10, q12, q14 were unblocked once {@code date_add}/{@code
 * date_sub} over a {@code DATE('...')} literal landed (#21991); their captured values match the
 * source SQL plugin's {@code CalcitePPLTpchIT} assertions. q5 returns an empty result set, which the
 * source IT also asserts ({@code verifyNumOfRows(actual, 0)}) — the full 6-table join is correct,
 * the {@code c_nationkey = s_nationkey} + {@code r_name = 'ASIA'} predicate just selects no rows in
 * this dataset — so it is asserted (empty expected file), not skipped. The remaining 5 are skipped
 * via {@link #getSkipQueries()}:
 * <ul>
 *   <li><b>q7, q8</b> — {@code BETWEEN} with mixed timestamp/date operands fails type
 *       coercion ({@code BETWEEN expression types are incompatible: [TIMESTAMP, DATE, DATE]}).</li>
 *   <li><b>q15, q20</b> — a timestamp-vs-date comparison inside a <em>decorrelated subquery</em>
 *       fails Substrait conversion ("Unable to convert call &gt;=(precision_timestamp&lt;0&gt;?,
 *       string?)"). The same comparison at the top level (q1, q6, …) works; inside a subquery
 *       {@code RelDecorrelator}'s expression simplification constant-folds the PPL {@code TIMESTAMP}
 *       UDF down to its bare string argument before the backend's scalar-function adapter runs, so
 *       the timestamp coercion is lost. Independent of the literal form ({@code date('...')} and
 *       {@code DATE '...'} both regress).</li>
 *   <li><b>q4</b> — a <em>correlated</em> {@code EXISTS} subquery over lineitem times out (60s).
 *       The date window alone (no EXISTS) runs fine; even a bare {@code exists [lineitem where
 *       l_orderkey = o_orderkey]} hangs, so the cause is the correlated-EXISTS execution path
 *       (not decorrelated to a join, unlike q15/q20's scalar/IN subqueries), not date arithmetic.</li>
 * </ul>
 * As the engine closes these gaps, move queries out of {@link #getSkipQueries()} and capture
 * their expected files via {@link #testCaptureExpected}.
 *
 * <p><b>Capturing expected files.</b> Run with {@code -Dtests.tpch.capture.dir=<abs path>}
 * to provision the dataset, execute all 22 queries, and write each successful response to
 * {@code <dir>/q{N}.json} in the canonical {schema, datarows, total, size} shape. Copy the
 * verified ones into {@code src/test/resources/datasets/tpch/ppl/expected/}.
 */
public class TpchPplIT extends BasePplIT {

    /** Queries not yet runnable on the parquet/DataFusion path. After date_add/date_sub over a DATE
     *  literal landed (#21991, unblocking q1,q6,q10,q12,q14) and q5 was confirmed legitimately empty
     *  (now asserted, not skipped), the remaining gaps are: q7,q8 — BETWEEN timestamp/date coercion;
     *  q15,q20 — a timestamp/date comparison inside a decorrelated subquery (the PPL TIMESTAMP UDF is
     *  constant-folded to a bare string before the backend adapter runs); q4 — a correlated EXISTS
     *  subquery times out. See class javadoc. */
    private static final Set<Integer> UNSUPPORTED = Set.of(4, 7, 8, 15, 20);

    /** The eight TPC-H tables, each provisioned from its own {@code mapping_<index>.json} +
     *  {@code bulk_<index>.json} under {@code resources/datasets/tpch/}. */
    private static final Dataset DATASET = new Dataset(
        "tpch",
        "customer",
        "lineitem",
        "orders",
        "supplier",
        "part",
        "partsupp",
        "nation",
        "region"
    );

    @Override
    protected Dataset getDataset() {
        return DATASET;
    }

    @Override
    protected ExpectedResponseStrategy getStrategy() {
        return ExpectedResponseStrategy.FAIL_ON_MISSING;
    }

    @Override
    protected Set<Integer> getSkipQueries() {
        return UNSUPPORTED;
    }

    /** Provision each TPC-H index with 2 shards so multi-table joins exercise the MPP path
     *  (broadcast / hash-shuffle / coordinator-centric), not the trivial single-shard reduce.
     *  Overrides {@link BasePplIT#onBeforeQuery} (whose {@code dataProvisioned} flag is private)
     *  with a local once-per-JVM guard. */
    private static boolean tpchProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (!tpchProvisioned) {
            DatasetProvisioner.provision(client(), getDataset(), 2);
            tpchProvisioned = true;
        }
    }

    /**
     * Runs the 11 currently-supported TPC-H queries against their captured expected files
     * (row-multiset comparison via {@link ResponseValidator}). The 11 date-arithmetic queries in
     * {@link #getSkipQueries()} are excluded until the engine recognizes date_add/date_sub over
     * DATE literals and BETWEEN timestamp/date coercion.
     */
    public void testTpchPplQueries() throws Exception {
        runPplQueries();
    }

    /**
     * Capture-mode helper: only does work when {@code -Dtests.tpch.capture.dir} is set.
     * Provisions the dataset, runs every discovered query against the real parquet-backed
     * cluster, and snapshots each <em>successful</em> response as {@code q{N}.json} into the
     * capture dir. Per-query failures are collected and logged, not thrown, so one run
     * inventories the full pass/fail split across all 22 queries.
     */
    public void testCaptureExpected() throws Exception {
        String captureDir = System.getProperty("tests.tpch.capture.dir");
        if (captureDir == null || captureDir.isBlank()) {
            logger.info("tests.tpch.capture.dir not set — skipping TPC-H expected capture");
            return;
        }
        onBeforeQuery(); // provision dataset (idempotent, gated in BasePplIT)
        Path outDir = Path.of(captureDir);
        Files.createDirectories(outDir);

        List<Integer> queryNumbers = DatasetQueryRunner.discoverQueryNumbers(getDataset(), "ppl");
        List<String> failures = new ArrayList<>();
        int captured = 0;
        for (int n : queryNumbers) {
            String ppl = loadResource(getDataset().queryResourcePath("ppl", "ppl", n)).trim();
            try {
                Request request = new Request("POST", "/_plugins/_ppl");
                request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
                Response response = client().performRequest(request);
                Map<String, Object> body = assertOkAndParse(response, "TPC-H capture q" + n);

                XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
                builder.map(body);
                String json = builder.toString();
                Files.write(outDir.resolve("q" + n + ".json"), (json + "\n").getBytes(StandardCharsets.UTF_8));
                logger.info("Captured TPC-H q{} -> {}", n, outDir.resolve("q" + n + ".json"));
                captured++;
            } catch (Exception e) {
                String msg = "q" + n + ": " + e.getMessage().replaceAll("\\s+", " ");
                logger.warn("TPC-H capture FAILED {}", msg);
                failures.add(msg);
            }
        }
        logger.info("TPC-H capture summary: {} captured, {} failed", captured, failures.size());
        for (String f : failures) {
            logger.info("  FAILED {}", f);
        }
    }
}
