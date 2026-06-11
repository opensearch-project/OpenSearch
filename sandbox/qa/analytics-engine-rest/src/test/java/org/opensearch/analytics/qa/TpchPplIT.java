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
 * <p><b>Coverage on this backend (2 shards).</b> All 22 canonical TPC-H queries run and
 * match captured expected files (most are multi-table joins, so the MPP join path,
 * including multi-broadcast queries, is genuinely covered). q1, q6, q10, q12, q14 were unblocked once
 * {@code date_add}/{@code date_sub} over a {@code DATE('...')} literal landed (#21991). q5/q7/q20
 * return empty result sets, which the source IT also asserts ({@code verifyNumOfRows(actual, 0)}) —
 * the joins are correct, the dataset predicates (q5 {@code r_name='ASIA'}, q7 FRANCE/GERMANY, q20
 * {@code n_name='CANADA'} + forest-part subquery) just select no rows here — so they are asserted
 * (empty expected files), not skipped. q8's all-zero {@code mkt_share} ({@code [1995,0.0],[1996,0.0]})
 * likewise matches the source IT (no BRAZIL rows survive the join). Every captured value was
 * cross-checked against the source SQL plugin's {@code CalcitePPLTpchIT} assertions.
 *
 * <p>q4 (a <em>correlated</em> {@code EXISTS} over lineitem) was unblocked once the broadcast
 * dispatcher's pass-1 learned to run a <em>multi-stage build sub-tree</em>: q4's decorrelated EXISTS
 * lowers to an INNER join whose build side is {@code Project(distinct l_orderkey)} over a
 * PARTIAL→FINAL aggregate. CBO picks BROADCAST, so the build stage is itself a coordinator-reduce
 * over a shard aggregate. The old pass-1 built only the build <em>node</em> and never scheduled its
 * child shard stage, so the build reduce blocked forever in {@code streamNext} on input nobody fed
 * (the "60s timeout" was the client read-timeout; the server reduce thread stayed deadlocked). The
 * fix (see {@code BroadcastDispatch.run} → {@code StageExecutionBuilder.buildSubGraphWithSink})
 * builds + dispatches the build stage's entire sub-tree. q7/q8 (BETWEEN timestamp/date coercion) and
 * q20 (timestamp/date in a decorrelated subquery) were unblocked by the merge's coercion fixes
 * (#22010 / #22045 / #21978).
 *
 * <p>q15 (which references its {@code revenue0} CTE twice, yielding a HASH_SHUFFLE_AGG for the CTE
 * plus a coordinator-centric join whose build side is a bare {@code supplier} scan) was unblocked by
 * fixing a backend-selection bug: the {@code HashShuffleAggregateDAGRewriter} re-runs
 * {@code PlanForker.forkAll} on the rewritten DAG, which re-expands every stage to all viable
 * backends, but did NOT re-run {@code PlanAlternativeSelector.selectAll}. The supplier scan stage
 * (viable on both {@code lucene} and {@code datafusion}) therefore kept its Lucene alternative;
 * the data node picked Lucene first and streamed a 0-column metadata batch the DataFusion join
 * couldn't read, then the join-reduce hung waiting for columns that never arrived. The fix:
 * {@code PlanAlternativeSelector} now applies a parent-backend correctness constraint (a child stage
 * is restricted to backends its consuming {@code OpenSearchStageInputScan} declares viable) on EVERY
 * selection pass, and the agg rewriter re-runs {@code selectAll} after its {@code forkAll}. q15 now
 * returns the source-IT-expected single row ({@code Supplier#000000010 ... 797313.3838}).
 *
 * <p><b>Capturing expected files.</b> Run with {@code -Dtests.tpch.capture.dir=<abs path>}
 * to provision the dataset, execute all 22 queries, and write each successful response to
 * {@code <dir>/q{N}.json} in the canonical {schema, datarows, total, size} shape. Copy the
 * verified ones into {@code src/test/resources/datasets/tpch/ppl/expected/}.
 */
public class TpchPplIT extends BasePplIT {

    /** Queries not yet runnable on the parquet/DataFusion path. All 22 TPC-H queries now run: q15
     *  was the last holdout (it hung because the HASH_SHUFFLE_AGG rewriter re-forked plan
     *  alternatives without re-selecting, leaving the supplier scan on Lucene — a 0-column batch the
     *  DataFusion join couldn't read; fixed by the parent-backend constraint in
     *  {@code PlanAlternativeSelector} + the agg rewriter re-running {@code selectAll}). See class
     *  javadoc. */
    private static final Set<Integer> UNSUPPORTED = Set.of();

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
