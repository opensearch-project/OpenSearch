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
 * <p><b>Coverage on this backend (2 shards).</b> Of the 22 canonical TPC-H queries, 21 run and
 * match captured expected files (all but q15 — most are multi-table joins, so the MPP join path,
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
 * <p>The remaining 1 is skipped via {@link #getSkipQueries()}:
 * <ul>
 *   <li><b>q15</b> — <em>hangs</em> (the engine deadlocks; it does not error). q15 references its
 *       {@code revenue0} CTE twice — once as the join's right input and once inside a
 *       {@code where total_revenue = [... max(total_revenue)]} scalar subquery — producing a root
 *       {@code Join(Join(supplier, revenue0), MAX(revenue0))} that the planner cuts into <b>three
 *       cascaded coordinator reduces</b> (the join-root reduce with three inputs, the revenue0
 *       consumer reduce, and the max-branch reduce). Multi-node thread dumps + DEBUG tracing show
 *       the underlying HASH_SHUFFLE aggregate for {@code revenue0} <em>completes fine</em> (producer
 *       ships batches, both workers drain), but one coordinator reduce then freezes in native
 *       {@code streamNext} while the others are idle and no producer is blocked sending — i.e. the
 *       root join-reduce waits forever for an input that never arrives. This is a deep
 *       <b>multi-input cascaded-coordinator-reduce streaming deadlock</b> in the native reduce path,
 *       NOT the empty-partition / coercion issues earlier suspected (both ruled out by tracing), and
 *       distinct from q4's broadcast-build-subtree bug. A fix needs native (Rust) work on how a
 *       join-reduce drives three registered input partition-streams, one of which is fed by another
 *       concurrent local reduce; the buffered (non-eager) memtable sink can't substitute because it
 *       is single-input only. Tracked as a follow-up. The source IT asserts 1 row for q15.</li>
 * </ul>
 * As the engine closes this gap, move q15 out of {@link #getSkipQueries()} and capture its
 * expected file via {@link #testCaptureExpected}.
 *
 * <p><b>Capturing expected files.</b> Run with {@code -Dtests.tpch.capture.dir=<abs path>}
 * to provision the dataset, execute all 22 queries, and write each successful response to
 * {@code <dir>/q{N}.json} in the canonical {schema, datarows, total, size} shape. Copy the
 * verified ones into {@code src/test/resources/datasets/tpch/ppl/expected/}.
 */
public class TpchPplIT extends BasePplIT {

    /** Queries not yet runnable on the parquet/DataFusion path. Only q15 remains: it HANGS (does not
     *  error) — it references the {@code revenue0} CTE twice (join input + max-subquery), yielding a
     *  three-cascaded-coordinator-reduce join that deadlocks in native streamNext (the revenue0
     *  hash-shuffle itself completes; one reduce then waits forever for an input that never arrives).
     *  A native (Rust) reduce-path fix is needed; tracked as a follow-up. (The old "subquery coercion
     *  error" reason is stale — fixed by merge #22010.) q4 (broadcast multi-stage build sub-tree, see
     *  {@code BroadcastDispatch.run}), q7/q8 (BETWEEN timestamp/date coercion — #22010/#22045/#21978),
     *  and q20 (subquery coercion #22010) were all unblocked; their captured results match the source
     *  SQL plugin's {@code CalcitePPLTpchIT} assertions exactly (q7→0 rows, q8→[1995,0.0]/[1996,0.0],
     *  q20→0 rows). See class javadoc. */
    private static final Set<Integer> UNSUPPORTED = Set.of(15);

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
