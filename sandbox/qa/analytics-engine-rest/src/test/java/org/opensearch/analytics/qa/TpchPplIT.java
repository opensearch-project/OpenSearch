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
 * including multi-broadcast queries, is genuinely covered). Every captured value was
 * cross-checked against the source SQL plugin's {@code CalcitePPLTpchIT} assertions.
 *
 * <p><b>Capturing expected files.</b> Run with {@code -Dtests.tpch.capture.dir=<abs path>}
 * to provision the dataset, execute all 22 queries, and write each successful response to
 * {@code <dir>/q{N}.json} in the canonical {schema, datarows, total, size} shape. Copy the
 * verified ones into {@code src/test/resources/datasets/tpch/ppl/expected/}.
 */
public class TpchPplIT extends BasePplIT {

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
        // This IT exists to exercise the MPP path; enable it explicitly rather than relying on the
        // cluster default (production defaults analytics.mpp.enabled=false, and the QA build.gradle
        // value may change). Self-enabling keeps the TPC-H queries on the BROADCAST / HASH_SHUFFLE /
        // HASH_SHUFFLE_AGG paths these tests are written to validate.
        applySetting("analytics.mpp.enabled", "true");
        if (!tpchProvisioned) {
            DatasetProvisioner.provision(client(), getDataset(), 2);
            tpchProvisioned = true;
        }
    }

    @Override
    public void tearDown() throws Exception {
        // analytics.mpp.enabled is applied cluster-wide (transient) in onBeforeQuery; reset it so the
        // setting does not leak onto subsequent test classes (the shared 2-node cluster persists across
        // ITs, and an un-reset mpp.enabled=true would route later coord-centric suites — e.g. the
        // TwoShard* reduce tests — onto the MPP distributed path they are not written for).
        resetSetting("analytics.mpp.enabled");
        super.tearDown();
    }

    private void applySetting(String key, String value) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\": {\"" + key + "\": " + value + "}}");
        client().performRequest(request);
    }

    private void resetSetting(String key) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\": {\"" + key + "\": null}}");
        client().performRequest(request);
    }

    /**
     * Runs the TPC-H queries against their captured expected files.
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
