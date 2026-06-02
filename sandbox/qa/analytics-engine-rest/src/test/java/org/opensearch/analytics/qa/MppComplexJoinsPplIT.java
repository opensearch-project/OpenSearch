/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;

import java.io.IOException;
import java.util.Set;

/**
 * MPP-focused complex-joins PPL integration test (multi-index).
 *
 * <p>Provisions every index referenced by the {@code complex_joins} PPL query
 * resources — {@code security_logs}, {@code app_monitor}, {@code event_processor},
 * {@code monitor_tracking}, {@code tax_withholding}, {@code voice_verification}
 * (from {@link MultiSourceJoinsTestHelper#DATASET}), plus
 * {@code kubernetes_logs} and {@code performance_metrics} — and runs each query
 * twice on the same data: once with {@code analytics.mpp.enabled=false} (every
 * join routes through coordinator-centric) and once with
 * {@code analytics.mpp.enabled=true} (CBO is free to pick BROADCAST or
 * HASH_SHUFFLE when its cost model favors it).
 *
 * <p>Each pass goes through the standard expected-response check
 * ({@link DatasetQueryRunner} + {@link ResponseValidator}) so the asserted row
 * multisets must match the pinned expected JSON under both modes — a regression
 * in either code path fails the test.
 *
 * <p>Q1, Q2, Q3, Q5, Q7 are skipped because they exercise {@code security_logs},
 * which has {@code source_ip} / {@code destination_ip} mapped as {@code ip} —
 * Substrait emits {@code Binary} for those columns but Parquet returns
 * {@code BinaryView}, so the exchange-sink schema check fails. Q6, Q8, Q9, Q10
 * are skipped because they hit the {@code DIVIDE(decimal, i64)} → Substrait
 * conversion gap in the engine. These are known engine-side bugs unrelated to
 * MPP join routing; un-skip once they are fixed.
 */
public class MppComplexJoinsPplIT extends BasePplIT {

    /**
     * Queries known to fail at engine layer for reasons unrelated to MPP routing.
     * Both passes (mpp on / mpp off) hit the same failure, so the queries are
     * skipped under both — the parity guarantee still holds for the surviving
     * queries (currently Q4).
     */
    private static final Set<Integer> SKIPPED_QUERIES = Set.of(
        // security_logs ip-column Binary vs BinaryView Substrait mismatch.
        1, 2, 3, 5, 7,
        // DIVIDE(decimal, i64) Substrait conversion not yet supported.
        6, 8, 9, 10
    );

    private boolean additionalDataProvisioned = false;

    @Override
    protected Dataset getDataset() {
        return ComplexJoinsTestHelper.DATASET;
    }

    @Override
    protected Set<Integer> getSkipQueries() {
        return SKIPPED_QUERIES;
    }

    @Override
    public void tearDown() throws Exception {
        // Reset the MPP gate after every test so a failure can't leak state into the next.
        resetSetting("analytics.mpp.enabled");
        super.tearDown();
    }

    private void ensureAdditionalDataProvisioned() throws Exception {
        if (!additionalDataProvisioned) {
            DatasetProvisioner.provision(client(), SecurityLogsTestHelper.DATASET);
            // MultiSourceJoinsTestHelper covers app_monitor, event_processor,
            // monitor_tracking, tax_withholding, and voice_verification — all
            // referenced by complex_joins/q*.ppl.
            DatasetProvisioner.provision(client(), MultiSourceJoinsTestHelper.DATASET);
            DatasetProvisioner.provision(client(), KubernetesLogsTestHelper.DATASET);
            DatasetProvisioner.provision(client(), PerformanceMetricsTestHelper.DATASET);
            additionalDataProvisioned = true;
        }
    }

    /** Run with the kill switch on — every join routes through coordinator-centric. */
    public void testComplexJoinsPplQueries_mppDisabled() throws Exception {
        ensureAdditionalDataProvisioned();
        applySetting("analytics.mpp.enabled", "false");
        runPplQueries();
    }

    /** Run with MPP on — CBO is free to pick BROADCAST or HASH_SHUFFLE per query. */
    public void testComplexJoinsPplQueries_mppEnabled() throws Exception {
        ensureAdditionalDataProvisioned();
        applySetting("analytics.mpp.enabled", "true");
        runPplQueries();
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
}
