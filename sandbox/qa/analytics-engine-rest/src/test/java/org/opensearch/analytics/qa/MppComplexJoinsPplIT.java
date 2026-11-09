/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
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
 * <p>Currently {@code @AwaitsFix}'d. All ten queries fail at the engine layer for
 * reasons unrelated to MPP join routing — both mpp-on and mpp-off paths hit the
 * same failures, so the parity guarantee is irrelevant until the underlying
 * engine bugs are fixed:
 * <ul>
 *   <li>Q1, Q2, Q3, Q5, Q7 — {@code security_logs} has {@code source_ip} /
 *       {@code destination_ip} mapped as {@code ip}. Substrait emits {@code Binary}
 *       for those columns but Parquet returns {@code BinaryView}, so the
 *       exchange-sink schema check fails.</li>
 *   <li>Q4 — {@code stats by kubernetes.pod_name | inner join … | stats by host}
 *       returns 0 rows instead of the expected 3. Same failure under mpp on/off.</li>
 *   <li>Q6, Q8, Q9, Q10 — hit the {@code DIVIDE(decimal, i64)} → Substrait
 *       conversion gap in the engine.</li>
 * </ul>
 *
 * <p>The sibling {@link ComplexJoinsPplIT} is also {@code @AwaitsFix}'d for the
 * same reason. Un-mute once the engine bugs are fixed; the parity-pass-pair will
 * automatically catch any MPP-side regression at that point.
 */
@AwaitsFix(
    bugUrl = "Engine-side bugs (Binary/BinaryView, DIVIDE(decimal,i64), pod_name=host inner join) — see class javadoc"
)
public class MppComplexJoinsPplIT extends BasePplIT {

    /**
     * Queries known to fail at engine layer for reasons unrelated to MPP routing.
     * Kept for when the test is un-muted; {@link #SKIPPED_QUERIES} should match
     * the engine-bug list in the class javadoc.
     */
    private static final Set<Integer> SKIPPED_QUERIES = Set.of(
        // security_logs ip-column Binary vs BinaryView Substrait mismatch.
        1, 2, 3, 5, 7,
        // Stats-join-stats over kubernetes_logs + performance_metrics returns 0 rows.
        4,
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
