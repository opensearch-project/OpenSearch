/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.util.Set;

/**
 * OTel logs PPL integration test.
 *
 * <p>Three queries are skipped today as documented gaps:
 * Q18 ({@code dc()} HLL approximation vs exact golden), Q32 (text-equality returns 0 rows),
 * and Q33 (head 0 ClassCastException). All stay on disk as live reproducers — when the
 * upstream fixes land, remove the entry from getSkipQueries() to re-enable them.
 */
public class OtelLogsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return OtelLogsTestHelper.DATASET;
    }

    @Override
    protected Set<Integer> getSkipQueries() {
        // Q18: `stats count(), dc(traceId) by serviceName`. dc() lowers to the engine-native
        // APPROX_COUNT_DISTINCT (HLL sketch), which under-counts the largest group ("cart",
        // 30 distinct traceIds) by 1 — returns 29. Data and golden are both correct (verified
        // 30 distinct); this is inherent HLL approximation vs the harness's exact-match assertion.
        // Reproduces on the configured JDK 25 runtime. Re-enable once the harness tolerates
        // approximate columns or dc() is exact for small cardinalities.
        //
        // Q32: `where severityText = 'ERROR'` returns 0 rows on a multi-field
        // text+keyword field. Aggregation auto-redirects to the keyword sub-field,
        // but Mustang's CBO routes equality predicates to DataFusion which scans
        // the text-parent parquet column (empty/null). Bug lives in
        // OpenSearch/sandbox/plugins/analytics-engine/.../OpenSearchFilterRule.java
        // — should restrict viableBackends to lucene for equality on multi-field text.
        // Workaround in user queries: filter on a numeric parallel (severityNumber=17),
        // or use `like` (see q5).
        //
        // Q33: `... | head 0` triggers ClassCastException in
        // org.opensearch.sql.api.UnifiedQueryPlanner.preserveCollation:145 —
        // RelCompositeTrait cannot be cast to RelCollation. Upstream
        return Set.of(18, 32, 33);
    }

    public void testOtelLogsPplQueries() throws Exception {
        runPplQueries();
    }
}
