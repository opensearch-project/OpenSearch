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
 * <p>Two queries are skipped today as documented Mustang/SQL-plugin gaps:
 * Q32 (text-equality returns 0 rows) and Q33 (head 0 ClassCastException).
 * Both stay on disk as live reproducers — when the upstream fixes land,
 * remove the entry from getSkipQueries() to re-enable them.
 */
public class OtelLogsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return OtelLogsTestHelper.DATASET;
    }

    @Override
    protected Set<Integer> getSkipQueries() {
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
        return Set.of(32, 33);
    }

    public void testOtelLogsPplQueries() throws Exception {
        runPplQueries();
    }
}
