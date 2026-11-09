/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 2-shard reduce correctness with the MPP distributed path FORCED on (see {@link #forceMpp()}). The
 * coordinator-centric variants ({@link TwoShardAggregationIT} / {@link TwoShardScalarIT}) cover the same
 * query families with {@code analytics.mpp.enabled} at its default (off); this variant lowers the
 * {@code analytics.mpp.distribute.min_rows} floor to 1 so the distribution-enforcement pass distributes
 * the tiny {@code merge_coverage} dataset and the queries flow through the reduce-stage substrait emit.
 *
 * <p>Guards the MPP-reduce shapes that the enforcement pass must keep intact across the {@code DAGBuilder}
 * exchange cut:
 * <ul>
 *   <li><b>{@code agg/span_percentile_50}, {@code agg/span_median}</b> — a {@code percentile_approx} whose
 *       percentile literal must ride with the aggregate (not be stranded below the gather, which yields
 *       "APPROX_PERCENTILE_CONT must be a literal").</li>
 *   <li><b>{@code approx/dc_*}, {@code agg/span_distinct_count_label}</b> — {@code APPROX_COUNT_DISTINCT}
 *       whose CBO PARTIAL/FINAL split must keep the exchange between the two phases so the FINAL reads
 *       HLL partial state (Binary), not raw rows (the Utf8View/Int32→Binary cast and count-state index
 *       errors).</li>
 *   <li><b>{@code scalar/sc_round_int}</b> — {@code round(int)}, whose Calcite-declared type differs from
 *       the backend runtime type and must stay in one fragment (no StageInputScan type clash).</li>
 * </ul>
 */
public class TwoShardMppReduceIT extends TwoShardReduceTestCase {

    @Override
    protected boolean forceMpp() {
        return true;
    }

    @Override
    protected Map<String, Boolean> tiers() {
        Map<String, Boolean> t = new LinkedHashMap<>();
        t.put("agg", false);       // exact
        t.put("approx", true);     // golden + tolerance
        t.put("scalar", false);    // exact
        return t;
    }

    @Override
    protected Map<String, String> knownIssues() {
        // Same pre-existing scalar gaps muted by TwoShardScalarIT — orthogonal to MPP and failing in the
        // coordinator-centric path too.
        return Map.of(
            "sc_coalesce_ip", "coalesce() on ip throws ExpressionEvaluationException: unsupported object class [B",
            "sc_str_to_date", "str_to_date timestamp format is locale-sensitive (T vs space separator)"
        );
    }
}
