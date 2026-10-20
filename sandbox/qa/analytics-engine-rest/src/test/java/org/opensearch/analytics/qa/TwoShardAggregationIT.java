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
 * 2-shard reduce correctness for aggregations: exact tier {@code agg/} (count/sum/avg/min/max,
 * stddev/var, span, values/list) and approximate tier {@code approx/} (distinct_count/dc/percentile/
 * median). {@code distinct_count}/{@code dc} are rewritten to {@code APPROX_COUNT_DISTINCT} and
 * computed once at the coordinator, so per-shard distinct counts are never summed.
 *
 * <p>The {@code agg/span_*} set covers every aggregate over a {@code span(...)} group key — a
 * non-prefix groupSet that exercises the PARTIAL/FINAL key-fronting fix. A few of those
 * ({@code span_distinct_count_label}, {@code span_percentile_50}, {@code span_median}) use
 * approximate algorithms (HLL / percentile_approx) yet sit in the EXACT tier: on the fixed 30-row
 * {@code merge_coverage} dataset they are below the approximation threshold and return exact,
 * shard-invariant results, so the 1-shard==2-shard differential holds. If that dataset ever grows
 * past the approximation threshold these must move to the {@code approx/} tier — but note the
 * approx tier's tolerance aligner keys rows on non-numeric group cells, so a numeric {@code span}
 * key can't be aligned there without a harness change.
 */
public class TwoShardAggregationIT extends TwoShardReduceTestCase {

    @Override
    protected Map<String, Boolean> tiers() {
        Map<String, Boolean> t = new LinkedHashMap<>();
        t.put("agg", false);       // exact
        t.put("approx", true);     // golden + tolerance
        return t;
    }
}
