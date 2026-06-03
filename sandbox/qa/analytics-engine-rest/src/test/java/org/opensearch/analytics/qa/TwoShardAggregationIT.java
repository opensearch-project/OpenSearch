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
 * median).
 *
 * <p>xfail: {@code distinct_count}/{@code dc} HLL cross-shard merge over-counts for repeated keyword
 * sets ({@code distinct_count(label)} = 5 at 1 shard, 9 at 2); correct for all-unique/low-card columns.
 */
public class TwoShardAggregationIT extends TwoShardReduceTestCase {

    @Override
    protected Map<String, Boolean> tiers() {
        Map<String, Boolean> t = new LinkedHashMap<>();
        t.put("agg", false);       // exact
        t.put("approx", true);     // golden + tolerance
        return t;
    }

    @Override
    protected Map<String, String> knownIssues() {
        String hll = "HLL cross-shard merge over-counts repeated keyword sets (label 5->9 at 2 shards)";
        return Map.of("distinct_count_label", hll, "distinct_count_by_cat", hll, "dc_label", hll);
    }
}
