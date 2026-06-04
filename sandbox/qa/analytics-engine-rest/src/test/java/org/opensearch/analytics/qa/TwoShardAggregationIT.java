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
