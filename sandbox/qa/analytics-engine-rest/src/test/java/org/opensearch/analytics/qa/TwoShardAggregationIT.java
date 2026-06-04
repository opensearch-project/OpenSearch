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
 * stddev/var, span, values/list, earliest/latest) and approximate tier {@code approx/}
 * (distinct_count/dc/percentile/median).
 *
 * <p>{@code distinct_count}/{@code dc} are correct cross-shard: DISTINCT aggregates skip the additive
 * PARTIAL/FINAL split ({@link org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule})
 * and are computed once at the coordinator, so per-shard distinct counts are never summed.
 *
 * <p>{@code earliest(value, ts)}/{@code latest(value, ts)} lower to ARG_MIN/ARG_MAX, rewritten to
 * DataFusion {@code first_value}/{@code last_value(value ORDER BY ts)} by PplAggregateCallRewriter.
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
