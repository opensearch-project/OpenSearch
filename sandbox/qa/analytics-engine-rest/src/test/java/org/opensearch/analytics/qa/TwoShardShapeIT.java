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
 * 2-shard correctness for query shapes ({@code shape/} — group-by, filtered group-by, sort/limit
 * top-N, dedup) and windows ({@code window/} — eventstats, streamstats).
 *
 * <p>xfail: {@code streamstats} (cumulative window) is order-sensitive on input, but the cross-shard
 * gather is arrival-ordered, so the running aggregate is computed over the wrong order — 1-shard
 * {@code 1,3,6,10,…} vs 2-shard {@code 216,2,5,9,…}.
 */
public class TwoShardShapeIT extends TwoShardReduceTestCase {

    @Override
    protected Map<String, Boolean> tiers() {
        Map<String, Boolean> t = new LinkedHashMap<>();
        t.put("shape", false);
        t.put("window", false);
        return t;
    }

    @Override
    protected Map<String, String> knownIssues() {
        return Map.of(
            "streamstats_sum",
            "cumulative window over arrival-ordered gather: 1-shard 1,3,6,10,... vs 2-shard 216,2,5,9,...",
            "dedup_category",
            "Multiple filters in a single fragment are not supported (PR #21948)"
        );
    }
}
