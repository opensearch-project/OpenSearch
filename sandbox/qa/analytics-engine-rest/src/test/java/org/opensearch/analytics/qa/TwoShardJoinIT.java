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
 * 2-shard correctness for joins ({@code join/} — inner/left/semi/anti + join-then-group, self-joined
 * on {@code %INDEX%}, asserting cardinality 30/300/30/10/20 and by-group 100/cat).
 *
 * <p>All currently muted: since #21867 the bare {@code [ source ]} inner arm is driven by lucene while
 * the outer arm stays datafusion, and {@code OpenSearchJoin} rejects the mixed backends. Reduce-sound
 * before #21867. Unmute (drop from {@link #knownIssues()}) once the planner reconciles per-arm backends.
 */
public class TwoShardJoinIT extends TwoShardReduceTestCase {

    @Override
    protected Map<String, Boolean> tiers() {
        return Map.of("join", false);
    }

    @Override
    protected Map<String, String> knownIssues() {
        String reason = "OpenSearchJoin rejects mixed per-arm backends ([lucene] inner vs [datafusion]) at 2 shards (#21867)";
        Map<String, String> m = new LinkedHashMap<>();
        for (String q : new String[] {
            "join_inner_id_count", "join_inner_category_count", "join_left_count",
            "join_semi_count", "join_anti_count", "join_inner_by_category"
        }) {
            m.put(q, reason);
        }
        return m;
    }
}
