/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.util.Map;

/**
 * 2-shard correctness for joins ({@code join/} — inner/left/semi/anti + join-then-group, self-joined
 * on {@code %INDEX%}, asserting cardinality 30/300/30/10/20 and by-group 100/cat).
 *
 * <p>These were muted because the bare {@code [ source ]} inner arm resolved to lucene while the outer
 * arm stayed datafusion, and a single (exchange-free) multi-input stage cannot straddle backends —
 * {@code PlanForker} produced zero plan alternatives for the join. Now that {@code PlanForker} picks,
 * for each arm, an alternative on a backend the join itself can run, the arms reconcile on datafusion
 * and the join resolves; all six pass at 2 shards.
 */
public class TwoShardJoinIT extends TwoShardReduceTestCase {

    @Override
    protected Map<String, Boolean> tiers() {
        return Map.of("join", false);
    }
}
