/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Same exact {@code agg/} differential + golden suite as {@link TwoShardAggregationIT}, but with
 * {@code analytics.shard_bucket_oversampling_factor} turned on. Oversampling changes how many
 * candidate buckets each shard ships to the coordinator (and inserts the TopK shard-limit rewrite),
 * so this exercises that rewrite composing with the non-prefix-groupSet span aggregates (and with
 * the sort/top-N-over-span shapes). At this dataset scale oversampling does not change the final
 * exact result, so the 1-shard == 2-shard differential and the goldens must still hold — proving
 * the oversampling/TopK rewrite doesn't perturb exact aggregation over a fronted-key FINAL stage.
 *
 * <p>Only the exact {@code agg} tier is run — the {@code approx} tier is genuinely non-deterministic
 * under oversampling and is covered without oversampling by {@link TwoShardAggregationIT}.
 */
public class TwoShardOversamplingAggregationIT extends TwoShardReduceTestCase {

    @Override
    protected Map<String, Boolean> tiers() {
        Map<String, Boolean> t = new LinkedHashMap<>();
        t.put("agg", false); // exact tier only — oversampling must be a no-op here
        return t;
    }

    @Override
    protected void onBeforeQuery() throws IOException {
        super.onBeforeQuery();
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"persistent\":{\"analytics.shard_bucket_oversampling_factor\": 2.0}}");
        client().performRequest(req);
    }
}
