/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.profile;

import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Per-{@code SHARD_FRAGMENT}-stage summary of the can_match pre-filter phase. Attached to a
 * {@link StageProfile} only when the phase actually ran for that stage — i.e. the query carried
 * range filters or a bounded-field sort, and the fan-out cleared the {@code worthPreFiltering}
 * threshold. Its presence therefore signals "can_match ran here"; its absence means the phase was
 * skipped (query shape or narrow fan-out) and every resolved shard was dispatched as-is.
 *
 * <p>All fields are coordinator-side aggregates. The can_match probe is a plain request/response
 * transport action (no streaming), and pruning/ordering/top-N decisions are all made on the
 * coordinator, so no shard-side or native (Rust) metrics are involved.
 *
 * @param canMatchMs           wall-clock latency of the parallel can_match probe round-trip, in millis
 * @param totalShards          shard targets considered before can_match ran
 * @param shardsPrunedByFilter shards dropped before dispatch because parquet row-group stats proved
 *                             their range disjoint from the query's {@code WHERE} predicates
 * @param shardsSkippedByTopN  shards skipped during staggered dispatch because their folded sort-column
 *                             bounds could not beat the top-N bar (the {@code sort | head N} gate); 0
 *                             when the query has no gateable sort or the gate never armed
 * @param topNGateArmed        whether the top-N gate reached its limit K, i.e. dynamic skipping was active
 * @param shardsDispatched     shards that actually ran a fragment ({@code totalShards - pruned - skipped})
 */
public record CanMatchProfile(long canMatchMs, int totalShards, int shardsPrunedByFilter, int shardsSkippedByTopN, boolean topNGateArmed,
    int shardsDispatched) implements ToXContentObject {

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("can_match_ms", canMatchMs);
        builder.field("total_shards", totalShards);
        builder.field("shards_pruned_by_filter", shardsPrunedByFilter);
        builder.field("shards_skipped_by_topn", shardsSkippedByTopN);
        builder.field("topn_gate_armed", topNGateArmed);
        builder.field("shards_dispatched", shardsDispatched);
        builder.endObject();
        return builder;
    }
}
