/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.stats;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Cumulative shard indexing pressure stats
 *
 * @opensearch.api
 */
@PublicApi(since = "1.3.0")
public class ShardIndexingPressureStats implements Writeable, ToXContentFragment {

    private final Map<ShardId, IndexingPressurePerShardStats> shardIndexingPressureStore;
    private final long totalNodeLimitsBreachedRejections;
    private final long totalLastSuccessfulRequestLimitsBreachedRejections;
    private final long totalThroughputDegradationLimitsBreachedRejections;
    private final boolean shardIndexingPressureEnabled;
    private final boolean shardIndexingPressureEnforced;

    public ShardIndexingPressureStats(StreamInput in) throws IOException {
        int shardEntries = in.readInt();
        shardIndexingPressureStore = new HashMap<>();
        for (int i = 0; i < shardEntries; i++) {
            ShardId shardId = new ShardId(in);
            IndexingPressurePerShardStats shardStats = new IndexingPressurePerShardStats(in);
            shardIndexingPressureStore.put(shardId, shardStats);
        }
        totalNodeLimitsBreachedRejections = in.readVLong();
        totalLastSuccessfulRequestLimitsBreachedRejections = in.readVLong();
        totalThroughputDegradationLimitsBreachedRejections = in.readVLong();
        shardIndexingPressureEnabled = in.readBoolean();
        shardIndexingPressureEnforced = in.readBoolean();
    }

    public ShardIndexingPressureStats(
        Map<ShardId, IndexingPressurePerShardStats> shardIndexingPressureStore,
        long totalNodeLimitsBreachedRejections,
        long totalLastSuccessfulRequestLimitsBreachedRejections,
        long totalThroughputDegradationLimitsBreachedRejections,
        boolean shardIndexingPressureEnabled,
        boolean shardIndexingPressureEnforced
    ) {
        this.shardIndexingPressureStore = shardIndexingPressureStore;
        this.totalNodeLimitsBreachedRejections = totalNodeLimitsBreachedRejections;
        this.totalLastSuccessfulRequestLimitsBreachedRejections = totalLastSuccessfulRequestLimitsBreachedRejections;
        this.totalThroughputDegradationLimitsBreachedRejections = totalThroughputDegradationLimitsBreachedRejections;
        this.shardIndexingPressureEnabled = shardIndexingPressureEnabled;
        this.shardIndexingPressureEnforced = shardIndexingPressureEnforced;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(shardIndexingPressureStore.size());
        for (Map.Entry<ShardId, IndexingPressurePerShardStats> entry : shardIndexingPressureStore.entrySet()) {
            entry.getKey().writeTo(out);
            entry.getValue().writeTo(out);
        }
        out.writeVLong(totalNodeLimitsBreachedRejections);
        out.writeVLong(totalLastSuccessfulRequestLimitsBreachedRejections);
        out.writeVLong(totalThroughputDegradationLimitsBreachedRejections);
        out.writeBoolean(shardIndexingPressureEnabled);
        out.writeBoolean(shardIndexingPressureEnforced);
    }

    public IndexingPressurePerShardStats getIndexingPressureShardStats(ShardId shardId) {
        return shardIndexingPressureStore.get(shardId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject("shard_indexing_pressure");
        builder.startObject("stats");
        for (Map.Entry<ShardId, IndexingPressurePerShardStats> entry : shardIndexingPressureStore.entrySet()) {
            entry.getValue().toXContent(builder, params);
        }
        builder.endObject();
        if (shardIndexingPressureEnforced) {
            builder.startObject("total_rejections_breakup");
        } else {
            builder.startObject("total_rejections_breakup_shadow_mode");
        }
        builder.field("node_limits", totalNodeLimitsBreachedRejections);
        builder.field("no_successful_request_limits", totalLastSuccessfulRequestLimitsBreachedRejections);
        builder.field("throughput_degradation_limits", totalThroughputDegradationLimitsBreachedRejections);
        builder.endObject();
        builder.field("enabled", shardIndexingPressureEnabled);
        builder.field("enforced", shardIndexingPressureEnforced);
        return builder.endObject();
    }

    public void addAll(ShardIndexingPressureStats shardIndexingPressureStats) {
        if (this.shardIndexingPressureStore != null) {
            this.shardIndexingPressureStore.putAll(shardIndexingPressureStats.shardIndexingPressureStore);
        }
    }
}
