/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.composite.stats.CompositeShardStats;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Per-shard result containing the shard routing and composite stats.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DataFormatStatsShardResult implements Writeable {

    private final ShardRouting shardRouting;
    private final CompositeShardStats stats;

    public DataFormatStatsShardResult(ShardRouting shardRouting, CompositeShardStats stats) {
        this.shardRouting = shardRouting;
        this.stats = stats;
    }

    public DataFormatStatsShardResult(StreamInput in) throws IOException {
        this.shardRouting = new ShardRouting(in);
        this.stats = new CompositeShardStats(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardRouting.writeTo(out);
        stats.writeTo(out);
    }

    public ShardRouting getShardRouting() {
        return shardRouting;
    }

    public CompositeShardStats getStats() {
        return stats;
    }
}
