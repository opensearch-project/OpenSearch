/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats.transport;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.plugin.stats.DataFormatShardStats;

import java.io.IOException;

/**
 * Per-shard result holding typed stats produced by the format's provider.
 *
 * @param <T> concrete shard-stats type
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class FormatStatsShardResult<T extends DataFormatShardStats<T>> implements Writeable {

    private final ShardRouting shardRouting;
    private final T stats;

    public FormatStatsShardResult(ShardRouting shardRouting, T stats) {
        this.shardRouting = shardRouting;
        this.stats = stats;
    }

    public FormatStatsShardResult(StreamInput in, Writeable.Reader<T> reader) throws IOException {
        this.shardRouting = new ShardRouting(in);
        if (in.readBoolean()) {
            this.stats = reader.read(in);
        } else {
            this.stats = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardRouting.writeTo(out);
        if (stats != null) {
            out.writeBoolean(true);
            stats.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    public ShardRouting shardRouting() {
        return shardRouting;
    }

    public T stats() {
        return stats;
    }
}
