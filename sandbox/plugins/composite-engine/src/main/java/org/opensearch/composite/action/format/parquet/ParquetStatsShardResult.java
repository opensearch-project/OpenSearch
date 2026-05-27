/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action.format.parquet;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Per-shard result containing the shard routing and pre-rendered parquet stats JSON bytes.
 * Stats are serialized to JSON on the sender side (where the concrete class is loaded)
 * to avoid classloader issues during transport deserialization.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ParquetStatsShardResult implements Writeable {

    private final ShardRouting shardRouting;
    private final BytesReference statsJsonBytes;

    public ParquetStatsShardResult(ShardRouting shardRouting, org.opensearch.core.xcontent.ToXContentFragment stats) throws IOException {
        this.shardRouting = shardRouting;
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        if (stats != null) {
            stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        builder.endObject();
        this.statsJsonBytes = BytesReference.bytes(builder);
    }

    public ParquetStatsShardResult(StreamInput in) throws IOException {
        this.shardRouting = new ShardRouting(in);
        this.statsJsonBytes = in.readBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardRouting.writeTo(out);
        out.writeBytesReference(statsJsonBytes);
    }

    public ShardRouting getShardRouting() {
        return shardRouting;
    }

    public BytesReference getStatsJsonBytes() {
        return statsJsonBytes;
    }
}
