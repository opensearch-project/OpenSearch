/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Remote Store stats response
 *
 * @opensearch.internal
 */
public class RemoteStoreStatsResponse extends BroadcastResponse {

    private final RemoteStoreStats[] shards;

    public RemoteStoreStatsResponse(StreamInput in) throws IOException {
        super(in);
        shards = in.readArray(RemoteStoreStats::new, RemoteStoreStats[]::new);
    }

    public RemoteStoreStatsResponse(
        RemoteStoreStats[] shards,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shards = shards;
    }

    public RemoteStoreStats[] getShards() {
        return this.shards;
    }

    public RemoteStoreStats getAt(int position) {
        return shards[position];
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray(shards);
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("stats");
        for (RemoteStoreStats shard : shards) {
            shard.toXContent(builder, params);
        }
        builder.endArray();
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this, true, false);
    }
}
