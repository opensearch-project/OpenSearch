/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RemoteStoreStatsResponse extends BroadcastResponse {

    private final RemoteStoreStats[] shards;

    public RemoteStoreStatsResponse(StreamInput in) throws IOException {
        super(in);
        shards = in.readArray(RemoteStoreStats::new, RemoteStoreStats[]::new);
    }

    RemoteStoreStatsResponse(
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
        final String level = params.param("level", "indices");
        final boolean isLevelValid = "cluster".equalsIgnoreCase(level)
            || "indices".equalsIgnoreCase(level)
            || "shards".equalsIgnoreCase(level);
        if (!isLevelValid) {
            throw new IllegalArgumentException("level parameter must be one of [cluster] or [indices] or [shards] but was [" + level + "]");
        }
        builder.startArray("stats");
        Arrays.stream(shards).forEach(shard -> {
            try {
                shard.toXContent(builder, params);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        builder.endArray();
    }

    /**
     * Fields for parsing and toXContent
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String INDICES = "indices";
        static final String SHARDS = "shards";
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this, true, false);
    }
}
