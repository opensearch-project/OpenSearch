/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.stats;

import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.index.shard.ShardId;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * ProtobufIndexShardStats for OpenSearch
 *
 * @opensearch.internal
 */
public class ProtobufIndexShardStats implements Iterable<ProtobufShardStats>, ProtobufWriteable {

    private final ShardId shardId;

    private final ProtobufShardStats[] shards;

    public ProtobufIndexShardStats(CodedInputStream in) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        shardId = new ShardId(in);
        shards = protobufStreamInput.readArray(ProtobufShardStats::new, ProtobufShardStats[]::new);
    }

    public ProtobufIndexShardStats(ShardId shardId, ProtobufShardStats[] shards) {
        this.shardId = shardId;
        this.shards = shards;
    }

    public ShardId getShardId() {
        return this.shardId;
    }

    public ProtobufShardStats[] getShards() {
        return shards;
    }

    public ProtobufShardStats getAt(int position) {
        return shards[position];
    }

    @Override
    public Iterator<ProtobufShardStats> iterator() {
        return Arrays.stream(shards).iterator();
    }

    private ProtobufCommonStats total = null;

    public ProtobufCommonStats getTotal() {
        if (total != null) {
            return total;
        }
        ProtobufCommonStats stats = new ProtobufCommonStats();
        for (ProtobufShardStats shard : shards) {
            stats.add(shard.getStats());
        }
        total = stats;
        return stats;
    }

    private ProtobufCommonStats primary = null;

    public ProtobufCommonStats getPrimary() {
        if (primary != null) {
            return primary;
        }
        ProtobufCommonStats stats = new ProtobufCommonStats();
        for (ProtobufShardStats shard : shards) {
            if (shard.getShardRouting().primary()) {
                stats.add(shard.getStats());
            }
        }
        primary = stats;
        return stats;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        shardId.writeTo(out);
        protobufStreamOutput.writeArray((o, v) -> v.writeTo(o), shards);
    }
}
