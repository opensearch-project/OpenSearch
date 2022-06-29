/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.index.shard.ShardId;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

public class PrepareShardRequest extends TransportRequest {

    private final ShardId shardId;

    protected PrepareShardRequest(ShardId shardId) {
        this.shardId = shardId;
    }

    protected PrepareShardRequest(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
    }

    public ShardId getShardId() {
        return shardId;
    }
}
