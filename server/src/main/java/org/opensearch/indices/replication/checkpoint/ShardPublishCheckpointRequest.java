/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.opensearch.action.support.replication.ReplicationRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.index.shard.ShardId;

import java.io.IOException;

public class ShardPublishCheckpointRequest extends ReplicationRequest<ShardPublishCheckpointRequest> {

    private final PublishCheckpointRequest request;

    public ShardPublishCheckpointRequest(PublishCheckpointRequest request, ShardId shardId) {
        super(shardId);
        this.request = request;
    }

    public ShardPublishCheckpointRequest(StreamInput in) throws IOException {
        super(in);
        this.request = new PublishCheckpointRequest(in);
    }

    PublishCheckpointRequest getRequest() {
        return request;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }

    @Override
    public String toString() {
        return "ShardPublishCheckpointRequest{" + shardId + "}";
    }
}
