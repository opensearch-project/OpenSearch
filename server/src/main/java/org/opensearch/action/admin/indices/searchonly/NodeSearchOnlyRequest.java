/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.searchonly;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.List;

public class NodeSearchOnlyRequest extends TransportRequest {
    private final String index;
    private final List<ShardId> shardIds;

    public NodeSearchOnlyRequest(String index, List<ShardId> shardIds) {
        this.index = index;
        this.shardIds = shardIds;
    }

    public NodeSearchOnlyRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.shardIds = in.readList(ShardId::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeList(shardIds);
    }

    public String getIndex() {
        return index;
    }

    public List<ShardId> getShardIds() {
        return shardIds;
    }
}
