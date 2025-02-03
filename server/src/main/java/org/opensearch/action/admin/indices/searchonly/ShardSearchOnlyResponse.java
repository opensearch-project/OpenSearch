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
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;

public class ShardSearchOnlyResponse implements Writeable {
    private final ShardId shardId;
    private final boolean needsSync;
    private final int uncommittedOperations;

    public ShardSearchOnlyResponse(ShardId shardId, boolean needsSync, int uncommittedOperations) {
        this.shardId = shardId;
        this.needsSync = needsSync;
        this.uncommittedOperations = uncommittedOperations;
    }

    public ShardSearchOnlyResponse(StreamInput in) throws IOException {
        this.shardId = new ShardId(in);
        this.needsSync = in.readBoolean();
        this.uncommittedOperations = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeBoolean(needsSync);
        out.writeVInt(uncommittedOperations);
    }

    public ShardId getShardId() {
        return shardId;
    }

    public boolean needsSync() {
        return needsSync;
    }

    public boolean hasUncommittedOperations() {
        return uncommittedOperations > 0;
    }
}
