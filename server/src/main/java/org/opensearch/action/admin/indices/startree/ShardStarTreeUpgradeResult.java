/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.startree;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;

/**
 * Result for a star tree upgrade operation on a single shard.
 * Contains the shard identifier and whether it was a primary shard.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ShardStarTreeUpgradeResult implements Writeable {

    private final ShardId shardId;
    private final boolean primary;

    ShardStarTreeUpgradeResult(ShardId shardId, boolean primary) {
        this.shardId = shardId;
        this.primary = primary;
    }

    ShardStarTreeUpgradeResult(StreamInput in) throws IOException {
        this.shardId = new ShardId(in);
        this.primary = in.readBoolean();
    }

    public ShardId getShardId() {
        return shardId;
    }

    public boolean primary() {
        return primary;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeBoolean(primary);
    }
}
