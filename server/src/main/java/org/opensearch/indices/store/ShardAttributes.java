/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.store;

import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.gateway.AsyncShardFetch;

import java.io.IOException;

/**
 * This class contains information about the shard that needs to be sent as part of request in Transport Action implementing
 * {@link AsyncShardFetch.Lister} to fetch shard information in async manner
 *
 * @opensearch.internal
 */
public class ShardAttributes implements Writeable {
    private final ShardId shardId;
    @Nullable
    private final String customDataPath;

    public ShardAttributes(ShardId shardId, String customDataPath) {
        this.shardId = shardId;
        this.customDataPath = customDataPath;
    }

    public ShardAttributes(StreamInput in) throws IOException {
        shardId = new ShardId(in);
        customDataPath = in.readString();
    }

    public ShardId getShardId() {
        return shardId;
    }

    /**
     * Returns the custom data path that is used to look up information for this shard.
     * Returns an empty string if no custom data path is used for this index.
     * Returns null if custom data path information is not available (due to BWC).
     */
    @Nullable
    public String getCustomDataPath() {
        return customDataPath;
    }

    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeString(customDataPath);
    }
}
