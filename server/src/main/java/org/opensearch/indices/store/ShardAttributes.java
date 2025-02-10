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

import java.io.IOException;

/**
 * This class contains Attributes related to Shards that are necessary for making the
 * {@link org.opensearch.gateway.TransportNodesListGatewayStartedShards} transport requests
 *
 * @opensearch.internal
 */
public class ShardAttributes implements Writeable {
    @Nullable
    private final String customDataPath;

    public ShardAttributes(String customDataPath) {
        this.customDataPath = customDataPath;
    }

    public ShardAttributes(StreamInput in) throws IOException {
        customDataPath = in.readString();
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
        out.writeString(customDataPath);
    }

    @Override
    public String toString() {
        return "ShardAttributes{" + ", customDataPath='" + customDataPath + '\'' + '}';
    }
}
