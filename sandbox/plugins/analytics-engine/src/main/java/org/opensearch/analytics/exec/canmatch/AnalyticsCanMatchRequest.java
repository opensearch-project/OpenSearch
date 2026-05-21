/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Lightweight request sent to each target shard before fragment execution.
 * Carries the filter predicates (as serialized Substrait bytes) and the
 * target shard ID. The data node checks Parquet metadata stats against
 * the predicates without opening any data pages.
 */
public class AnalyticsCanMatchRequest extends TransportRequest {

    private final ShardId shardId;
    private final byte[] filterBytes;
    private final String backendId;

    public AnalyticsCanMatchRequest(ShardId shardId, byte[] filterBytes, String backendId) {
        this.shardId = shardId;
        this.filterBytes = filterBytes;
        this.backendId = backendId;
    }

    public AnalyticsCanMatchRequest(StreamInput in) throws IOException {
        super(in);
        this.shardId = new ShardId(in);
        this.filterBytes = in.readByteArray();
        this.backendId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeByteArray(filterBytes);
        out.writeString(backendId);
    }

    public ShardId getShardId() {
        return shardId;
    }

    public byte[] getFilterBytes() {
        return filterBytes;
    }

    public String getBackendId() {
        return backendId;
    }
}
