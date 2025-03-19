/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.state;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Represents ingestion shard state.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ShardIngestionState implements Writeable, ToXContentFragment {
    private static final String SHARD = "shard";
    private static final String POLLER_STATE = "poller_state";
    private static final String ERROR_POLICY = "error_policy";

    private int shardId;
    @Nullable
    private String pollerState;
    @Nullable
    private String errorPolicy;

    public ShardIngestionState() {
        shardId = -1;
    }

    public ShardIngestionState(StreamInput in) throws IOException {
        shardId = in.readInt();
        pollerState = in.readString();
        errorPolicy = in.readString();
    }

    public ShardIngestionState(int shardId, @Nullable String pollerState, @Nullable String errorPolicy) {
        this.shardId = shardId;
        this.pollerState = pollerState;
        this.errorPolicy = errorPolicy;
    }

    public int getShardId() {
        return shardId;
    }

    public String getPollerState() {
        return pollerState;
    }

    public String getErrorPolicy() {
        return errorPolicy;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(shardId);
        out.writeString(pollerState);
        out.writeString(errorPolicy);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SHARD, shardId);
        builder.field(POLLER_STATE, pollerState);
        builder.field(ERROR_POLICY, errorPolicy);
        builder.endObject();
        return builder;
    }
}
