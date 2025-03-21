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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private static final String POLLER_PAUSED = "poller_paused";

    private String index;
    private int shardId;
    @Nullable
    private String pollerState;
    @Nullable
    private String errorPolicy;

    // Indicates if the poller is paused. This can be different from pollerState if the poller is yet to update its
    // state after being paused.
    private boolean isPollerPaused;

    public ShardIngestionState() {
        shardId = -1;
    }

    public ShardIngestionState(StreamInput in) throws IOException {
        index = in.readString();
        shardId = in.readInt();
        pollerState = in.readString();
        errorPolicy = in.readString();
        isPollerPaused = in.readBoolean();
    }

    public ShardIngestionState(
        String index,
        int shardId,
        @Nullable String pollerState,
        @Nullable String errorPolicy,
        boolean isPollerPaused
    ) {
        this.index = index;
        this.shardId = shardId;
        this.pollerState = pollerState;
        this.errorPolicy = errorPolicy;
        this.isPollerPaused = isPollerPaused;
    }

    public int getShardId() {
        return shardId;
    }

    public String getPollerState() {
        return pollerState;
    }

    public String getIndex() {
        return index;
    }

    public boolean isPollerPaused() {
        return isPollerPaused;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeInt(shardId);
        out.writeString(pollerState);
        out.writeString(errorPolicy);
        out.writeBoolean(isPollerPaused);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SHARD, shardId);
        builder.field(POLLER_STATE, pollerState);
        builder.field(ERROR_POLICY, errorPolicy);
        builder.field(POLLER_PAUSED, isPollerPaused);
        builder.endObject();
        return builder;
    }

    /**
     * Groups list of ShardIngestionStates by the index name.
     */
    public static Map<String, List<ShardIngestionState>> groupShardStateByIndex(ShardIngestionState[] shardIngestionStates) {
        Map<String, List<ShardIngestionState>> shardIngestionStatesByIndex = new HashMap<>();

        for (ShardIngestionState state : shardIngestionStates) {
            shardIngestionStatesByIndex.computeIfAbsent(state.getIndex(), (index) -> new ArrayList<>());
            shardIngestionStatesByIndex.get(state.getIndex()).add(state);
        }

        return shardIngestionStatesByIndex;
    }
}
