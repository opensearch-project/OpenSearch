/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.state;

import org.opensearch.Version;
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
    private static final String WRITE_BLOCK_ENABLED = "write_block_enabled";
    private static final String BATCH_START_POINTER = "batch_start_pointer";
    private static final String IS_PRIMARY = "is_primary";
    private static final String NODE_NAME = "node";

    private String index;
    private int shardId;
    private String pollerState;
    private String errorPolicy;
    private boolean isPollerPaused;
    boolean isWriteBlockEnabled;
    private String batchStartPointer;
    private boolean isPrimary;
    private String nodeName;

    public ShardIngestionState() {
        this("", -1, "", "", false, false, "", true, "");
    }

    public ShardIngestionState(StreamInput in) throws IOException {
        this.index = in.readString();
        this.shardId = in.readVInt();
        this.pollerState = in.readOptionalString();
        this.errorPolicy = in.readOptionalString();
        this.isPollerPaused = in.readBoolean();
        this.isWriteBlockEnabled = in.readBoolean();
        this.batchStartPointer = in.readString();

        if (in.getVersion().onOrAfter(Version.V_3_3_0)) {
            this.isPrimary = in.readBoolean();
            this.nodeName = in.readString();
        } else {
            // added from version 3.3 onwards
            this.isPrimary = true;
            this.nodeName = "";
        }
    }

    public ShardIngestionState(
        String index,
        int shardId,
        @Nullable String pollerState,
        @Nullable String errorPolicy,
        boolean isPollerPaused,
        boolean isWriteBlockEnabled,
        String batchStartPointer
    ) {
        this(index, shardId, pollerState, errorPolicy, isPollerPaused, isWriteBlockEnabled, batchStartPointer, true, "");
    }

    public ShardIngestionState(
        String index,
        int shardId,
        @Nullable String pollerState,
        @Nullable String errorPolicy,
        boolean isPollerPaused,
        boolean isWriteBlockEnabled,
        String batchStartPointer,
        boolean isPrimary,
        String nodeName
    ) {
        this.index = index;
        this.shardId = shardId;
        this.pollerState = pollerState;
        this.errorPolicy = errorPolicy;
        this.isPollerPaused = isPollerPaused;
        this.isWriteBlockEnabled = isWriteBlockEnabled;
        this.batchStartPointer = batchStartPointer;
        this.isPrimary = isPrimary;
        this.nodeName = nodeName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeVInt(shardId);
        out.writeOptionalString(pollerState);
        out.writeOptionalString(errorPolicy);
        out.writeBoolean(isPollerPaused);
        out.writeBoolean(isWriteBlockEnabled);
        out.writeString(batchStartPointer);

        if (out.getVersion().onOrAfter(Version.V_3_3_0)) {
            // added from version 3.3 onwards
            out.writeBoolean(isPrimary);
            out.writeString(nodeName);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SHARD, shardId);
        builder.field(POLLER_STATE, pollerState);
        builder.field(ERROR_POLICY, errorPolicy);
        builder.field(POLLER_PAUSED, isPollerPaused);
        builder.field(WRITE_BLOCK_ENABLED, isWriteBlockEnabled);
        builder.field(BATCH_START_POINTER, batchStartPointer);
        builder.field(IS_PRIMARY, isPrimary);
        builder.field(NODE_NAME, nodeName);
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

    public String getIndex() {
        return index;
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

    public boolean isPollerPaused() {
        return isPollerPaused;
    }

    public boolean isWriteBlockEnabled() {
        return isWriteBlockEnabled;
    }

    public String getBatchStartPointer() {
        return batchStartPointer;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setPrimary(boolean primary) {
        this.isPrimary = primary;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }
}
