/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.state;

import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Transport response for retrieving ingestion state.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class GetIngestionStateResponse extends BroadcastResponse {
    private static final String INGESTION_STATE = "ingestion_state";

    private String index;

    private ShardIngestionState[] shardStates;

    public GetIngestionStateResponse(StreamInput in) throws IOException {
        super(in);
        index = in.readString();
        shardStates = in.readArray(ShardIngestionState::new, ShardIngestionState[]::new);
    }

    public GetIngestionStateResponse(
        String index,
        ShardIngestionState[] shardStates,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.index = index;
        this.shardStates = shardStates;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeArray(shardStates);
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(index);
        builder.startArray(INGESTION_STATE);
        for (ShardIngestionState shardState : shardStates) {
            shardState.toXContent(builder, params);
        }

        builder.endArray();
        builder.endObject();
    }

    public ShardIngestionState[] getShardStates() {
        return shardStates;
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, true, false);
    }
}
