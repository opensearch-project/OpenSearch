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
import java.util.Map;

/**
 * Transport response for retrieving ingestion state.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class GetIngestionStateResponse extends BroadcastResponse {
    private static final String INGESTION_STATE = "ingestion_state";

    private ShardIngestionState[] shardStates;

    public GetIngestionStateResponse(StreamInput in) throws IOException {
        super(in);
        shardStates = in.readArray(ShardIngestionState::new, ShardIngestionState[]::new);
    }

    public GetIngestionStateResponse(
        ShardIngestionState[] shardStates,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shardStates = shardStates;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray(shardStates);
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        super.addCustomXContentFields(builder, params);
        Map<String, List<ShardIngestionState>> shardStateByIndex = ShardIngestionState.groupShardStateByIndex(shardStates);
        builder.startObject(INGESTION_STATE);

        for (Map.Entry<String, List<ShardIngestionState>> indexShardIngestionStateEntry : shardStateByIndex.entrySet()) {
            builder.startArray(indexShardIngestionStateEntry.getKey());
            for (ShardIngestionState shardIngestionState : indexShardIngestionStateEntry.getValue()) {
                shardIngestionState.toXContent(builder, params);
            }
            builder.endArray();
        }

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
