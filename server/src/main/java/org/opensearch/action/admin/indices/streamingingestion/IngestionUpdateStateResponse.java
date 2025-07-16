/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Transport response for ingestion state updates.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class IngestionUpdateStateResponse extends AcknowledgedResponse {
    protected static final String SHARD_ACK = "shards_acknowledged";
    protected static final String ERROR = "error";
    protected static final String FAILURES = "failures";

    protected boolean shardsAcknowledged;
    protected IngestionStateShardFailure[] shardFailuresList;
    protected String errorMessage;

    public IngestionUpdateStateResponse(StreamInput in) throws IOException {
        super(in);
        shardFailuresList = in.readArray(IngestionStateShardFailure::new, IngestionStateShardFailure[]::new);
        errorMessage = in.readString();
        shardsAcknowledged = in.readBoolean();
    }

    public IngestionUpdateStateResponse(
        final boolean acknowledged,
        final boolean shardsAcknowledged,
        final IngestionStateShardFailure[] shardFailuresList,
        String errorMessage
    ) {
        super(acknowledged);
        this.shardFailuresList = shardFailuresList;
        this.shardsAcknowledged = shardsAcknowledged;
        this.errorMessage = errorMessage;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray(shardFailuresList);
        out.writeString(errorMessage);
        out.writeBoolean(shardsAcknowledged);
    }

    @Override
    protected void addCustomFields(final XContentBuilder builder, final Params params) throws IOException {
        super.addCustomFields(builder, params);
        builder.field(SHARD_ACK, shardsAcknowledged);

        if (Strings.isEmpty(errorMessage) == false) {
            builder.field(ERROR, errorMessage);
        }

        if (shardFailuresList.length > 0) {
            Map<String, List<IngestionStateShardFailure>> shardFailuresByIndex = IngestionStateShardFailure.groupShardFailuresByIndex(
                shardFailuresList
            );
            builder.startObject(FAILURES);
            for (Map.Entry<String, List<IngestionStateShardFailure>> indexShardFailures : shardFailuresByIndex.entrySet()) {
                builder.startArray(indexShardFailures.getKey());
                for (IngestionStateShardFailure shardFailure : indexShardFailures.getValue()) {
                    shardFailure.toXContent(builder, params);
                }
                builder.endArray();
            }
            builder.endObject();
        }
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    public boolean isShardsAcknowledged() {
        return shardsAcknowledged;
    }

    public IngestionStateShardFailure[] getShardFailures() {
        return shardFailuresList;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
