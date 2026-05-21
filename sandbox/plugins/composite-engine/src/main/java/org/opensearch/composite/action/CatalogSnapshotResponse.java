/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action;

import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Response for catalog snapshot containing per-shard results.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CatalogSnapshotResponse extends BroadcastResponse {

    private final List<CatalogSnapshotShardResult> shardResults;
    private final boolean shardLevel;

    public CatalogSnapshotResponse(
        List<CatalogSnapshotShardResult> shardResults,
        boolean shardLevel,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shardResults = shardResults;
        this.shardLevel = shardLevel;
    }

    public CatalogSnapshotResponse(StreamInput in) throws IOException {
        super(in);
        this.shardLevel = in.readBoolean();
        int size = in.readVInt();
        this.shardResults = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            shardResults.add(new CatalogSnapshotShardResult(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(shardLevel);
        out.writeVInt(shardResults.size());
        for (CatalogSnapshotShardResult result : shardResults) {
            result.writeTo(out);
        }
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        if (shardLevel) {
            builder.startArray("shards");
            for (CatalogSnapshotShardResult result : shardResults) {
                builder.startObject();
                builder.field("shard", result.getShardRouting().shardId().id());
                builder.field("primary", result.getShardRouting().primary());
                // Embed the pre-built snapshot XContent
                try (
                    XContentParser parser = XContentType.JSON.xContent()
                        .createParser(
                            org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                            org.opensearch.core.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS,
                            BytesReference.toBytes(result.getSnapshotBytes())
                        )
                ) {
                    builder.field("snapshot");
                    builder.copyCurrentStructure(parser);
                }
                builder.endObject();
            }
            builder.endArray();
        } else {
            // Index-level: just use the first shard result (primary)
            if (!shardResults.isEmpty()) {
                CatalogSnapshotShardResult result = shardResults.get(0);
                try (
                    XContentParser parser = XContentType.JSON.xContent()
                        .createParser(
                            org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                            org.opensearch.core.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS,
                            BytesReference.toBytes(result.getSnapshotBytes())
                        )
                ) {
                    builder.field("snapshot");
                    builder.copyCurrentStructure(parser);
                }
            }
        }
    }
}
