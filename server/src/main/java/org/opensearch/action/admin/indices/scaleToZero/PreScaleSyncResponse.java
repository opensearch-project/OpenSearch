/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scaleToZero;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class PreScaleSyncResponse extends ActionResponse implements ToXContent {
    private final Collection<NodePreScaleSyncResponse> nodeResponses;
    private final String failureReason;
    private final boolean hasFailures;

    public PreScaleSyncResponse(Collection<NodePreScaleSyncResponse> responses) {
        this.nodeResponses = responses;
        this.hasFailures = responses.stream()
            .anyMatch(r -> r.getShardResponses().stream().anyMatch(s -> s.hasUncommittedOperations() || s.needsSync()));
        this.failureReason = buildFailureReason();
    }

    public PreScaleSyncResponse(StreamInput in) throws IOException {
        this.nodeResponses = in.readList(NodePreScaleSyncResponse::new);
        this.hasFailures = in.readBoolean();
        this.failureReason = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(new ArrayList<>(nodeResponses));  // Convert Collection to List
        out.writeBoolean(hasFailures);
        out.writeOptionalString(failureReason);
    }

    public boolean hasFailures() {
        return hasFailures;
    }

    public String getFailureReason() {
        return failureReason;
    }

    private String buildFailureReason() {
        if (!hasFailures) {
            return null;
        }
        StringBuilder reason = new StringBuilder();
        for (NodePreScaleSyncResponse nodeResponse : nodeResponses) {
            for (ShardPreScaleSyncResponse shardResponse : nodeResponse.getShardResponses()) {
                if (shardResponse.hasUncommittedOperations() || shardResponse.needsSync()) {
                    reason.append("Shard ")
                        .append(shardResponse.getShardId())
                        .append(" on node ")
                        .append(nodeResponse.getNode())
                        .append(": ");
                    if (shardResponse.hasUncommittedOperations()) {
                        reason.append("has uncommitted operations ");
                    }
                    if (shardResponse.needsSync()) {
                        reason.append("needs sync ");
                    }
                    reason.append("; ");
                }
            }
        }
        return reason.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("has_failures", hasFailures);
        if (failureReason != null) {
            builder.field("failure_reason", failureReason);
        }
        builder.endObject();
        return builder;
    }
}
