/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scale.searchonly;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Response for search-only scale operations, containing information about shards' synchronization status.
 * <p>
 * This response aggregates the results of shard synchronization attempts from multiple nodes
 * during a scale operation. It tracks:
 * <ul>
 *   <li>Whether any shards have uncommitted operations</li>
 *   <li>Whether any shards still need synchronization</li>
 *   <li>Detailed failure reasons if the scale operation cannot proceed</li>
 * </ul>
 * <p>
 * The response is used by the cluster manager to determine whether a scale operation
 * can be finalized or needs to be retried after more time is allowed for synchronization.
 */
class ScaleIndexResponse extends ActionResponse implements ToXContent {
    private final Collection<ScaleIndexNodeResponse> nodeResponses;
    private final String failureReason;
    private final boolean hasFailures;

    /**
     * Constructs a new SearchOnlyResponse by aggregating responses from multiple nodes.
     * <p>
     * This constructor analyzes the responses to determine if any shards report conditions
     * that would prevent safely finalizing a scale operation, such as uncommitted operations
     * or pending synchronization tasks.
     *
     * @param responses the collection of node responses containing shard status information
     */
    ScaleIndexResponse(Collection<ScaleIndexNodeResponse> responses) {
        this.nodeResponses = responses;
        this.hasFailures = responses.stream()
            .anyMatch(r -> r.getShardResponses().stream().anyMatch(s -> s.hasUncommittedOperations() || s.needsSync()));
        this.failureReason = buildFailureReason();
    }

    /**
     * Serializes this response to the given output stream.
     *
     * @param out the output stream to write to
     * @throws IOException if there is an I/O error during serialization
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(new ArrayList<>(nodeResponses));
        out.writeBoolean(hasFailures);
        out.writeOptionalString(failureReason);
    }

    /**
     * Indicates whether any shards reported conditions that would prevent
     * safely finalizing the scale operation.
     *
     * @return true if any shard has uncommitted operations or needs sync, false otherwise
     */
    boolean hasFailures() {
        return hasFailures;
    }

    /**
     * Builds a detailed description of failure reasons if the scale operation cannot proceed.
     * <p>
     * This method constructs a human-readable string explaining which shards on which nodes
     * reported conditions that prevent the scale operation from being finalized, including
     * whether they have uncommitted operations or need additional synchronization.
     *
     * @return a detailed failure description, or null if no failures were detected
     */
    String buildFailureReason() {
        if (!hasFailures) {
            return null;
        }
        StringBuilder reason = new StringBuilder();
        for (ScaleIndexNodeResponse nodeResponse : nodeResponses) {
            for (ScaleIndexShardResponse shardResponse : nodeResponse.getShardResponses()) {
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

    /**
     * Converts this response to XContent format for API responses.
     * <p>
     * The generated content includes:
     * <ul>
     *   <li>Whether any failures were detected</li>
     *   <li>Detailed failure reasons if applicable</li>
     * </ul>
     *
     * @param builder the XContentBuilder to use
     * @param params  parameters for XContent generation
     * @return the XContentBuilder with response data added
     * @throws IOException if there is an error generating the XContent
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (failureReason != null) {
            builder.field("failure_reason", failureReason);
        }
        builder.endObject();
        return builder;
    }
}
