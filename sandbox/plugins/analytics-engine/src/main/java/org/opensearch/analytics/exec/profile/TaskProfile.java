/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.profile;

import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Per-task profile snapshot. Captures identity, target node, terminal state and
 * wall-clock timing. A "task" is one dispatch unit within a stage (one shard for
 * SOURCE, one partition for HASH_PARTITIONED, one total for COORDINATOR).
 *
 * @param stageId id of the owning stage
 * @param partitionId ordinal of the task within its stage (0-based)
 * @param node target node id the task ran on, or "(unresolved)" if dispatch never happened
 * @param state terminal state — CREATED if the task was never dispatched
 * @param startMs wall-clock millis of the first RUNNING transition, 0 if never dispatched
 * @param endMs wall-clock millis of the first terminal transition, 0 if still running
 * @param elapsedMs {@code endMs - startMs}, or 0 if either stamp is missing
 */
public record TaskProfile(int stageId, int partitionId, String node, String state, long startMs, long endMs, long elapsedMs)
    implements
        ToXContentObject {

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("partition_id", partitionId);
        builder.field("node", node);
        builder.field("state", state);
        if (startMs > 0) builder.field("start_ms", startMs);
        if (endMs > 0) builder.field("end_ms", endMs);
        builder.field("elapsed_ms", elapsedMs);
        builder.endObject();
        return builder;
    }
}
