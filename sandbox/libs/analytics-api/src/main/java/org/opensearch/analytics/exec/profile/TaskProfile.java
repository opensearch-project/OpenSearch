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
 * Per-task profile snapshot. Captures target node, terminal state and
 * wall-clock elapsed time. A "task" is one dispatch unit within a stage
 * (one shard for SOURCE, one partition for HASH_PARTITIONED, one total for COORDINATOR).
 *
 * @param node target node and shard the task ran on, or "(unknown)" if dispatch never happened
 * @param state terminal state — CREATED if the task was never dispatched
 * @param elapsedMs wall-clock time from dispatch to terminal, or 0 if never dispatched
 */
public record TaskProfile(String node, String state, long elapsedMs) implements ToXContentObject {

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("node", node);
        builder.field("state", state);
        builder.field("elapsed_ms", elapsedMs);
        builder.endObject();
        return builder;
    }
}
