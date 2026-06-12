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
import java.util.Map;

/**
 * Per-task profile snapshot. Captures target node, terminal state,
 * wall-clock elapsed time, and optional data-node execution metrics.
 *
 * @param node target node and shard the task ran on, or "(unknown)" if dispatch never happened
 * @param state terminal state — CREATED if the task was never dispatched
 * @param elapsedMs wall-clock time from dispatch to terminal, or 0 if never dispatched
 * @param dataNodeMetrics execution metrics from the data node (DataFusion operator timings), or null if not profiled
 */
public record TaskProfile(String node, String state, long elapsedMs, Map<String, Long> dataNodeMetrics) implements ToXContentObject {

    /** Convenience constructor without data node metrics. */
    public TaskProfile(String node, String state, long elapsedMs) {
        this(node, state, elapsedMs, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("node", node);
        builder.field("state", state);
        builder.field("elapsed_ms", elapsedMs);
        if (dataNodeMetrics != null && dataNodeMetrics.isEmpty() == false) {
            builder.startObject("data_node_metrics");
            for (Map.Entry<String, Long> entry : dataNodeMetrics.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
