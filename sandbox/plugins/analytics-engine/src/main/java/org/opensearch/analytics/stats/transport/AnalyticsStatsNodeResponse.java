/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.stats.transport;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.analytics.stats.AnalyticsStats;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Per-node payload of {@link AnalyticsStatsResponse}: the local
 * {@link AnalyticsStats} snapshot from this node's
 * {@link org.opensearch.analytics.stats.AnalyticsStatsCollector}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class AnalyticsStatsNodeResponse extends BaseNodeResponse {

    private final AnalyticsStats stats;

    public AnalyticsStatsNodeResponse(DiscoveryNode node, AnalyticsStats stats) {
        super(node);
        this.stats = stats;
    }

    public AnalyticsStatsNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.stats = new AnalyticsStats(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        stats.writeTo(out);
    }

    public AnalyticsStats getStats() {
        return stats;
    }
}
