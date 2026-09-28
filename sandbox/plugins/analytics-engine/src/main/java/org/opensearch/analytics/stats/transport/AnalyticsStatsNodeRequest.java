/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.stats.transport;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Per-node request emitted by the coordinator to each fan-out target. The
 * outer {@link AnalyticsStatsRequest} carries no filters today, so this
 * request is empty too — it exists to satisfy
 * {@link org.opensearch.action.support.nodes.TransportNodesAction}'s
 * generic contract.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class AnalyticsStatsNodeRequest extends TransportRequest {

    public AnalyticsStatsNodeRequest() {}

    public AnalyticsStatsNodeRequest(StreamInput in) throws IOException {
        super(in);
    }
}
