/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.stats.transport;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Request for {@link AnalyticsStatsAction}. No payload — the response is the
 * full per-node analytics stats rollup. Inherits {@code nodesIds()} filtering
 * from {@link BaseNodesRequest} (not exposed via REST, but available
 * programmatically and supported by the transport-action fan-out).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class AnalyticsStatsRequest extends BaseNodesRequest<AnalyticsStatsRequest> {

    public AnalyticsStatsRequest() {
        super((String[]) null);
    }

    public AnalyticsStatsRequest(String... nodesIds) {
        super(nodesIds);
    }

    public AnalyticsStatsRequest(StreamInput in) throws IOException {
        super(in);
    }
}
