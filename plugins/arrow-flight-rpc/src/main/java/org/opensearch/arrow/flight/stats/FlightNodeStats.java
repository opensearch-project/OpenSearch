/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.stats;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Flight transport statistics for a single node
 */
class FlightNodeStats extends BaseNodeResponse {

    private final FlightTransportStats flightStats;

    public FlightNodeStats(StreamInput in) throws IOException {
        super(in);
        this.flightStats = new FlightTransportStats(in);
    }

    public FlightNodeStats(DiscoveryNode node, FlightTransportStats flightStats) {
        super(node);
        this.flightStats = flightStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        flightStats.writeTo(out);
    }

    public FlightTransportStats getFlightStats() {
        return flightStats;
    }
}
