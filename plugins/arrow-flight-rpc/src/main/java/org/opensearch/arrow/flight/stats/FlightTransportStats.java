/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Flight transport statistics for a single node
 */
class FlightTransportStats implements Writeable, ToXContentFragment {

    final PerformanceStats performance;
    final ResourceUtilizationStats resourceUtilization;
    final ReliabilityStats reliability;

    public FlightTransportStats(PerformanceStats performance, ResourceUtilizationStats resourceUtilization, ReliabilityStats reliability) {
        this.performance = performance;
        this.resourceUtilization = resourceUtilization;
        this.reliability = reliability;
    }

    public FlightTransportStats(StreamInput in) throws IOException {
        this.performance = new PerformanceStats(in);
        this.resourceUtilization = new ResourceUtilizationStats(in);
        this.reliability = new ReliabilityStats(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        performance.writeTo(out);
        resourceUtilization.writeTo(out);
        reliability.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("flight");
        performance.toXContent(builder, params);
        resourceUtilization.toXContent(builder, params);
        reliability.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

}
