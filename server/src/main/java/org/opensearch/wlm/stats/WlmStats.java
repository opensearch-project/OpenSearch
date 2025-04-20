/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.stats;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * This class contains the stats for Workload Management
 */
public class WlmStats extends BaseNodeResponse implements ToXContentObject, Writeable {
    private final WorkloadGroupStats workloadGroupStats;

    public WlmStats(DiscoveryNode node, WorkloadGroupStats workloadGroupStats) {
        super(node);
        this.workloadGroupStats = workloadGroupStats;
    }

    public WlmStats(StreamInput in) throws IOException {
        super(in);
        workloadGroupStats = new WorkloadGroupStats(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        workloadGroupStats.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return workloadGroupStats.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WlmStats that = (WlmStats) o;
        return Objects.equals(getWorkloadGroupStats(), that.getWorkloadGroupStats());
    }

    @Override
    public int hashCode() {
        return Objects.hash(workloadGroupStats);
    }

    public WorkloadGroupStats getWorkloadGroupStats() {
        return workloadGroupStats;
    }
}
