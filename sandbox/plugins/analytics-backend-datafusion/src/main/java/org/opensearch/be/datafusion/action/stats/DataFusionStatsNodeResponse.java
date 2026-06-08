/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.be.datafusion.stats.DataFusionStats;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * Per-node response carrying that node's {@link DataFusionStats}.
 * Extends {@link BaseNodeResponse} to participate in the {@code TransportNodesAction} framework.
 *
 * <p>Wire format:
 * <pre>
 * [BaseNodeResponse fields: DiscoveryNode]
 * [DataFusionStats: via existing Writeable implementation]
 * </pre>
 */
public class DataFusionStatsNodeResponse extends BaseNodeResponse {

    private final DataFusionStats stats;

    /**
     * Construct a node response with the given node and stats.
     *
     * @param node  the discovery node this response is from
     * @param stats the DataFusion stats for this node (may be null if stats unavailable)
     */
    public DataFusionStatsNodeResponse(DiscoveryNode node, DataFusionStats stats) {
        super(node);
        this.stats = stats;
    }

    /**
     * Deserialize from stream.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public DataFusionStatsNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.stats = in.readOptionalWriteable(DataFusionStats::new);
    }

    /**
     * Returns the DataFusion stats for this node, or {@code null} if unavailable.
     */
    public DataFusionStats getStats() {
        return stats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(stats);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataFusionStatsNodeResponse that = (DataFusionStatsNodeResponse) o;
        return Objects.equals(getNode(), that.getNode()) && Objects.equals(stats, that.stats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNode(), stats);
    }
}
