/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Request for DataFusion cluster stats that targets one or more nodes.
 *
 * <p>Extends {@link BaseNodesRequest} to carry an optional set of stat section
 * names to retrieve. When {@code statsToRetrieve} is null or empty, all stat
 * sections are returned.
 *
 * @opensearch.internal
 */
public class DataFusionStatsNodesRequest extends BaseNodesRequest<DataFusionStatsNodesRequest> {

    private final Set<String> statsToRetrieve;

    /**
     * Creates a new request targeting the specified nodes with an optional stat filter.
     *
     * @param nodesIds       the node IDs to target (null or empty means all nodes)
     * @param statsToRetrieve the stat section names to retrieve (null or empty means all stats)
     */
    public DataFusionStatsNodesRequest(String[] nodesIds, Set<String> statsToRetrieve) {
        super(nodesIds);
        this.statsToRetrieve = statsToRetrieve != null ? new HashSet<>(statsToRetrieve) : new HashSet<>();
    }

    /**
     * Deserialization constructor.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs
     */
    public DataFusionStatsNodesRequest(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        this.statsToRetrieve = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            statsToRetrieve.add(in.readString());
        }
    }

    /**
     * Returns the set of stat section names to retrieve.
     * An empty set indicates all stats should be returned.
     *
     * @return the stat section names to retrieve
     */
    public Set<String> getStatsToRetrieve() {
        return statsToRetrieve;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(statsToRetrieve.size());
        for (String stat : statsToRetrieve) {
            out.writeString(stat);
        }
    }
}
