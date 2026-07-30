/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Per-node request sent to each target node during the DataFusion stats fan-out.
 *
 * <p>Carries the stat section filter from the parent {@link DataFusionStatsNodesRequest}
 * so each node knows which sections to collect.
 *
 * @opensearch.internal
 */
public class DataFusionStatsNodeRequest extends TransportRequest {

    private final Set<String> statsToRetrieve;

    /**
     * Constructs a node request from the parent nodes request, extracting the stat filter.
     *
     * @param nodesRequest the parent cluster-level request
     */
    public DataFusionStatsNodeRequest(DataFusionStatsNodesRequest nodesRequest) {
        this.statsToRetrieve = nodesRequest.getStatsToRetrieve();
    }

    /**
     * Deserialization constructor.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs
     */
    public DataFusionStatsNodeRequest(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        if (size == 0) {
            this.statsToRetrieve = Collections.emptySet();
        } else {
            Set<String> stats = new HashSet<>(size);
            for (int i = 0; i < size; i++) {
                stats.add(in.readString());
            }
            this.statsToRetrieve = Collections.unmodifiableSet(stats);
        }
    }

    /**
     * Returns the set of stat section names to retrieve.
     * An empty set indicates all stats should be returned.
     *
     * @return the stat section filter, never null
     */
    public Set<String> getStatsToRetrieve() {
        return statsToRetrieve;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (statsToRetrieve == null || statsToRetrieve.isEmpty()) {
            out.writeVInt(0);
        } else {
            out.writeVInt(statsToRetrieve.size());
            for (String stat : statsToRetrieve) {
                out.writeString(stat);
            }
        }
    }
}
