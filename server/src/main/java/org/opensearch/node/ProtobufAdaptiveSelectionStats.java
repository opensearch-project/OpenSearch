/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.node;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Class representing statistics about adaptive replica selection. This includes
* EWMA of queue size, service time, and response time, as well as outgoing
* searches to each node and the "rank" based on the ARS formula.
*
* @opensearch.internal
*/
public class ProtobufAdaptiveSelectionStats implements ProtobufWriteable, ToXContentFragment {

    private final Map<String, Long> clientOutgoingConnections;
    private final Map<String, ResponseCollectorService.ProtobufComputedNodeStats> nodeComputedStats;

    public ProtobufAdaptiveSelectionStats(
        Map<String, Long> clientConnections,
        Map<String, ResponseCollectorService.ProtobufComputedNodeStats> nodeComputedStats
    ) {
        this.clientOutgoingConnections = clientConnections;
        this.nodeComputedStats = nodeComputedStats;
    }

    public ProtobufAdaptiveSelectionStats(CodedInputStream in) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        this.clientOutgoingConnections = protobufStreamInput.readMap(CodedInputStream::readString, CodedInputStream::readInt64);
        this.nodeComputedStats = protobufStreamInput.readMap(
            CodedInputStream::readString,
            ResponseCollectorService.ProtobufComputedNodeStats::new
        );
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        protobufStreamOutput.writeMap(
            this.clientOutgoingConnections,
            CodedOutputStream::writeStringNoTag,
            CodedOutputStream::writeInt64NoTag
        );
        protobufStreamOutput.writeMap(
            this.nodeComputedStats,
            CodedOutputStream::writeStringNoTag,
            (stream, stats) -> stats.writeTo(stream)
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("adaptive_selection");
        Set<String> allNodeIds = Sets.union(clientOutgoingConnections.keySet(), nodeComputedStats.keySet());
        for (String nodeId : allNodeIds) {
            builder.startObject(nodeId);
            ResponseCollectorService.ProtobufComputedNodeStats stats = nodeComputedStats.get(nodeId);
            if (stats != null) {
                long outgoingSearches = clientOutgoingConnections.getOrDefault(nodeId, 0L);
                builder.field("outgoing_searches", outgoingSearches);
                builder.field("avg_queue_size", stats.queueSize);
                if (builder.humanReadable()) {
                    builder.field("avg_service_time", new TimeValue((long) stats.serviceTime, TimeUnit.NANOSECONDS).toString());
                }
                builder.field("avg_service_time_ns", (long) stats.serviceTime);
                if (builder.humanReadable()) {
                    builder.field("avg_response_time", new TimeValue((long) stats.responseTime, TimeUnit.NANOSECONDS).toString());
                }
                builder.field("avg_response_time_ns", (long) stats.responseTime);
                builder.field("rank", String.format(Locale.ROOT, "%.1f", stats.rank(outgoingSearches)));
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    /**
     * Returns a map of node id to the outgoing search requests to that node
    */
    public Map<String, Long> getOutgoingConnections() {
        return clientOutgoingConnections;
    }

    /**
     * Returns a map of node id to the computed stats
    */
    public Map<String, ResponseCollectorService.ProtobufComputedNodeStats> getComputedStats() {
        return nodeComputedStats;
    }

    /**
     * Returns a map of node id to the ranking of the nodes based on the adaptive replica formula
    */
    public Map<String, Double> getRanks() {
        return nodeComputedStats.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().rank(clientOutgoingConnections.getOrDefault(e.getKey(), 0L))));
    }
}
