/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.action;

import org.opensearch.arrow.search.scan.ArrowScanBatchResponse;
import org.opensearch.arrow.search.scan.ArrowShardStreamHandler;
import org.opensearch.arrow.search.scan.ArrowShardStreamRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.tasks.Task;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportRequestOptions;

/**
 * Opens shard streams over the native Arrow stream transport.
 */
final class FlightShardStreamClient implements ShardStreamClient {

    // No timeout: a shard stream lasts as long as the client reads, which the client idle timeout bounds.
    private static final TransportRequestOptions OPTIONS = TransportRequestOptions.builder()
        .withType(TransportRequestOptions.Type.STREAM)
        .build();

    private final StreamTransportService streamTransportService;

    FlightShardStreamClient(StreamTransportService streamTransportService) {
        this.streamTransportService = streamTransportService;
    }

    @Override
    public void open(
        DiscoveryNode node,
        ArrowShardStreamRequest request,
        Task parentTask,
        StreamTransportResponseHandler<ArrowScanBatchResponse> handler
    ) {
        streamTransportService.sendChildRequest(
            streamTransportService.getConnection(node),
            ArrowShardStreamHandler.ACTION,
            request,
            parentTask,
            OPTIONS,
            handler
        );
    }
}
