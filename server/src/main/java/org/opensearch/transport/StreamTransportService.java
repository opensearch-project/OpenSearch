/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;

import java.util.Set;
import java.util.function.Function;

import static org.opensearch.discovery.HandshakingTransportAddressConnector.PROBE_CONNECT_TIMEOUT_SETTING;
import static org.opensearch.discovery.HandshakingTransportAddressConnector.PROBE_HANDSHAKE_TIMEOUT_SETTING;

/**
 * Transport service for streaming requests, handling StreamTransportResponse.
 * @opensearch.internal
 */
public class StreamTransportService extends TransportService {
    private static final Logger logger = LogManager.getLogger(StreamTransportService.class);

    public StreamTransportService(
        Settings settings,
        Transport streamTransport,
        ThreadPool threadPool,
        TransportInterceptor transportInterceptor,
        Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
        @Nullable ClusterSettings clusterSettings,
        Set<String> taskHeaders,
        Tracer tracer
    ) {
        super(
            settings,
            streamTransport,
            threadPool,
            transportInterceptor,
            localNodeFactory,
            clusterSettings,
            taskHeaders,
            // it's a single channel profile and let underlying client handle parallelism by creating multiple channels as needed
            new ClusterConnectionManager(
                ConnectionProfile.buildSingleChannelProfile(
                    TransportRequestOptions.Type.STREAM,
                    PROBE_CONNECT_TIMEOUT_SETTING.get(settings),
                    PROBE_HANDSHAKE_TIMEOUT_SETTING.get(settings),
                    TimeValue.MINUS_ONE,
                    false
                ),
                streamTransport
            ),
            tracer
        );
    }

    @Override
    public <T extends TransportResponse> void sendChildRequest(
        final Transport.Connection connection,
        final String action,
        final TransportRequest request,
        final Task parentTask,
        final TransportResponseHandler<T> handler
    ) {
        sendChildRequest(
            connection,
            action,
            request,
            parentTask,
            TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
            handler
        );
    }

    @Override
    public void connectToNode(final DiscoveryNode node, ConnectionProfile connectionProfile, ActionListener<Void> listener) {
        if (isLocalNode(node)) {
            listener.onResponse(null);
            return;
        }
        // TODO: add logic for validation
        connectionManager.connectToNode(node, connectionProfile, (connection, profile, listener1) -> listener1.onResponse(null), listener);
    }

    @Override
    public Transport.Connection getConnection(DiscoveryNode node) {
        // no direct channel for local node
        // TODO: add support for direct channel for streaming
        return connectionManager.getConnection(node);
    }
}
