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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;

import java.util.function.Function;

import static org.opensearch.discovery.HandshakingTransportAddressConnector.PROBE_CONNECT_TIMEOUT_SETTING;
import static org.opensearch.discovery.HandshakingTransportAddressConnector.PROBE_HANDSHAKE_TIMEOUT_SETTING;

/**
 * Transport service for streaming requests, handling StreamTransportResponse.
 * @opensearch.internal
 */
public class StreamTransportService extends TransportService {
    private static final Logger logger = LogManager.getLogger(StreamTransportService.class);
    public static final Setting<TimeValue> STREAM_TRANSPORT_REQ_TIMEOUT_SETTING = Setting.timeSetting(
        "transport.stream.request_timeout",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile TimeValue streamTransportReqTimeout;

    public StreamTransportService(
        Settings settings,
        Transport streamTransport,
        ThreadPool threadPool,
        TransportInterceptor transportInterceptor,
        Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
        @Nullable ClusterSettings clusterSettings,
        TaskManager taskManager,
        RemoteClusterService remoteClusterService,
        Tracer tracer
    ) {
        super(
            settings,
            streamTransport,
            threadPool,
            transportInterceptor,
            localNodeFactory,
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
            tracer,
            taskManager,
            remoteClusterService,
            true
        );
        this.streamTransportReqTimeout = STREAM_TRANSPORT_REQ_TIMEOUT_SETTING.get(settings);
        if (clusterSettings != null) {
            clusterSettings.addSettingsUpdateConsumer(STREAM_TRANSPORT_REQ_TIMEOUT_SETTING, this::setStreamTransportReqTimeout);
        }
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
            TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).withTimeout(streamTransportReqTimeout).build(),
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
        final ActionListener<Void> wrappedListener = ActionListener.wrap(response -> { listener.onResponse(response); }, exception -> {
            logger.warn("Failed to connect to streaming node [{}]: {}", node, exception.getMessage());
            listener.onFailure(new ConnectTransportException(node, "Failed to connect for streaming", exception));
        });

        connectionManager.connectToNode(
            node,
            connectionProfile,
            (connection, profile, listener1) -> listener1.onResponse(null),
            wrappedListener
        );
    }

    @Override
    public Transport.Connection getConnection(DiscoveryNode node) {
        try {
            return connectionManager.getConnection(node);
        } catch (Exception e) {
            logger.error("Failed to get streaming connection to node [{}]: {}", node, e.getMessage());
            throw new ConnectTransportException(node, "Failed to get streaming connection", e);
        }
    }

    private void setStreamTransportReqTimeout(TimeValue streamTransportReqTimeout) {
        this.streamTransportReqTimeout = streamTransportReqTimeout;
    }
}
