/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.transport;

import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.support.AbstractClient;

/**
 * Client that is aware of remote clusters. Beyond the regular request/response path it exposes
 * {@link #sendStreamRequest} for streaming (Arrow Flight) requests to a remote cluster, so it is a
 * public type callers can hold to reach that streaming path.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class RemoteClusterAwareClient extends AbstractClient {

    private final TransportService service;
    private final String clusterAlias;
    private final RemoteClusterService remoteClusterService;
    // Null when the stream transport is not enabled on this node; sendStreamRequest then throws rather
    // than falling back to the regular request/response path.
    private final StreamTransportService streamTransportService;

    RemoteClusterAwareClient(Settings settings, ThreadPool threadPool, TransportService service, String clusterAlias) {
        this(settings, threadPool, service, clusterAlias, null);
    }

    RemoteClusterAwareClient(
        Settings settings,
        ThreadPool threadPool,
        TransportService service,
        String clusterAlias,
        StreamTransportService streamTransportService
    ) {
        super(settings, threadPool);
        this.service = service;
        this.clusterAlias = clusterAlias;
        this.remoteClusterService = service.getRemoteClusterService();
        this.streamTransportService = streamTransportService;
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        remoteClusterService.ensureConnected(clusterAlias, ActionListener.wrap(v -> {
            Transport.Connection connection;
            if (request instanceof RemoteClusterAwareRequest remoteClusterAwareRequest) {
                DiscoveryNode preferredTargetNode = remoteClusterAwareRequest.getPreferredTargetNode();
                connection = remoteClusterService.getConnection(preferredTargetNode, clusterAlias);
            } else {
                connection = remoteClusterService.getConnection(clusterAlias);
            }
            service.sendRequest(
                connection,
                action.name(),
                request,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(listener, action.getResponseReader())
            );
        }, listener::onFailure));
    }

    /**
     * Streaming sibling of {@link #doExecute}: sends {@code request} to the remote cluster over the
     * stream (Arrow Flight) transport and delivers the response batches to {@code handler}.
     *
     * <p>Semantics mirror {@link #doExecute} exactly — same {@code ensureConnected} on the remote
     * cluster, same node selection ({@link RemoteClusterAwareRequest#getPreferredTargetNode()} when
     * the request implements it, otherwise any connected remote node) — except the request is
     * carried by {@link StreamTransportService} with {@link TransportRequestOptions.Type#STREAM}.
     * Because the stream transport maintains its own connections, the target node is connected on
     * the stream transport (no-op if already connected) before the request is sent.
     *
     * @throws IllegalStateException if the stream transport is not enabled on this node.
     */
    public <T extends TransportResponse> void sendStreamRequest(
        String action,
        TransportRequest request,
        DiscoveryNode targetNode,
        StreamTransportResponseHandler<T> handler
    ) {
        if (streamTransportService == null) {
            throw new IllegalStateException(
                "stream transport is not enabled on this node; cannot open a streaming request to remote cluster [" + clusterAlias + "]"
            );
        }
        remoteClusterService.ensureConnected(clusterAlias, ActionListener.wrap(v -> {
            final DiscoveryNode node = targetNode != null ? targetNode : selectTargetNode(request);
            // The stream transport has its own connection manager, so connect the node there separately
            // (the ensureConnected above only covers the regular transport).
            streamTransportService.connectToNode(node, null, ActionListener.wrap(c -> {
                final TransportRequestOptions options = TransportRequestOptions.builder()
                    .withType(TransportRequestOptions.Type.STREAM)
                    .withTimeout(streamTransportService.getStreamTransportReqTimeout())
                    .build();
                streamTransportService.sendRequest(node, action, request, options, handler);
            }, e -> handler.handleException(wrapAsTransportException(node, e))));
        }, e -> handler.handleException(wrapAsTransportException(null, e))));
    }

    /**
     * Node-selection rule shared with {@link #doExecute}: the request's preferred target when it is a
     * {@link RemoteClusterAwareRequest}, otherwise any connected remote node. ({@code doExecute} keeps
     * its connection-oriented form because it needs a {@link Transport.Connection}, not just the node.)
     */
    private DiscoveryNode selectTargetNode(TransportRequest request) {
        if (request instanceof RemoteClusterAwareRequest remoteClusterAwareRequest) {
            return remoteClusterAwareRequest.getPreferredTargetNode();
        }
        return remoteClusterService.getConnection(clusterAlias).getNode();
    }

    private static TransportException wrapAsTransportException(DiscoveryNode node, Exception e) {
        if (e instanceof TransportException te) {
            return te;
        }
        return node != null ? new ConnectTransportException(node, "failed to open streaming connection", e) : new TransportException(e);
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public Client getRemoteClusterClient(String clusterAlias) {
        return remoteClusterService.getRemoteClusterClient(threadPool(), clusterAlias);
    }
}
