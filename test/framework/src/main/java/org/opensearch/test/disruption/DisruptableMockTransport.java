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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.test.disruption;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.coordination.DeterministicTaskQueue;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.test.transport.MockTransport;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.CloseableConnection;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.ConnectionProfile;
import org.opensearch.transport.RequestHandlerRegistry;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.opensearch.test.OpenSearchTestCase.copyWriteable;

public abstract class DisruptableMockTransport extends MockTransport {
    private final DiscoveryNode localNode;
    private final Logger logger;
    private final DeterministicTaskQueue deterministicTaskQueue;

    public DisruptableMockTransport(DiscoveryNode localNode, Logger logger, DeterministicTaskQueue deterministicTaskQueue) {
        this.localNode = localNode;
        this.logger = logger;
        this.deterministicTaskQueue = deterministicTaskQueue;
    }

    protected abstract ConnectionStatus getConnectionStatus(DiscoveryNode destination);

    protected abstract Optional<DisruptableMockTransport> getDisruptableMockTransport(TransportAddress address);

    protected abstract void execute(Runnable runnable);

    public DiscoveryNode getLocalNode() {
        return localNode;
    }

    @Override
    public TransportService createTransportService(
        Settings settings,
        ThreadPool threadPool,
        TransportInterceptor interceptor,
        Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
        @Nullable ClusterSettings clusterSettings,
        Set<String> taskHeaders
    ) {
        return new TransportService(settings, this, threadPool, interceptor, localNodeFactory, clusterSettings, taskHeaders);
    }

    @Override
    public void openConnection(DiscoveryNode node, ConnectionProfile profile, ActionListener<Connection> listener) {
        final Optional<DisruptableMockTransport> optionalMatchingTransport = getDisruptableMockTransport(node.getAddress());
        if (optionalMatchingTransport.isPresent()) {
            final DisruptableMockTransport matchingTransport = optionalMatchingTransport.get();
            final ConnectionStatus connectionStatus = getConnectionStatus(matchingTransport.getLocalNode());
            if (connectionStatus != ConnectionStatus.CONNECTED) {
                listener.onFailure(
                    new ConnectTransportException(node, "node [" + node + "] is [" + connectionStatus + "] not [CONNECTED]")
                );
            } else {
                listener.onResponse(new CloseableConnection() {
                    @Override
                    public DiscoveryNode getNode() {
                        return node;
                    }

                    @Override
                    public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                        throws TransportException {
                        onSendRequest(requestId, action, request, matchingTransport);
                    }
                });
            }
        } else {
            listener.onFailure(new ConnectTransportException(node, "node " + node + " does not exist"));
        }
    }

    protected void onSendRequest(long requestId, String action, TransportRequest request, DisruptableMockTransport destinationTransport) {

        assert destinationTransport.getLocalNode().equals(getLocalNode()) == false : "non-local message from "
            + getLocalNode()
            + " to itself";

        destinationTransport.execute(new Runnable() {
            @Override
            public void run() {
                final ConnectionStatus connectionStatus = getConnectionStatus(destinationTransport.getLocalNode());
                switch (connectionStatus) {
                    case BLACK_HOLE:
                    case BLACK_HOLE_REQUESTS_ONLY:
                        onBlackholedDuringSend(requestId, action, destinationTransport);
                        break;

                    case DISCONNECTED:
                        onDisconnectedDuringSend(requestId, action, destinationTransport);
                        break;

                    case CONNECTED:
                        onConnectedDuringSend(requestId, action, request, destinationTransport);
                        break;

                    default:
                        throw new AssertionError("unexpected status: " + connectionStatus);
                }
            }

            @Override
            public String toString() {
                return getRequestDescription(requestId, action, destinationTransport.getLocalNode());
            }
        });
    }

    protected Runnable getDisconnectException(long requestId, String action, DiscoveryNode destination) {
        return new Runnable() {
            @Override
            public void run() {
                handleError(requestId, new ConnectTransportException(destination, "disconnected"));
            }

            @Override
            public String toString() {
                return "disconnection response to " + getRequestDescription(requestId, action, destination);
            }
        };
    }

    protected String getRequestDescription(long requestId, String action, DiscoveryNode destination) {
        return new ParameterizedMessage("[{}][{}] from {} to {}", requestId, action, getLocalNode(), destination).getFormattedMessage();
    }

    protected void onBlackholedDuringSend(long requestId, String action, DisruptableMockTransport destinationTransport) {
        logger.trace("dropping {}", getRequestDescription(requestId, action, destinationTransport.getLocalNode()));
        // Delaying the request for one day and then disconnect to simulate a very long pause
        deterministicTaskQueue.scheduleAt(
            deterministicTaskQueue.getCurrentTimeMillis() + TimeUnit.DAYS.toMillis(1L),
            () -> onDisconnectedDuringSend(requestId, action, destinationTransport)
        );
    }

    protected void onDisconnectedDuringSend(long requestId, String action, DisruptableMockTransport destinationTransport) {
        destinationTransport.execute(getDisconnectException(requestId, action, destinationTransport.getLocalNode()));
    }

    protected void onConnectedDuringSend(
        long requestId,
        String action,
        TransportRequest request,
        DisruptableMockTransport destinationTransport
    ) {
        final RequestHandlerRegistry<TransportRequest> requestHandler = destinationTransport.getRequestHandlers().getHandler(action);

        final DiscoveryNode destination = destinationTransport.getLocalNode();

        final String requestDescription = getRequestDescription(requestId, action, destination);

        final TransportChannel transportChannel = new TransportChannel() {
            @Override
            public String getProfileName() {
                return "default";
            }

            @Override
            public String getChannelType() {
                return "disruptable-mock-transport-channel";
            }

            @Override
            public void sendResponse(final TransportResponse response) {
                execute(new Runnable() {
                    @Override
                    public void run() {
                        final ConnectionStatus connectionStatus = destinationTransport.getConnectionStatus(getLocalNode());
                        switch (connectionStatus) {
                            case CONNECTED:
                            case BLACK_HOLE_REQUESTS_ONLY:
                                handleResponse(requestId, response);
                                break;

                            case BLACK_HOLE:
                            case DISCONNECTED:
                                logger.trace("delaying response to {}: channel is {}", requestDescription, connectionStatus);
                                onBlackholedDuringSend(requestId, action, destinationTransport);
                                break;

                            default:
                                throw new AssertionError("unexpected status: " + connectionStatus);
                        }
                    }

                    @Override
                    public String toString() {
                        return "response to " + requestDescription;
                    }
                });
            }

            @Override
            public void sendResponse(Exception exception) {

                execute(new Runnable() {
                    @Override
                    public void run() {
                        final ConnectionStatus connectionStatus = destinationTransport.getConnectionStatus(getLocalNode());
                        switch (connectionStatus) {
                            case CONNECTED:
                            case BLACK_HOLE_REQUESTS_ONLY:
                                handleRemoteError(requestId, exception);
                                break;

                            case BLACK_HOLE:
                            case DISCONNECTED:
                                logger.trace("delaying exception response to {}: channel is {}", requestDescription, connectionStatus);
                                onBlackholedDuringSend(requestId, action, destinationTransport);
                                break;

                            default:
                                throw new AssertionError("unexpected status: " + connectionStatus);
                        }
                    }

                    @Override
                    public String toString() {
                        return "error response to " + requestDescription;
                    }
                });
            }
        };

        final TransportRequest copiedRequest;
        try {
            copiedRequest = copyWriteable(request, writeableRegistry(), requestHandler::newRequest);
        } catch (IOException e) {
            throw new AssertionError("exception de/serializing request", e);
        }

        try {
            requestHandler.processMessageReceived(copiedRequest, transportChannel);
        } catch (Exception e) {
            try {
                transportChannel.sendResponse(e);
            } catch (Exception ee) {
                logger.warn("failed to send failure", e);
            }
        }
    }

    /**
     * Response type from {@link DisruptableMockTransport#getConnectionStatus(DiscoveryNode)} indicating whether, and how, messages should
     * be disrupted on this transport.
     */
    public enum ConnectionStatus {
        /**
         * No disruption: deliver messages normally.
         */
        CONNECTED,

        /**
         * Simulate disconnection: inbound and outbound messages throw a {@link ConnectTransportException}.
         */
        DISCONNECTED,

        /**
         * Simulate a blackhole partition: inbound and outbound messages are silently discarded.
         */
        BLACK_HOLE,

        /**
         * Simulate an asymmetric partition: outbound messages are silently discarded, but inbound messages are delivered normally.
         */
        BLACK_HOLE_REQUESTS_ONLY
    }
}
