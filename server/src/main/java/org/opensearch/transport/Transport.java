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

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.ConcurrentMapLong;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.transport.TransportResponse;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * OpenSearch Transport Interface
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface Transport extends LifecycleComponent {

    /**
     * Registers a new request handler
     */
    default <Request extends TransportRequest> void registerRequestHandler(RequestHandlerRegistry<Request> reg) {
        getRequestHandlers().registerHandler(reg);
    }

    void setMessageListener(TransportMessageListener listener);

    default void setSlowLogThreshold(TimeValue slowLogThreshold) {}

    default boolean isSecure() {
        return false;
    }

    /**
     * The address the transport is bound on.
     */
    BoundTransportAddress boundAddress();

    /**
     * Further profile bound addresses
     * @return <code>null</code> iff profiles are unsupported, otherwise a map with name of profile and its bound transport address
     */
    Map<String, BoundTransportAddress> profileBoundAddresses();

    /**
     * Returns an address from its string representation.
     */
    TransportAddress[] addressesFromString(String address) throws UnknownHostException;

    /**
     * Returns a list of all local addresses for this transport
     */
    List<String> getDefaultSeedAddresses();

    /**
     * Opens a new connection to the given node. When the connection is fully connected, the listener is called.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     */
    void openConnection(DiscoveryNode node, ConnectionProfile profile, ActionListener<Transport.Connection> listener);

    TransportStats getStats();

    ResponseHandlers getResponseHandlers();

    RequestHandlers getRequestHandlers();

    /**
     * Resolve the publishPort for a server provided a list of boundAddresses and a publishInetAddress.
     * Resolution strategy is as follows:
     * If a configured port exists resolve to that port.
     * If a bound address matches the publishInetAddress resolve to that port.
     * If a bound address is a wildcard address resolve to that port.
     * If all bound addresses share the same port resolve to that port.
     *
     * @param publishPort -1 if no configured publish port exists
     * @param boundAddresses addresses bound by the server
     * @param publishInetAddress address published for the server
     * @return Resolved port. If publishPort is negative and no port can be resolved return publishPort.
     */
    static int resolvePublishPort(int publishPort, List<InetSocketAddress> boundAddresses, InetAddress publishInetAddress) {
        if (publishPort < 0) {
            for (InetSocketAddress boundAddress : boundAddresses) {
                InetAddress boundInetAddress = boundAddress.getAddress();
                if (boundInetAddress.isAnyLocalAddress() || boundInetAddress.equals(publishInetAddress)) {
                    publishPort = boundAddress.getPort();
                    break;
                }
            }
        }

        if (publishPort < 0) {
            final Set<Integer> ports = new HashSet<>();
            for (InetSocketAddress boundAddress : boundAddresses) {
                ports.add(boundAddress.getPort());
            }
            if (ports.size() == 1) {
                publishPort = ports.iterator().next();
            }
        }

        return publishPort;
    }

    static int resolveTransportPublishPort(int publishPort, List<TransportAddress> boundAddresses, InetAddress publishInetAddress) {
        return Transport.resolvePublishPort(
            publishPort,
            boundAddresses.stream().map(TransportAddress::address).collect(Collectors.toList()),
            publishInetAddress
        );
    }

    /**
     * A unidirectional connection to a {@link DiscoveryNode}
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    interface Connection extends Closeable {
        /**
         * The node this connection is associated with
         */
        DiscoveryNode getNode();

        /**
         * Sends the request to the node this connection is associated with
         * @param requestId see {@link ResponseHandlers#add(ResponseContext)} for details
         * @param action the action to execute
         * @param request the request to send
         * @param options request options to apply
         * @throws NodeNotConnectedException if the given node is not connected
         */
        void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException,
            TransportException;

        /**
         * The listener's {@link ActionListener#onResponse(Object)} method will be called when this
         * connection is closed. No implementations currently throw an exception during close, so
         * {@link ActionListener#onFailure(Exception)} will not be called.
         *
         * @param listener to be called
         */
        void addCloseListener(ActionListener<Void> listener);

        boolean isClosed();

        /**
         * Returns the version of the node this connection was established with.
         */
        default Version getVersion() {
            return getNode().getVersion();
        }

        /**
         * Returns a key that this connection can be cached on. Delegating subclasses must delegate method call to
         * the original connection.
         */
        default Object getCacheKey() {
            return this;
        }

        @Override
        void close();
    }

    /**
     * This class represents a response context that encapsulates the actual response handler, the action and the connection it was
     * executed on.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    final class ResponseContext<T extends TransportResponse> {

        private final TransportResponseHandler<T> handler;

        private final Connection connection;

        private final String action;

        ResponseContext(TransportResponseHandler<T> handler, Connection connection, String action) {
            this.handler = handler;
            this.connection = connection;
            this.action = action;
        }

        public TransportResponseHandler<T> handler() {
            return handler;
        }

        public Connection connection() {
            return this.connection;
        }

        public String action() {
            return this.action;
        }
    }

    /**
     * This class is a registry that allows
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    final class ResponseHandlers {
        private final ConcurrentMapLong<ResponseContext<? extends TransportResponse>> handlers = ConcurrentCollections
            .newConcurrentMapLongWithAggressiveConcurrency();
        private final AtomicLong requestIdGenerator = new AtomicLong();

        /**
         * Returns <code>true</code> if the give request ID has a context associated with it.
         */
        public boolean contains(long requestId) {
            return handlers.containsKey(requestId);
        }

        /**
         * Removes and return the {@link ResponseContext} for the given request ID or returns
         * <code>null</code> if no context is associated with this request ID.
         */
        public ResponseContext<? extends TransportResponse> remove(long requestId) {
            return handlers.remove(requestId);
        }

        /**
         * Adds a new response context and associates it with a new request ID.
         * @return the new request ID
         * @see Connection#sendRequest(long, String, TransportRequest, TransportRequestOptions)
         */
        public long add(ResponseContext<? extends TransportResponse> holder) {
            long requestId = newRequestId();
            ResponseContext<? extends TransportResponse> existing = handlers.put(requestId, holder);
            assert existing == null : "request ID already in use: " + requestId;
            return requestId;
        }

        /**
         * Returns a new request ID to use when sending a message via {@link Connection#sendRequest(long, String,
         * TransportRequest, TransportRequestOptions)}
         */
        long newRequestId() {
            return requestIdGenerator.incrementAndGet();
        }

        /**
         * Removes and returns all {@link ResponseContext} instances that match the predicate
         */
        public List<ResponseContext<? extends TransportResponse>> prune(Predicate<ResponseContext<? extends TransportResponse>> predicate) {
            final List<ResponseContext<? extends TransportResponse>> holders = new ArrayList<>();
            for (Map.Entry<Long, ResponseContext<? extends TransportResponse>> entry : handlers.entrySet()) {
                ResponseContext<? extends TransportResponse> holder = entry.getValue();
                if (predicate.test(holder)) {
                    ResponseContext<? extends TransportResponse> remove = handlers.remove(entry.getKey());
                    if (remove != null) {
                        holders.add(holder);
                    }
                }
            }
            return holders;
        }

        /**
         * called by the {@link Transport} implementation when a response or an exception has been received for a previously
         * sent request (before any processing or deserialization was done). Returns the appropriate response handler or null if not
         * found.
         */
        public TransportResponseHandler<? extends TransportResponse> onResponseReceived(
            final long requestId,
            final TransportMessageListener listener
        ) {
            ResponseContext<? extends TransportResponse> context = handlers.remove(requestId);
            listener.onResponseReceived(requestId, context);
            if (context == null) {
                return null;
            } else {
                return context.handler();
            }
        }
    }

    /**
     * Request handler implementations
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    final class RequestHandlers {

        private volatile Map<String, RequestHandlerRegistry<? extends TransportRequest>> requestHandlers = Collections.emptyMap();

        synchronized <Request extends TransportRequest> void registerHandler(RequestHandlerRegistry<Request> reg) {
            if (requestHandlers.containsKey(reg.getAction())) {
                throw new IllegalArgumentException("transport handlers for action " + reg.getAction() + " is already registered");
            }
            requestHandlers = MapBuilder.newMapBuilder(requestHandlers).put(reg.getAction(), reg).immutableMap();
        }

        // TODO: Only visible for testing. Perhaps move StubbableTransport from
        // org.opensearch.test.transport to org.opensearch.transport
        public synchronized <Request extends TransportRequest> void forceRegister(RequestHandlerRegistry<Request> reg) {
            requestHandlers = MapBuilder.newMapBuilder(requestHandlers).put(reg.getAction(), reg).immutableMap();
        }

        @SuppressWarnings("unchecked")
        public <T extends TransportRequest> RequestHandlerRegistry<T> getHandler(String action) {
            return (RequestHandlerRegistry<T>) requestHandlers.get(action);
        }
    }
}
