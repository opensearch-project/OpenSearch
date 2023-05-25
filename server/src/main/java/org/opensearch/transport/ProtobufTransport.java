/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.node.ProtobufDiscoveryNode;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.component.LifecycleComponent;
import org.opensearch.common.transport.ProtobufBoundTransportAddress;
import org.opensearch.common.transport.ProtobufTransportAddress;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.ConcurrentMapLong;

import java.io.Closeable;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * OpenSearch ProtobufTransport Interface
*
* @opensearch.internal
*/
public interface ProtobufTransport extends LifecycleComponent {

    /**
     * Registers a new request handler
    */
    default <Request extends ProtobufTransportRequest> void registerRequestHandler(ProtobufRequestHandlerRegistry<Request> reg) {
        getRequestHandlers().registerHandler(reg);
    }

    void setMessageListener(ProtobufTransportMessageListener listener);

    default void setSlowLogThreshold(TimeValue slowLogThreshold) {}

    default boolean isSecure() {
        return false;
    }

    /**
     * The address the transport is bound on.
    */
    ProtobufBoundTransportAddress boundAddress();

    /**
     * Further profile bound addresses
    * @return <code>null</code> iff profiles are unsupported, otherwise a map with name of profile and its bound transport address
    */
    Map<String, ProtobufBoundTransportAddress> profileBoundAddresses();

    /**
     * Returns an address from its string representation.
    */
    ProtobufTransportAddress[] addressesFromString(String address) throws UnknownHostException;

    /**
     * Returns a list of all local addresses for this transport
    */
    List<String> getDefaultSeedAddresses();

    /**
     * Opens a new connection to the given node. When the connection is fully connected, the listener is called.
    * The ActionListener will be called on the calling thread or the generic thread pool.
    */
    void openConnection(
        ProtobufDiscoveryNode node,
        ProtobufConnectionProfile profile,
        ActionListener<ProtobufTransport.Connection> listener
    );

    TransportStats getStats();

    ResponseHandlers getResponseHandlers();

    RequestHandlers getRequestHandlers();

    /**
     * A unidirectional connection to a {@link ProtobufDiscoveryNode}
    */
    interface Connection extends Closeable {
        /**
         * The node this connection is associated with
        */
        ProtobufDiscoveryNode getNode();

        /**
         * Sends the request to the node this connection is associated with
        * @param requestId see {@link ResponseHandlers#add(ResponseContext)} for details
        * @param action the action to execute
        * @param request the request to send
        * @param options request options to apply
        * @throws NodeNotConnectedException if the given node is not connected
        */
        void sendRequest(long requestId, String action, ProtobufTransportRequest request, TransportRequestOptions options)
            throws IOException, TransportException;

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
    */
    final class ResponseContext<T extends ProtobufTransportResponse> {

        private final ProtobufTransportResponseHandler<T> handler;

        private final Connection connection;

        private final String action;

        ResponseContext(ProtobufTransportResponseHandler<T> handler, Connection connection, String action) {
            this.handler = handler;
            this.connection = connection;
            this.action = action;
        }

        public ProtobufTransportResponseHandler<T> handler() {
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
    */
    final class ResponseHandlers {
        private final ConcurrentMapLong<ResponseContext<? extends ProtobufTransportResponse>> handlers = ConcurrentCollections
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
        public ResponseContext<? extends ProtobufTransportResponse> remove(long requestId) {
            return handlers.remove(requestId);
        }

        /**
         * Adds a new response context and associates it with a new request ID.
        * @return the new request ID
        * @see Connection#sendRequest(long, String, ProtobufTransportRequest, TransportRequestOptions)
        */
        public long add(ResponseContext<? extends ProtobufTransportResponse> holder) {
            long requestId = newRequestId();
            ResponseContext<? extends ProtobufTransportResponse> existing = handlers.put(requestId, holder);
            assert existing == null : "request ID already in use: " + requestId;
            return requestId;
        }

        /**
         * Returns a new request ID to use when sending a message via {@link Connection#sendRequest(long, String,
        * ProtobufTransportRequest, TransportRequestOptions)}
        */
        long newRequestId() {
            return requestIdGenerator.incrementAndGet();
        }

        /**
         * Removes and returns all {@link ResponseContext} instances that match the predicate
        */
        public List<ResponseContext<? extends ProtobufTransportResponse>> prune(
            Predicate<ResponseContext<? extends ProtobufTransportResponse>> predicate
        ) {
            final List<ResponseContext<? extends ProtobufTransportResponse>> holders = new ArrayList<>();
            for (Map.Entry<Long, ResponseContext<? extends ProtobufTransportResponse>> entry : handlers.entrySet()) {
                ResponseContext<? extends ProtobufTransportResponse> holder = entry.getValue();
                if (predicate.test(holder)) {
                    ResponseContext<? extends ProtobufTransportResponse> remove = handlers.remove(entry.getKey());
                    if (remove != null) {
                        holders.add(holder);
                    }
                }
            }
            return holders;
        }

        /**
         * called by the {@link ProtobufTransport} implementation when a response or an exception has been received for a previously
        * sent request (before any processing or deserialization was done). Returns the appropriate response handler or null if not
        * found.
        */
        public ProtobufTransportResponseHandler<? extends ProtobufTransportResponse> onResponseReceived(
            final long requestId,
            final ProtobufTransportMessageListener listener
        ) {
            ResponseContext<? extends ProtobufTransportResponse> context = handlers.remove(requestId);
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
    * @opensearch.internal
    */
    final class RequestHandlers {

        private volatile Map<String, ProtobufRequestHandlerRegistry<? extends ProtobufTransportRequest>> requestHandlers = Collections
            .emptyMap();

        synchronized <Request extends ProtobufTransportRequest> void registerHandler(ProtobufRequestHandlerRegistry<Request> reg) {
            if (requestHandlers.containsKey(reg.getAction())) {
                throw new IllegalArgumentException("transport handlers for action " + reg.getAction() + " is already registered");
            }
            requestHandlers = MapBuilder.newMapBuilder(requestHandlers).put(reg.getAction(), reg).immutableMap();
        }

        // TODO: Only visible for testing. Perhaps move StubbableTransport from
        // org.opensearch.test.transport to org.opensearch.transport
        public synchronized <Request extends ProtobufTransportRequest> void forceRegister(ProtobufRequestHandlerRegistry<Request> reg) {
            requestHandlers = MapBuilder.newMapBuilder(requestHandlers).put(reg.getAction(), reg).immutableMap();
        }

        @SuppressWarnings("unchecked")
        public <T extends ProtobufTransportRequest> ProtobufRequestHandlerRegistry<T> getHandler(String action) {
            return (ProtobufRequestHandlerRegistry<T>) requestHandlers.get(action);
        }
    }
}
