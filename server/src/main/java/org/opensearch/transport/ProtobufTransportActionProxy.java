/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.tasks.ProtobufTask;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;

/**
 * ProtobufTransportActionProxy allows an arbitrary action to be executed on a defined target node while the initial request is sent to a second
* node that acts as a request proxy to the target node. This is useful if a node is not directly connected to a target node but is
* connected to an intermediate node that establishes a transitive connection.
*
* @opensearch.internal
*/
public final class ProtobufTransportActionProxy {

    private ProtobufTransportActionProxy() {} // no instance

    /**
     * Handler for proxy requests
    *
    * @opensearch.internal
    */
    private static class ProxyRequestHandler<T extends ProxyRequest> implements ProtobufTransportRequestHandler<T> {

        private final TransportService service;
        private final String action;
        private final Function<TransportRequest, ProtobufWriteable.Reader<? extends TransportResponse>> responseFunction;

        ProxyRequestHandler(
            TransportService service,
            String action,
            Function<TransportRequest, ProtobufWriteable.Reader<? extends TransportResponse>> responseFunction
        ) {
            this.service = service;
            this.action = action;
            this.responseFunction = responseFunction;
        }

        @Override
        public void messageReceived(T request, TransportChannel channel, ProtobufTask task) throws Exception {
            DiscoveryNode targetNode = request.targetNode;
            TransportRequest wrappedRequest = request.wrapped;
            service.sendRequest(
                targetNode,
                action,
                wrappedRequest,
                new ProxyResponseHandler<>(channel, responseFunction.apply(wrappedRequest))
            );
        }
    }

    /**
     * Handler for the proxy response
    *
    * @opensearch.internal
    */
    private static class ProxyResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {

        private final ProtobufWriteable.Reader<T> reader;
        private final TransportChannel channel;

        ProxyResponseHandler(TransportChannel channel, ProtobufWriteable.Reader<T> reader) {
            this.reader = reader;
            this.channel = channel;
        }

        @Override
        public T read(CodedInputStream in) throws IOException {
            return reader.read(in);
        }

        @Override
        public void handleResponse(T response) {
            try {
                channel.sendResponse(response);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void handleExceptionProtobuf(ProtobufTransportException exp) {
            try {
                channel.sendResponse(exp);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public T read(StreamInput in) throws IOException {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'read'");
        }

        @Override
        public void handleException(TransportException exp) {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'handleException'");
        }
    }

    /**
     * The proxy request
    *
    * @opensearch.internal
    */
    static class ProxyRequest<T extends TransportRequest> extends TransportRequest {
        final T wrapped;
        final DiscoveryNode targetNode;

        ProxyRequest(T wrapped, DiscoveryNode targetNode) {
            this.wrapped = wrapped;
            this.targetNode = targetNode;
        }

        ProxyRequest(CodedInputStream in, ProtobufWriteable.Reader<T> reader) throws IOException {
            super(in);
            targetNode = new DiscoveryNode(in);
            wrapped = reader.read(in);
        }

        @Override
        public void writeTo(CodedOutputStream out) throws IOException {
            super.writeTo(out);
            targetNode.writeTo(out);
            wrapped.writeTo(out);
        }
    }

    /**
     * Registers a proxy request handler that allows to forward requests for the given action to another node. To be used when the
    * response type changes based on the upcoming request (quite rare)
    */
    public static void registerProxyActionWithDynamicResponseType(
        TransportService service,
        String action,
        Function<TransportRequest, ProtobufWriteable.Reader<? extends TransportResponse>> responseFunction
    ) {
        ProtobufRequestHandlerRegistry<? extends TransportRequest> requestHandler = service.getRequestHandlerProtobuf(action);
        service.registerRequestHandlerProtobuf(
            getProxyAction(action),
            ThreadPool.Names.SAME,
            true,
            false,
            in -> new ProxyRequest<>(in, requestHandler::newRequest),
            new ProxyRequestHandler<>(service, action, responseFunction)
        );
    }

    /**
     * Registers a proxy request handler that allows to forward requests for the given action to another node. To be used when the
    * response type is always the same (most of the cases).
    */
    public static void registerProxyAction(
        TransportService service,
        String action,
        ProtobufWriteable.Reader<? extends TransportResponse> reader
    ) {
        ProtobufRequestHandlerRegistry<? extends TransportRequest> requestHandler = service.getRequestHandlerProtobuf(action);
        service.registerRequestHandlerProtobuf(
            getProxyAction(action),
            ThreadPool.Names.SAME,
            true,
            false,
            in -> new ProxyRequest<>(in, requestHandler::newRequest),
            new ProxyRequestHandler<>(service, action, request -> reader)
        );
    }

    private static final String PROXY_ACTION_PREFIX = "internal:transport/proxy/";

    /**
     * Returns the corresponding proxy action for the given action
    */
    public static String getProxyAction(String action) {
        return PROXY_ACTION_PREFIX + action;
    }

    /**
     * Wraps the actual request in a proxy request object that encodes the target node.
    */
    public static TransportRequest wrapRequest(DiscoveryNode node, TransportRequest request) {
        return new ProxyRequest<>(request, node);
    }

    /**
     * Unwraps a proxy request and returns the original request
    */
    public static TransportRequest unwrapRequest(TransportRequest request) {
        if (request instanceof ProxyRequest) {
            return ((ProxyRequest) request).wrapped;
        }
        return request;
    }

    /**
     * Unwraps a proxy action and returns the underlying action
    */
    public static String unwrapAction(String action) {
        assert isProxyAction(action) : "Attempted to unwrap non-proxy action: " + action;
        return action.substring(PROXY_ACTION_PREFIX.length());
    }

    /**
     * Returns <code>true</code> iff the given action is a proxy action
    */
    public static boolean isProxyAction(String action) {
        return action.startsWith(PROXY_ACTION_PREFIX);
    }

    /**
     * Returns <code>true</code> iff the given request is a proxy request
    */
    public static boolean isProxyRequest(TransportRequest request) {
        return request instanceof ProxyRequest;
    }
}
