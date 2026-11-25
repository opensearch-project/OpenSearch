/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import javax.net.ssl.SSLSession;

import java.net.SocketAddress;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

/**
 * gRPC {@link ServerInterceptor} that exposes the caller's TLS session and remote address to the
 * Flight producer through the gRPC {@link Context}.
 *
 * <p>Arrow Flight does not surface transport attributes to {@link org.apache.arrow.flight.FlightProducer}s;
 * this makes them available to request handlers and transport interceptors, as they are on the
 * native transport.
 */
class PeerInfoServerInterceptor implements ServerInterceptor {
    static final Context.Key<SSLSession> SSL_SESSION = Context.key("flight-peer-ssl-session");
    static final Context.Key<SocketAddress> REMOTE_ADDRESS = Context.key("flight-peer-remote-address");

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call,
        Metadata headers,
        ServerCallHandler<ReqT, RespT> next
    ) {
        Context context = Context.current()
            .withValue(SSL_SESSION, call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION))
            .withValue(REMOTE_ADDRESS, call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
        return Contexts.interceptCall(context, call, headers, next);
    }
}
