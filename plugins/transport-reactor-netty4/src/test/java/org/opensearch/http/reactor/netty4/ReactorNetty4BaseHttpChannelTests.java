/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.test.OpenSearchTestCase;

import javax.net.ssl.SSLEngine;

import java.util.Optional;
import java.util.function.Consumer;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import reactor.netty.Connection;
import reactor.netty.NettyPipeline;
import reactor.netty.http.server.HttpServerRequest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReactorNetty4BaseHttpChannelTests extends OpenSearchTestCase {

    public void testSslHandlerUsesCurrentChannelBeforeParent() {
        final SslHandler currentHandler = mock(SslHandler.class);
        final SslHandler parentHandler = mock(SslHandler.class);
        final HttpServerRequest request = requestWithChannels(currentHandler, parentHandler);

        final Optional<SslHandler> result = ReactorNetty4BaseHttpChannel.get(request, "ssl_http", SslHandler.class);

        assertSame(currentHandler, result.orElseThrow());
    }

    public void testSslHandlerFallsBackToParentChannel() {
        final SslHandler parentHandler = mock(SslHandler.class);
        final HttpServerRequest request = requestWithChannels(null, parentHandler);

        final Optional<SslHandler> result = ReactorNetty4BaseHttpChannel.get(request, "ssl_http", SslHandler.class);

        assertSame(parentHandler, result.orElseThrow());
    }

    public void testSslEngineUsesHandlerFromCurrentChannel() {
        final SSLEngine engine = mock(SSLEngine.class);
        final SslHandler currentHandler = mock(SslHandler.class);
        when(currentHandler.engine()).thenReturn(engine);
        final HttpServerRequest request = requestWithChannels(currentHandler, null);

        final Optional<SSLEngine> result = ReactorNetty4BaseHttpChannel.get(request, "ssl_engine", SSLEngine.class);

        assertSame(engine, result.orElseThrow());
    }

    private static HttpServerRequest requestWithChannels(SslHandler currentHandler, SslHandler parentHandler) {
        final ChannelPipeline currentPipeline = mock(ChannelPipeline.class);
        final ChannelPipeline parentPipeline = mock(ChannelPipeline.class);
        when(currentPipeline.get(NettyPipeline.SslHandler)).thenReturn(currentHandler);
        when(parentPipeline.get(NettyPipeline.SslHandler)).thenReturn(parentHandler);

        final Channel parent = mock(Channel.class);
        when(parent.pipeline()).thenReturn(parentPipeline);

        final Channel current = mock(Channel.class);
        when(current.pipeline()).thenReturn(currentPipeline);
        when(current.parent()).thenReturn(parent);

        final Connection connection = mock(Connection.class);
        when(connection.channel()).thenReturn(current);

        final HttpServerRequest request = mock(HttpServerRequest.class);
        doAnswer(invocation -> {
            final Consumer<? super Connection> consumer = invocation.getArgument(0);
            consumer.accept(connection);
            return request;
        }).when(request).withConnection(any());
        return request;
    }
}
