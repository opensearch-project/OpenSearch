/*
 * Copyright 2015-2017 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.http.netty4.ssl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpHandlingSettings;
import org.opensearch.http.netty4.Netty4HttpChannel;
import org.opensearch.http.netty4.Netty4HttpServerTransport;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.SharedGroupFactory;
import org.opensearch.transport.netty4.ssl.SslUtils;

import javax.net.ssl.SSLEngine;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.handler.ssl.SslHandler;

/**
 * @see <a href="https://github.com/opensearch-project/security/blob/d526c9f6c2a438c14db8b413148204510b9fe2e2/src/main/java/org/opensearch/security/ssl/http/netty/SecuritySSLNettyHttpServerTransport.java">SecuritySSLNettyHttpServerTransport</a>
 */
public class SecureNetty4HttpServerTransport extends Netty4HttpServerTransport {
    private static final Logger logger = LogManager.getLogger(SecureNetty4HttpServerTransport.class);
    private final SecureTransportSettingsProvider secureTransportSettingsProvider;
    private final SecureTransportSettingsProvider.ServerExceptionHandler exceptionHandler;

    public SecureNetty4HttpServerTransport(
        final Settings settings,
        final NetworkService networkService,
        final BigArrays bigArrays,
        final ThreadPool threadPool,
        final NamedXContentRegistry namedXContentRegistry,
        final Dispatcher dispatcher,
        final ClusterSettings clusterSettings,
        final SharedGroupFactory sharedGroupFactory,
        final SecureTransportSettingsProvider secureTransportSettingsProvider,
        final Tracer tracer
    ) {
        super(
            settings,
            networkService,
            bigArrays,
            threadPool,
            namedXContentRegistry,
            dispatcher,
            clusterSettings,
            sharedGroupFactory,
            tracer
        );
        this.secureTransportSettingsProvider = secureTransportSettingsProvider;
        this.exceptionHandler = secureTransportSettingsProvider.buildHttpServerExceptionHandler(settings, this)
            .orElse(SecureTransportSettingsProvider.ServerExceptionHandler.NOOP);
    }

    @Override
    public ChannelHandler configureServerChannelHandler() {
        return new SslHttpChannelHandler(this, handlingSettings);
    }

    @Override
    public void onException(HttpChannel channel, Exception cause0) {
        Throwable cause = cause0;

        if (cause0 instanceof DecoderException && cause0 != null) {
            cause = cause0.getCause();
        }

        exceptionHandler.onError(cause);
        logger.error("Exception during establishing a SSL connection: " + cause, cause);
        super.onException(channel, cause0);
    }

    protected class SslHttpChannelHandler extends Netty4HttpServerTransport.HttpChannelHandler {
        /**
         * Application negotiation handler to select either HTTP 1.1 or HTTP 2 protocol, based
         * on client/server ALPN negotiations.
         */
        private class Http2OrHttpHandler extends ApplicationProtocolNegotiationHandler {
            protected Http2OrHttpHandler() {
                super(ApplicationProtocolNames.HTTP_1_1);
            }

            @Override
            protected void configurePipeline(ChannelHandlerContext ctx, String protocol) throws Exception {
                if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
                    configureDefaultHttp2Pipeline(ctx.pipeline());
                } else if (ApplicationProtocolNames.HTTP_1_1.equals(protocol)) {
                    configureDefaultHttpPipeline(ctx.pipeline());
                } else {
                    throw new IllegalStateException("Unknown application protocol: " + protocol);
                }
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                super.exceptionCaught(ctx, cause);
                Netty4HttpChannel channel = ctx.channel().attr(HTTP_CHANNEL_KEY).get();
                if (channel != null) {
                    if (cause instanceof Error) {
                        onException(channel, new Exception(cause));
                    } else {
                        onException(channel, (Exception) cause);
                    }
                }
            }
        }

        protected SslHttpChannelHandler(final Netty4HttpServerTransport transport, final HttpHandlingSettings handlingSettings) {
            super(transport, handlingSettings);
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            super.initChannel(ch);

            final SSLEngine sslEngine = secureTransportSettingsProvider.buildSecureHttpServerEngine(
                settings,
                SecureNetty4HttpServerTransport.this
            ).orElseGet(SslUtils::createDefaultServerSSLEngine);

            final SslHandler sslHandler = new SslHandler(sslEngine);
            ch.pipeline().addFirst("ssl_http", sslHandler);
        }

        @Override
        protected void configurePipeline(Channel ch) {
            ch.pipeline().addLast(new Http2OrHttpHandler());
        }
    }
}
