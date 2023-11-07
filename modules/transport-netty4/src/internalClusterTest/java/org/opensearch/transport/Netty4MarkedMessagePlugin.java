/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.indices.breaker.CircuitBreakerService;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.http.netty4.Netty4HttpServerTransport;
import org.opensearch.threadpool.ThreadPool;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.ReferenceCountUtil;

public class Netty4MarkedMessagePlugin extends Netty4Plugin {

    public static final AtomicReference<HttpMessage> MESSAGE = new AtomicReference<>(); 

    public class Netty4BlockingHttpServerTransport extends Netty4HttpServerTransport {

        public Netty4BlockingHttpServerTransport(
            Settings settings,
            NetworkService networkService,
            BigArrays bigArrays,
            ThreadPool threadPool,
            NamedXContentRegistry xContentRegistry,
            Dispatcher dispatcher,
            ClusterSettings clusterSettings,
            SharedGroupFactory sharedGroupFactory
        ) {
            super(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry,
                dispatcher,
                clusterSettings,
                sharedGroupFactory
            );
        }

        @Override
        protected ChannelInboundHandlerAdapter createHeaderVerifier() {
            return new ExampleBlockingNetty4HeaderVerifier();
        }
    }

    @Override
    public Map<String, Supplier<HttpServerTransport>> getHttpTransports(
        Settings settings,
        ThreadPool threadPool,
        BigArrays bigArrays,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedXContentRegistry xContentRegistry,
        NetworkService networkService,
        HttpServerTransport.Dispatcher dispatcher,
        ClusterSettings clusterSettings
    ) {
        return Collections.singletonMap(
            NETTY_HTTP_TRANSPORT_NAME,
            () -> new Netty4BlockingHttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry,
                dispatcher,
                clusterSettings,
                getSharedGroupFactory(settings)
            )
        );
    }

    /** POC for how an external header verifier would be implemented */
    @Sharable
    public class ExampleBlockingNetty4HeaderVerifier extends SimpleChannelInboundHandler<DefaultHttpRequest> {

        @Override
        public void channelRead0(ChannelHandlerContext ctx, DefaultHttpRequest msg) throws Exception {
            ReferenceCountUtil.retain(msg);
            if (isMarked(msg)) {
                MESSAGE.compareAndSet(null, msg);
            }

            // Lets the request pass to the next channel handler
            ctx.fireChannelRead(msg);
        }

        private boolean isMarked(HttpRequest request) {
            return request.headers().contains("marked-message");
        }
    }
}
