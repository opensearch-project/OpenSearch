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
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.http.netty4.Netty4HttpServerTransport;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;

public class Netty4BlockingPlugin extends Netty4Plugin {

    private static final AttributeKey<Boolean> SHOULD_BLOCK = AttributeKey.newInstance("should-block");

    public class Netty4BlockingHttpServerTransport extends Netty4HttpServerTransport {

        public Netty4BlockingHttpServerTransport(
            Settings settings,
            NetworkService networkService,
            BigArrays bigArrays,
            ThreadPool threadPool,
            NamedXContentRegistry xContentRegistry,
            Dispatcher dispatcher,
            ClusterSettings clusterSettings,
            SharedGroupFactory sharedGroupFactory,
            Tracer tracer
        ) {
            super(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry,
                dispatcher,
                clusterSettings,
                sharedGroupFactory,
                tracer
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
        ClusterSettings clusterSettings,
        Tracer tracer
    ) {
        return Collections.singletonMap(
            NETTY_HTTP_TRANSPORT_NAME,
            () -> new Netty4BlockingHttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry,
                new BlockingDispatcher(dispatcher),
                clusterSettings,
                getSharedGroupFactory(settings),
                tracer
            )
        );
    }

    /** POC for how an external header verifier would be implemented */
    public class ExampleBlockingNetty4HeaderVerifier extends SimpleChannelInboundHandler<DefaultHttpRequest> {

        @Override
        public void channelRead0(ChannelHandlerContext ctx, DefaultHttpRequest msg) throws Exception {
            ReferenceCountUtil.retain(msg);
            if (isBlocked(msg)) {
                msg.headers().add("blocked", true);
            }
            ctx.fireChannelRead(msg);
        }

        private boolean isBlocked(HttpMessage request) {
            final boolean shouldBlock = request.headers().contains("blockme");

            return shouldBlock;
        }
    }

    class BlockingDispatcher implements HttpServerTransport.Dispatcher {

        private HttpServerTransport.Dispatcher originalDispatcher;

        public BlockingDispatcher(final HttpServerTransport.Dispatcher originalDispatcher) {
            super();
            this.originalDispatcher = originalDispatcher;
        }

        @Override
        public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
            if (request.getHeaders().containsKey("blocked")) {
                channel.sendResponse(new BytesRestResponse(RestStatus.UNAUTHORIZED, "Hit header_verifier"));
                return;
            }
            originalDispatcher.dispatchRequest(request, channel, threadContext);

        }

        @Override
        public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
            originalDispatcher.dispatchBadRequest(channel, threadContext, cause);
        }
    }
}
