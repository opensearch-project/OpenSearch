/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor;

import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.timeout.ReadTimeoutException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.netty.DisposableServer;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

import org.opensearch.action.ActionListener;
import org.opensearch.common.concurrent.CompletableContext;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.http.AbstractHttpServerTransport;
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpChunk;
import org.opensearch.http.HttpConversionUtil;
import org.opensearch.http.HttpReadTimeoutException;
import org.opensearch.http.HttpRequest;
import org.opensearch.http.HttpResponse;
import org.opensearch.http.HttpServerChannel;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.NettyAllocator;
import org.opensearch.transport.SharedGroupFactory;
import org.opensearch.transport.netty4.Netty4TcpChannel;
import org.opensearch.transport.netty4.Netty4Utils;
import org.reactivestreams.FlowAdapters;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_READ_TIMEOUT;

public class ReactorNetty4HttpServerTransport extends AbstractHttpServerTransport {
    private final SharedGroupFactory sharedGroupFactory;
    private volatile SharedGroupFactory.SharedGroup sharedGroup;
    private DisposableServer disposableServer;
    private final int readTimeoutMillis;
    private volatile Scheduler scheduler;

    public ReactorNetty4HttpServerTransport(
        Settings settings,
        NetworkService networkService,
        BigArrays bigArrays,
        ThreadPool threadPool,
        NamedXContentRegistry xContentRegistry,
        Dispatcher dispatcher,
        ClusterSettings clusterSettings,
        SharedGroupFactory sharedGroupFactory
    ) {
        super(settings, networkService, bigArrays, threadPool, xContentRegistry, dispatcher, clusterSettings);
        Netty4Utils.setAvailableProcessors(OpenSearchExecutors.NODE_PROCESSORS_SETTING.get(settings));
        NettyAllocator.logAllocatorDescriptionIfNeeded();
        this.readTimeoutMillis = Math.toIntExact(SETTING_HTTP_READ_TIMEOUT.get(settings).getMillis());
        this.sharedGroupFactory = sharedGroupFactory;
    }

    @Override
    protected HttpServerChannel bind(InetSocketAddress socketAddress) throws Exception {
        HttpServer server = HttpServer.create()
            .httpFormDecoder(builder -> builder.scheduler(scheduler))
            .runOn(sharedGroup.getLowLevelGroup())
            .bindAddress(() -> socketAddress)
            .compress(true)
            .protocol(HttpProtocol.HTTP11, HttpProtocol.H2C)
            .handle((req, res) -> incomingRequest(req, res));

        disposableServer = server.bindNow();
        return new ReactorNetty4HttpServerChannel(disposableServer.channel());
    }

    @Override
    protected void stopInternal() {
        if (sharedGroup != null) {
            sharedGroup.shutdown();
            sharedGroup = null;
        }

        if (scheduler != null) {
            scheduler.dispose();
            scheduler = null;
        }

        if (disposableServer != null) {
            disposableServer.disposeNow();
            disposableServer = null;
        }
    }

    @Override
    public void onException(HttpChannel channel, Exception cause) {
        if (cause instanceof ReadTimeoutException) {
            super.onException(channel, new HttpReadTimeoutException(readTimeoutMillis, cause));
        } else {
            super.onException(channel, cause);
        }
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            scheduler = Schedulers.newBoundedElastic(
                Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE,
                Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
                "http-form-decoder"
            );
            sharedGroup = sharedGroupFactory.getHttpGroup();
            bindServer();
            success = true;
        } finally {
            if (success == false) {
                doStop(); // otherwise we leak threads since we never moved to started
            }
        }
    }

    protected Publisher<Void> incomingRequest(HttpServerRequest request, HttpServerResponse response) {
        final Method method = HttpConversionUtil.convertMethod(request.method());
        if (dispatcher.dispatchAsStream(request.uri(), request.fullPath(), method, request.params())) {
            final StreamingRequestConsumer<HttpContent> consumer = new StreamingRequestConsumer<>(request, response);

            request.receiveContent()
                .switchIfEmpty(Mono.just(DefaultLastHttpContent.EMPTY_LAST_CONTENT))
                .subscribe(consumer, null, () -> consumer.accept(DefaultLastHttpContent.EMPTY_LAST_CONTENT));
            consumer.start();

            return response.sendObject(consumer);
        } else {
            final NonStreamingRequestConsumer<HttpContent> consumer = new NonStreamingRequestConsumer<>(request, response);

            request.receiveContent()
                .switchIfEmpty(Mono.just(DefaultLastHttpContent.EMPTY_LAST_CONTENT))
                .subscribe(consumer, null, () -> consumer.accept(DefaultLastHttpContent.EMPTY_LAST_CONTENT));

            return Mono.from(consumer).flatMap(hc -> {
                // TODO: Copy other response details.
                final FullHttpResponse r = (FullHttpResponse) hc;
                response.status(r.status());
                r.headers().forEach(h -> response.addHeader(h.getKey(), h.getValue()));
                return Mono.from(response.chunkedTransfer(false).sendHeaders().sendObject(r.content().copy()));
            });
        }
    }

    private class StreamingRequestConsumer<T extends HttpContent> implements Consumer<T>, Publisher<HttpContent> {
        private final HttpServerRequest request;
        private final HttpServerResponse response;
        private final Publisher<HttpContent> sender;
        private final Publisher<HttpChunk> receiver;
        private final HttpChannel httpChannel;
        private volatile FluxSink<HttpContent> emitter;
        private volatile FluxSink<HttpChunk> producer;

        StreamingRequestConsumer(HttpServerRequest request, HttpServerResponse response) {
            this.request = request;
            this.response = response;
            this.sender = Flux.create(emitter -> this.emitter = emitter);
            this.receiver = Flux.create(producer -> this.producer = producer);

            this.httpChannel = new HttpChannel() {
                private volatile boolean lastChunkReceived = false;
                private final CompletableContext<Void> closeContext = new CompletableContext<>();

                {
                    request.withConnection(connection -> Netty4TcpChannel.addListener(connection.channel().closeFuture(), closeContext));
                }

                @Override
                public boolean isOpen() {
                    return true;
                }

                @Override
                public void close() {
                    request.withConnection(connection -> connection.channel().close());
                }

                @Override
                public void addCloseListener(ActionListener<Void> listener) {
                    closeContext.addListener(ActionListener.toBiConsumer(listener));
                }

                @Override
                public void sendChunk(HttpChunk chunk, ActionListener<Void> listener) {
                    sendContent(createContent(chunk), listener, chunk.isLast());
                }

                @Override
                public void sendResponse(HttpResponse response, ActionListener<Void> listener) {
                    sendContent(createContent(response), listener, true);
                }

                @Override
                public void prepareResponse(int status, Map<String, List<String>> headers) {
                    StreamingRequestConsumer.this.response.status(status);
                    headers.forEach((k, vs) -> vs.forEach(v -> StreamingRequestConsumer.this.response.addHeader(k, v)));
                }

                private void sendContent(HttpContent content, ActionListener<Void> listener, boolean isLast) {
                    try {
                        emitter.next(content);
                        listener.onResponse(null);
                        if (isLast) {
                            emitter.complete();
                        }
                    } catch (final Exception ex) {
                        emitter.error(ex);
                        listener.onFailure(ex);
                    }
                }

                @Override
                public InetSocketAddress getRemoteAddress() {
                    return (InetSocketAddress) response.remoteAddress();
                }

                @Override
                public InetSocketAddress getLocalAddress() {
                    return (InetSocketAddress) response.hostAddress();
                }

                @Override
                public void receiveChunk(HttpChunk message) {
                    if (lastChunkReceived) {
                        return;
                    }

                    producer.next(message);
                    if (message.isLast()) {
                        lastChunkReceived = true;
                        producer.complete();
                    }
                }

                @Override
                public void subscribe(java.util.concurrent.Flow.Subscriber<? super HttpChunk> s) {
                    receiver.subscribe(FlowAdapters.toSubscriber(s));
                }
            };
        }

        public void start() {
            incomingStream(createRequest(request), httpChannel);
        }

        @Override
        public void accept(T message) {
            if (message instanceof LastHttpContent) {
                httpChannel.receiveChunk(createChunk(message, true));
            } else if (message instanceof HttpContent) {
                httpChannel.receiveChunk(createChunk(message, false));
            }
        }

        HttpRequest createRequest(HttpServerRequest request) {
            return new ReactorNetty4HttpRequest(request, Unpooled.EMPTY_BUFFER);
        }

        HttpChunk createChunk(HttpContent chunk, boolean last) {
            return new ReactorNetty4HttpChunk(chunk.content(), last);
        }

        HttpContent createContent(HttpChunk chunk) {
            return new DefaultHttpContent(Unpooled.copiedBuffer(BytesReference.toBytes(chunk.content())));
        }

        HttpContent createContent(HttpResponse response) {
            final FullHttpResponse fr = (FullHttpResponse) response;
            return new DefaultHttpContent(fr.content());
        }

        @Override
        public void subscribe(Subscriber<? super HttpContent> s) {
            sender.subscribe(s);
        }
    }

    private class NonStreamingRequestConsumer<T extends HttpContent> implements Consumer<T>, Publisher<HttpContent> {
        private static final int DEFAULT_MAX_COMPOSITEBUFFER_COMPONENTS = 1024;

        private final HttpServerRequest request;
        private final HttpServerResponse response;
        private final CompositeByteBuf content;
        private final Publisher<HttpContent> publisher;
        private volatile FluxSink<HttpContent> emitter;
        private volatile boolean lastChunkReceived = false;

        NonStreamingRequestConsumer(HttpServerRequest request, HttpServerResponse response) {
            this.request = request;
            this.response = response;
            this.content = response.alloc().compositeBuffer(DEFAULT_MAX_COMPOSITEBUFFER_COMPONENTS);
            this.publisher = Flux.create(emitter -> register(emitter));
        }

        private void register(FluxSink<HttpContent> emitter) {
            this.emitter = emitter;
        }

        @Override
        public void accept(T message) {
            if (lastChunkReceived) {
                return;
            }

            try {
                if (message instanceof LastHttpContent) {
                    process(message, emitter);
                } else if (message instanceof HttpContent) {
                    process(message, emitter);
                }
            } catch (Throwable ex) {
                emitter.error(ex);
            }
        }

        public void process(HttpContent in, FluxSink<HttpContent> emitter) {
            // Consume request body in full before dispatching it
            content.addComponent(true, in.content().retain());
            if (in instanceof LastHttpContent) {
                lastChunkReceived = true;
                incomingRequest(createRequest(request, content), new HttpChannel() {
                    private final CompletableContext<Void> closeContext = new CompletableContext<>();

                    {
                        request.withConnection(
                            connection -> Netty4TcpChannel.addListener(connection.channel().closeFuture(), closeContext)
                        );
                    }

                    @Override
                    public boolean isOpen() {
                        return true;
                    }

                    @Override
                    public void close() {
                        request.withConnection(connection -> connection.channel().close());
                    }

                    @Override
                    public void addCloseListener(ActionListener<Void> listener) {
                        closeContext.addListener(ActionListener.toBiConsumer(listener));
                    }

                    @Override
                    public void sendResponse(HttpResponse response, ActionListener<Void> listener) {
                        emitter.next(createResponse(response));
                        listener.onResponse(null);
                        emitter.complete();
                    }

                    @Override
                    public InetSocketAddress getRemoteAddress() {
                        return (InetSocketAddress) response.remoteAddress();
                    }

                    @Override
                    public InetSocketAddress getLocalAddress() {
                        return (InetSocketAddress) response.hostAddress();
                    }
                });
            }
        }

        HttpRequest createRequest(HttpServerRequest request, CompositeByteBuf content) {
            return new ReactorNetty4HttpRequest(request, content);
        }

        FullHttpResponse createResponse(HttpResponse response) {
            return (FullHttpResponse) response;
        }

        @Override
        public void subscribe(Subscriber<? super HttpContent> s) {
            publisher.subscribe(s);
        }
    }
}
