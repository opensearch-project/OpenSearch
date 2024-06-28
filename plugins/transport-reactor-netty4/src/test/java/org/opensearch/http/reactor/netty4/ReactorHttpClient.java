/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.common.collect.Tuple;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.resolver.DefaultAddressResolverGroup;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;
import reactor.netty.http.Http11SslContextSpec;
import reactor.netty.http.Http2SslContextSpec;
import reactor.netty.http.client.HttpClient;

import static io.netty.handler.codec.http.HttpHeaderNames.HOST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Tiny helper to send http requests over netty.
 */
public class ReactorHttpClient implements Closeable {
    private final boolean compression;
    private final boolean secure;

    static Collection<String> returnHttpResponseBodies(Collection<FullHttpResponse> responses) {
        List<String> list = new ArrayList<>(responses.size());
        for (FullHttpResponse response : responses) {
            list.add(response.content().toString(StandardCharsets.UTF_8));
        }
        return list;
    }

    static Collection<String> returnOpaqueIds(Collection<FullHttpResponse> responses) {
        List<String> list = new ArrayList<>(responses.size());
        for (HttpResponse response : responses) {
            list.add(response.headers().get(Task.X_OPAQUE_ID));
        }
        return list;
    }

    public ReactorHttpClient(boolean compression, boolean secure) {
        this.compression = compression;
        this.secure = secure;
    }

    public static ReactorHttpClient create() {
        return create(true);
    }

    public static ReactorHttpClient create(boolean compression) {
        return new ReactorHttpClient(compression, false);
    }

    public static ReactorHttpClient https() {
        return new ReactorHttpClient(true, true);
    }

    public List<FullHttpResponse> get(InetSocketAddress remoteAddress, String... uris) throws InterruptedException {
        return get(remoteAddress, false, uris);
    }

    public List<FullHttpResponse> get(InetSocketAddress remoteAddress, boolean ordered, String... uris) throws InterruptedException {
        final List<FullHttpRequest> requests = new ArrayList<>(uris.length);

        for (int i = 0; i < uris.length; i++) {
            final FullHttpRequest httpRequest = new DefaultFullHttpRequest(HTTP_1_1, HttpMethod.GET, uris[i]);
            httpRequest.headers().add(HOST, "localhost");
            httpRequest.headers().add("X-Opaque-ID", String.valueOf(i));
            httpRequest.headers().add(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), secure ? "https" : "http");
            requests.add(httpRequest);
        }

        return sendRequests(remoteAddress, requests, ordered);
    }

    public final Collection<FullHttpResponse> post(InetSocketAddress remoteAddress, List<Tuple<String, CharSequence>> urisAndBodies)
        throws InterruptedException {
        return processRequestsWithBody(HttpMethod.POST, remoteAddress, urisAndBodies);
    }

    public final FullHttpResponse send(InetSocketAddress remoteAddress, FullHttpRequest httpRequest) throws InterruptedException {
        final List<FullHttpResponse> responses = sendRequests(remoteAddress, Collections.singleton(httpRequest), false);
        assert responses.size() == 1 : "expected 1 and only 1 http response";
        return responses.get(0);
    }

    public final FullHttpResponse stream(InetSocketAddress remoteAddress, HttpRequest httpRequest, Stream<ToXContent> stream)
        throws InterruptedException {
        return sendRequestStream(remoteAddress, httpRequest, stream);
    }

    public final FullHttpResponse send(InetSocketAddress remoteAddress, FullHttpRequest httpRequest, HttpContent content)
        throws InterruptedException {
        final List<FullHttpResponse> responses = sendRequests(
            remoteAddress,
            Collections.singleton(
                new DefaultFullHttpRequest(
                    httpRequest.protocolVersion(),
                    httpRequest.method(),
                    httpRequest.uri(),
                    content.content(),
                    httpRequest.headers(),
                    httpRequest.trailingHeaders()
                )
            ),
            false
        );
        assert responses.size() == 1 : "expected 1 and only 1 http response";
        return responses.get(0);
    }

    public final Collection<FullHttpResponse> put(InetSocketAddress remoteAddress, List<Tuple<String, CharSequence>> urisAndBodies)
        throws InterruptedException {
        return processRequestsWithBody(HttpMethod.PUT, remoteAddress, urisAndBodies);
    }

    private List<FullHttpResponse> processRequestsWithBody(
        HttpMethod method,
        InetSocketAddress remoteAddress,
        List<Tuple<String, CharSequence>> urisAndBodies
    ) throws InterruptedException {
        List<FullHttpRequest> requests = new ArrayList<>(urisAndBodies.size());
        for (int i = 0; i < urisAndBodies.size(); ++i) {
            final Tuple<String, CharSequence> uriAndBody = urisAndBodies.get(i);
            ByteBuf content = Unpooled.copiedBuffer(uriAndBody.v2(), StandardCharsets.UTF_8);
            FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uriAndBody.v1(), content);
            request.headers().add(HttpHeaderNames.HOST, "localhost");
            request.headers().add(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
            request.headers().add(HttpHeaderNames.CONTENT_TYPE, "application/json");
            request.headers().add(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), secure ? "https" : "http");
            request.headers().add("X-Opaque-ID", String.valueOf(i));
            requests.add(request);
        }
        return sendRequests(remoteAddress, requests, false);
    }

    private List<FullHttpResponse> sendRequests(
        final InetSocketAddress remoteAddress,
        final Collection<FullHttpRequest> requests,
        boolean orderer
    ) {
        final NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
        try {
            final HttpClient client = createClient(remoteAddress, eventLoopGroup);

            @SuppressWarnings("unchecked")
            final Mono<FullHttpResponse>[] monos = requests.stream()
                .map(
                    request -> client.headers(h -> h.add(request.headers()))
                        .baseUrl(request.getUri())
                        .request(request.method())
                        .send(Mono.fromSupplier(() -> request.content()))
                        .responseSingle(
                            (r, body) -> body.switchIfEmpty(Mono.just(Unpooled.EMPTY_BUFFER))
                                .map(
                                    b -> new DefaultFullHttpResponse(
                                        r.version(),
                                        r.status(),
                                        b.retain(),
                                        r.responseHeaders(),
                                        EmptyHttpHeaders.INSTANCE
                                    )
                                )
                        )
                )
                .toArray(Mono[]::new);

            if (orderer == false) {
                return ParallelFlux.from(monos).sequential().collectList().block();
            } else {
                return Flux.concat(monos).flatMapSequential(r -> Mono.just(r)).collectList().block();
            }
        } finally {
            eventLoopGroup.shutdownGracefully().awaitUninterruptibly();
        }
    }

    private FullHttpResponse sendRequestStream(
        final InetSocketAddress remoteAddress,
        final HttpRequest request,
        final Stream<ToXContent> stream
    ) {
        final NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
        try {
            final HttpClient client = createClient(remoteAddress, eventLoopGroup);

            return client.headers(h -> h.add(request.headers()))
                .baseUrl(request.getUri())
                .request(request.method())
                .send(Flux.fromStream(stream).map(s -> {
                    try (XContentBuilder builder = XContentType.JSON.contentBuilder()) {
                        return Unpooled.wrappedBuffer(
                            s.toXContent(builder, ToXContent.EMPTY_PARAMS).toString().getBytes(StandardCharsets.UTF_8)
                        );
                    } catch (final IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                }))
                .response(
                    (r, c) -> c.aggregate()
                        .map(
                            b -> new DefaultFullHttpResponse(
                                r.version(),
                                r.status(),
                                b.retain(),
                                r.responseHeaders(),
                                EmptyHttpHeaders.INSTANCE
                            )
                        )
                )
                .blockLast();

        } finally {
            eventLoopGroup.shutdownGracefully().awaitUninterruptibly();
        }
    }

    private HttpClient createClient(final InetSocketAddress remoteAddress, final NioEventLoopGroup eventLoopGroup) {
        final HttpClient client = HttpClient.newConnection()
            .resolver(DefaultAddressResolverGroup.INSTANCE)
            .runOn(eventLoopGroup)
            .host(remoteAddress.getHostString())
            .port(remoteAddress.getPort())
            .compress(compression);

        if (secure) {
            return client.secure(
                spec -> spec.sslContext(
                    OpenSearchTestCase.randomBoolean()
                        /* switch between HTTP 1.1/HTTP 2 randomly, both are supported */ ? Http11SslContextSpec.forClient()
                            .configure(s -> s.clientAuth(ClientAuth.NONE).trustManager(InsecureTrustManagerFactory.INSTANCE))
                        : Http2SslContextSpec.forClient()
                            .configure(s -> s.clientAuth(ClientAuth.NONE).trustManager(InsecureTrustManagerFactory.INSTANCE))
                )
            );
        }

        return client;
    }

    @Override
    public void close() {

    }
}
