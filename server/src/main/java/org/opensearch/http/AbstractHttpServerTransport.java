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

package org.opensearch.http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.network.CloseableChannel;
import org.opensearch.common.network.NetworkAddress;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.NetworkExceptionHelper;
import org.opensearch.common.transport.PortsRange;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanBuilder;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.channels.TraceableHttpChannel;
import org.opensearch.telemetry.tracing.channels.TraceableRestChannel;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.BindTransportException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_BIND_HOST;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_PORT;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_PUBLISH_HOST;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_PUBLISH_PORT;

/**
 * Base HttpServer class
 *
 * @opensearch.internal
 */
public abstract class AbstractHttpServerTransport extends AbstractLifecycleComponent implements HttpServerTransport {
    private static final Logger logger = LogManager.getLogger(AbstractHttpServerTransport.class);
    private static final ActionListener<Void> NO_OP = ActionListener.wrap(() -> {});

    protected final Settings settings;
    public final HttpHandlingSettings handlingSettings;
    protected final NetworkService networkService;
    protected final BigArrays bigArrays;
    protected final ThreadPool threadPool;
    protected final Dispatcher dispatcher;
    protected final CorsHandler corsHandler;
    private final NamedXContentRegistry xContentRegistry;

    protected final PortsRange port;
    protected final ByteSizeValue maxContentLength;
    private final String[] bindHosts;
    private final String[] publishHosts;

    private volatile BoundTransportAddress boundAddress;
    private final AtomicLong totalChannelsAccepted = new AtomicLong();
    private final Set<HttpChannel> httpChannels = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<HttpServerChannel> httpServerChannels = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final HttpTracer httpTracer;
    private final Tracer tracer;

    protected AbstractHttpServerTransport(
        Settings settings,
        NetworkService networkService,
        BigArrays bigArrays,
        ThreadPool threadPool,
        NamedXContentRegistry xContentRegistry,
        Dispatcher dispatcher,
        ClusterSettings clusterSettings,
        Tracer telemetryTracer
    ) {
        this.settings = settings;
        this.networkService = networkService;
        this.bigArrays = bigArrays;
        this.threadPool = threadPool;
        this.xContentRegistry = xContentRegistry;
        this.dispatcher = dispatcher;
        this.handlingSettings = HttpHandlingSettings.fromSettings(settings);
        this.corsHandler = CorsHandler.fromSettings(settings);

        // we can't make the network.bind_host a fallback since we already fall back to http.host hence the extra conditional here
        List<String> httpBindHost = SETTING_HTTP_BIND_HOST.get(settings);
        this.bindHosts = (httpBindHost.isEmpty() ? NetworkService.GLOBAL_NETWORK_BIND_HOST_SETTING.get(settings) : httpBindHost).toArray(
            Strings.EMPTY_ARRAY
        );
        // we can't make the network.publish_host a fallback since we already fall back to http.host hence the extra conditional here
        List<String> httpPublishHost = SETTING_HTTP_PUBLISH_HOST.get(settings);
        this.publishHosts = (httpPublishHost.isEmpty() ? NetworkService.GLOBAL_NETWORK_PUBLISH_HOST_SETTING.get(settings) : httpPublishHost)
            .toArray(Strings.EMPTY_ARRAY);

        this.port = SETTING_HTTP_PORT.get(settings);

        this.maxContentLength = SETTING_HTTP_MAX_CONTENT_LENGTH.get(settings);
        this.httpTracer = new HttpTracer(settings, clusterSettings);
        this.tracer = telemetryTracer;
    }

    @Override
    public BoundTransportAddress boundAddress() {
        return this.boundAddress;
    }

    @Override
    public HttpInfo info() {
        BoundTransportAddress boundTransportAddress = boundAddress();
        if (boundTransportAddress == null) {
            return null;
        }
        return new HttpInfo(boundTransportAddress, maxContentLength.getBytes());
    }

    @Override
    public HttpStats stats() {
        return new HttpStats(httpChannels.size(), totalChannelsAccepted.get());
    }

    protected void bindServer() {
        // Bind and start to accept incoming connections.
        InetAddress hostAddresses[];
        try {
            hostAddresses = networkService.resolveBindHostAddresses(bindHosts);
        } catch (IOException e) {
            throw new BindHttpException("Failed to resolve host [" + Arrays.toString(bindHosts) + "]", e);
        }

        List<TransportAddress> boundAddresses = new ArrayList<>(hostAddresses.length);
        for (InetAddress address : hostAddresses) {
            boundAddresses.add(bindAddress(address));
        }

        final InetAddress publishInetAddress;
        try {
            publishInetAddress = networkService.resolvePublishHostAddresses(publishHosts);
        } catch (Exception e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }

        final int publishPort = resolvePublishPort(settings, boundAddresses, publishInetAddress);
        TransportAddress publishAddress = new TransportAddress(new InetSocketAddress(publishInetAddress, publishPort));
        this.boundAddress = new BoundTransportAddress(boundAddresses.toArray(new TransportAddress[0]), publishAddress);
        logger.info("{}", boundAddress);
    }

    private TransportAddress bindAddress(final InetAddress hostAddress) {
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();
        boolean success = port.iterate(portNumber -> {
            try {
                synchronized (httpServerChannels) {
                    HttpServerChannel httpServerChannel = bind(new InetSocketAddress(hostAddress, portNumber));
                    httpServerChannels.add(httpServerChannel);
                    boundSocket.set(httpServerChannel.getLocalAddress());
                }
            } catch (Exception e) {
                lastException.set(e);
                return false;
            }
            return true;
        });
        if (!success) {
            throw new BindHttpException("Failed to bind to " + NetworkAddress.format(hostAddress, port), lastException.get());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Bound http to address {{}}", NetworkAddress.format(boundSocket.get()));
        }
        return new TransportAddress(boundSocket.get());
    }

    protected abstract HttpServerChannel bind(InetSocketAddress hostAddress) throws Exception;

    @Override
    protected void doStop() {
        synchronized (httpServerChannels) {
            if (httpServerChannels.isEmpty() == false) {
                try {
                    CloseableChannel.closeChannels(new ArrayList<>(httpServerChannels), true);
                } catch (Exception e) {
                    logger.warn("exception while closing channels", e);
                } finally {
                    httpServerChannels.clear();
                }
            }
        }

        try {
            CloseableChannel.closeChannels(new ArrayList<>(httpChannels), true);
        } catch (Exception e) {
            logger.warn("unexpected exception while closing http channels", e);
        }
        httpChannels.clear();

        stopInternal();
    }

    @Override
    protected void doClose() {}

    /**
     * Called to tear down internal resources
     */
    protected abstract void stopInternal();

    // package private for tests
    static int resolvePublishPort(Settings settings, List<TransportAddress> boundAddresses, InetAddress publishInetAddress) {
        int publishPort = SETTING_HTTP_PUBLISH_PORT.get(settings);

        if (publishPort < 0) {
            for (TransportAddress boundAddress : boundAddresses) {
                InetAddress boundInetAddress = boundAddress.address().getAddress();
                if (boundInetAddress.isAnyLocalAddress() || boundInetAddress.equals(publishInetAddress)) {
                    publishPort = boundAddress.getPort();
                    break;
                }
            }
        }

        // if no matching boundAddress found, check if there is a unique port for all bound addresses
        if (publishPort < 0) {
            final Set<Integer> ports = new HashSet<>();
            for (TransportAddress boundAddress : boundAddresses) {
                ports.add(boundAddress.getPort());
            }
            if (ports.size() == 1) {
                publishPort = ports.iterator().next();
            }
        }

        if (publishPort < 0) {
            throw new BindHttpException(
                "Failed to auto-resolve http publish port, multiple bound addresses "
                    + boundAddresses
                    + " with distinct ports and none of them matched the publish address ("
                    + publishInetAddress
                    + "). "
                    + "Please specify a unique port by setting "
                    + SETTING_HTTP_PORT.getKey()
                    + " or "
                    + SETTING_HTTP_PUBLISH_PORT.getKey()
            );
        }
        return publishPort;
    }

    public void onException(HttpChannel channel, Exception e) {
        channel.handleException(e);
        if (lifecycle.started() == false) {
            // just close and ignore - we are already stopped and just need to make sure we release all resources
            CloseableChannel.closeChannel(channel);
            return;
        }
        if (NetworkExceptionHelper.isCloseConnectionException(e)) {
            logger.trace(
                () -> new ParameterizedMessage(
                    "close connection exception caught while handling client http traffic, closing connection {}",
                    channel
                ),
                e
            );
            CloseableChannel.closeChannel(channel);
        } else if (NetworkExceptionHelper.isConnectException(e)) {
            logger.trace(
                () -> new ParameterizedMessage(
                    "connect exception caught while handling client http traffic, closing connection {}",
                    channel
                ),
                e
            );
            CloseableChannel.closeChannel(channel);
        } else if (e instanceof HttpReadTimeoutException) {
            logger.trace(() -> new ParameterizedMessage("http read timeout, closing connection {}", channel), e);
            CloseableChannel.closeChannel(channel);
        } else if (e instanceof CancelledKeyException) {
            logger.trace(
                () -> new ParameterizedMessage(
                    "cancelled key exception caught while handling client http traffic, closing connection {}",
                    channel
                ),
                e
            );
            CloseableChannel.closeChannel(channel);
        } else {
            logger.warn(
                () -> new ParameterizedMessage("caught exception while handling client http traffic, closing connection {}", channel),
                e
            );
            CloseableChannel.closeChannel(channel);
        }
    }

    protected void onServerException(HttpServerChannel channel, Exception e) {
        logger.error(new ParameterizedMessage("exception from http server channel caught on transport layer [channel={}]", channel), e);
    }

    protected void serverAcceptedChannel(HttpChannel httpChannel) {
        boolean addedOnThisCall = httpChannels.add(httpChannel);
        assert addedOnThisCall : "Channel should only be added to http channel set once";
        totalChannelsAccepted.incrementAndGet();
        httpChannel.addCloseListener(ActionListener.wrap(() -> httpChannels.remove(httpChannel)));
        logger.trace(() -> new ParameterizedMessage("Http channel accepted: {}", httpChannel));
    }

    /**
     * This method handles an incoming http request as a stream.
     *
     * @param httpRequest that is incoming
     * @param httpChannel that received the http request
     */
    public void incomingStream(HttpRequest httpRequest, final StreamingHttpChannel httpChannel) {
        handleIncomingRequest(httpRequest, httpChannel, httpRequest.getInboundException());
    }

    /**
     * This method handles an incoming http request.
     *
     * @param httpRequest that is incoming
     * @param httpChannel that received the http request
     */
    public void incomingRequest(final HttpRequest httpRequest, final HttpChannel httpChannel) {
        final Span span = tracer.startSpan(SpanBuilder.from(httpRequest), extractHeaders(httpRequest.getHeaders()));
        try (final SpanScope httpRequestSpanScope = tracer.withSpanInScope(span)) {
            HttpChannel traceableHttpChannel = TraceableHttpChannel.create(httpChannel, span, tracer);
            handleIncomingRequest(httpRequest, traceableHttpChannel, httpRequest.getInboundException());
        }
    }

    // Visible for testing
    void dispatchRequest(final RestRequest restRequest, final RestChannel channel, final Throwable badRequestCause) {
        RestChannel traceableRestChannel = channel;
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            final Span span = tracer.startSpan(SpanBuilder.from(restRequest));
            try (final SpanScope spanScope = tracer.withSpanInScope(span)) {
                if (channel != null) {
                    traceableRestChannel = TraceableRestChannel.create(channel, span, tracer);
                }
                if (badRequestCause != null) {
                    dispatcher.dispatchBadRequest(traceableRestChannel, threadContext, badRequestCause);
                } else {
                    dispatcher.dispatchRequest(restRequest, traceableRestChannel, threadContext);
                }
            }
        }

    }

    private void handleIncomingRequest(final HttpRequest httpRequest, final HttpChannel httpChannel, final Exception exception) {
        if (exception == null) {
            HttpResponse earlyResponse = corsHandler.handleInbound(httpRequest);
            if (earlyResponse != null) {
                httpChannel.sendResponse(earlyResponse, earlyResponseListener(httpRequest, httpChannel));
                httpRequest.release();
                return;
            }
        }

        Exception badRequestCause = exception;

        /*
         * We want to create a REST request from the incoming request from Netty. However, creating this request could fail if there
         * are incorrectly encoded parameters, or the Content-Type header is invalid. If one of these specific failures occurs, we
         * attempt to create a REST request again without the input that caused the exception (e.g., we remove the Content-Type header,
         * or skip decoding the parameters). Once we have a request in hand, we then dispatch the request as a bad request with the
         * underlying exception that caused us to treat the request as bad.
         */
        final RestRequest restRequest;
        {
            RestRequest innerRestRequest;
            try {
                innerRestRequest = RestRequest.request(xContentRegistry, httpRequest, httpChannel);
            } catch (final RestRequest.ContentTypeHeaderException e) {
                badRequestCause = ExceptionsHelper.useOrSuppress(badRequestCause, e);
                innerRestRequest = requestWithoutContentTypeHeader(httpRequest, httpChannel, badRequestCause);
            } catch (final RestRequest.BadParameterException e) {
                badRequestCause = ExceptionsHelper.useOrSuppress(badRequestCause, e);
                innerRestRequest = RestRequest.requestWithoutParameters(xContentRegistry, httpRequest, httpChannel);
            }
            restRequest = innerRestRequest;
        }

        final HttpTracer trace = httpTracer.maybeTraceRequest(restRequest, exception);

        /*
         * We now want to create a channel used to send the response on. However, creating this channel can fail if there are invalid
         * parameter values for any of the filter_path, human, or pretty parameters. We detect these specific failures via an
         * IllegalArgumentException from the channel constructor and then attempt to create a new channel that bypasses parsing of these
         * parameter values.
         */
        final RestChannel channel;
        {
            RestChannel innerChannel;
            ThreadContext threadContext = threadPool.getThreadContext();
            try {
                if (httpChannel instanceof StreamingHttpChannel) {
                    innerChannel = new DefaultStreamingRestChannel(
                        (StreamingHttpChannel) httpChannel,
                        httpRequest,
                        restRequest,
                        bigArrays,
                        handlingSettings,
                        threadContext,
                        corsHandler,
                        trace
                    );
                } else {
                    innerChannel = new DefaultRestChannel(
                        httpChannel,
                        httpRequest,
                        restRequest,
                        bigArrays,
                        handlingSettings,
                        threadContext,
                        corsHandler,
                        trace
                    );
                }
            } catch (final IllegalArgumentException e) {
                badRequestCause = ExceptionsHelper.useOrSuppress(badRequestCause, e);
                final RestRequest innerRequest = RestRequest.requestWithoutParameters(xContentRegistry, httpRequest, httpChannel);

                if (httpChannel instanceof StreamingHttpChannel) {
                    innerChannel = new DefaultStreamingRestChannel(
                        (StreamingHttpChannel) httpChannel,
                        httpRequest,
                        innerRequest,
                        bigArrays,
                        handlingSettings,
                        threadContext,
                        corsHandler,
                        trace
                    );
                } else {
                    innerChannel = new DefaultRestChannel(
                        httpChannel,
                        httpRequest,
                        innerRequest,
                        bigArrays,
                        handlingSettings,
                        threadContext,
                        corsHandler,
                        trace
                    );
                }
            }
            channel = innerChannel;
        }

        dispatchRequest(restRequest, channel, badRequestCause);
    }

    private RestRequest requestWithoutContentTypeHeader(HttpRequest httpRequest, HttpChannel httpChannel, Exception badRequestCause) {
        HttpRequest httpRequestWithoutContentType = httpRequest.removeHeader("Content-Type");
        try {
            return RestRequest.request(xContentRegistry, httpRequestWithoutContentType, httpChannel);
        } catch (final RestRequest.BadParameterException e) {
            badRequestCause.addSuppressed(e);
            return RestRequest.requestWithoutParameters(xContentRegistry, httpRequestWithoutContentType, httpChannel);
        }
    }

    private static ActionListener<Void> earlyResponseListener(HttpRequest request, HttpChannel httpChannel) {
        if (HttpUtils.shouldCloseConnection(request)) {
            return ActionListener.wrap(() -> CloseableChannel.closeChannel(httpChannel));
        } else {
            return NO_OP;
        }
    }

    @SuppressWarnings("unchecked")
    private static <Values extends Collection<String>> Map<String, Collection<String>> extractHeaders(Map<String, Values> headers) {
        return (Map<String, Collection<String>>) headers;
    }
}
