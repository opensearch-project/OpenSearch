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

package org.opensearch.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.ConnectionClosedException;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.AuthCache;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpTrace;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.entity.HttpEntityWrapper;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;

import javax.net.ssl.SSLHandshakeException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;

/**
 * Client that connects to an OpenSearch cluster through HTTP.
 * <p>
 * Must be created using {@link RestClientBuilder}, which allows to set all the different options or just rely on defaults.
 * The hosts that are part of the cluster need to be provided at creation time, but can also be replaced later
 * by calling {@link #setNodes(Collection)}.
 * <p>
 * The method {@link #performRequest(Request)} allows to send a request to the cluster. When
 * sending a request, a host gets selected out of the provided ones in a round-robin fashion. Failing hosts are marked dead and
 * retried after a certain amount of time (minimum 1 minute, maximum 30 minutes), depending on how many times they previously
 * failed (the more failures, the later they will be retried). In case of failures all of the alive nodes (or dead nodes that
 * deserve a retry) are retried until one responds or none of them does, in which case an {@link IOException} will be thrown.
 * <p>
 * Requests can be either synchronous or asynchronous. The asynchronous variants all end with {@code Async}.
 * <p>
 * Requests can be traced by enabling trace logging for "tracer". The trace logger outputs requests and responses in curl format.
 */
public class RestClient implements Closeable {

    private static final Log logger = LogFactory.getLog(RestClient.class);

    private final CloseableHttpAsyncClient client;
    // We don't rely on default headers supported by HttpAsyncClient as those cannot be replaced.
    // These are package private for tests.
    final List<Header> defaultHeaders;
    private final String pathPrefix;
    private final AtomicInteger lastNodeIndex = new AtomicInteger(0);
    private final ConcurrentMap<HttpHost, DeadHostState> denylist = new ConcurrentHashMap<>();
    private final FailureListener failureListener;
    private final NodeSelector nodeSelector;
    private volatile NodeTuple<List<Node>> nodeTuple;
    private final WarningsHandler warningsHandler;
    private final boolean compressionEnabled;
    private final Optional<Boolean> chunkedEnabled;

    RestClient(
        CloseableHttpAsyncClient client,
        Header[] defaultHeaders,
        List<Node> nodes,
        String pathPrefix,
        FailureListener failureListener,
        NodeSelector nodeSelector,
        boolean strictDeprecationMode,
        boolean compressionEnabled,
        boolean chunkedEnabled
    ) {
        this.client = client;
        this.defaultHeaders = Collections.unmodifiableList(Arrays.asList(defaultHeaders));
        this.failureListener = failureListener;
        this.pathPrefix = pathPrefix;
        this.nodeSelector = nodeSelector;
        this.warningsHandler = strictDeprecationMode ? WarningsHandler.STRICT : WarningsHandler.PERMISSIVE;
        this.compressionEnabled = compressionEnabled;
        this.chunkedEnabled = Optional.of(chunkedEnabled);
        setNodes(nodes);
    }

    RestClient(
        CloseableHttpAsyncClient client,
        Header[] defaultHeaders,
        List<Node> nodes,
        String pathPrefix,
        FailureListener failureListener,
        NodeSelector nodeSelector,
        boolean strictDeprecationMode,
        boolean compressionEnabled
    ) {
        this.client = client;
        this.defaultHeaders = Collections.unmodifiableList(Arrays.asList(defaultHeaders));
        this.failureListener = failureListener;
        this.pathPrefix = pathPrefix;
        this.nodeSelector = nodeSelector;
        this.warningsHandler = strictDeprecationMode ? WarningsHandler.STRICT : WarningsHandler.PERMISSIVE;
        this.compressionEnabled = compressionEnabled;
        this.chunkedEnabled = Optional.empty();
        setNodes(nodes);
    }

    /**
     * Returns a new {@link RestClientBuilder} to help with {@link RestClient} creation.
     * Creates a new builder instance and sets the nodes that the client will send requests to.
     *
     * @param cloudId a valid elastic cloud cloudId that will route to a cluster. The cloudId is located in
     *                the user console https://cloud.elastic.co and will resemble a string like the following
     *                optionalHumanReadableName:dXMtZWFzdC0xLmF3cy5mb3VuZC5pbyRlbGFzdGljc2VhcmNoJGtpYmFuYQ==
     */
    public static RestClientBuilder builder(String cloudId) {
        // there is an optional first portion of the cloudId that is a human readable string, but it is not used.
        if (cloudId.contains(":")) {
            if (cloudId.indexOf(":") == cloudId.length() - 1) {
                throw new IllegalStateException("cloudId " + cloudId + " must begin with a human readable identifier followed by a colon");
            }
            cloudId = cloudId.substring(cloudId.indexOf(":") + 1);
        }

        String decoded = new String(Base64.getDecoder().decode(cloudId), UTF_8);
        // once decoded the parts are separated by a $ character.
        // they are respectively domain name and optional port, opensearch id, opensearch-dashboards id
        String[] decodedParts = decoded.split("\\$");
        if (decodedParts.length != 3) {
            throw new IllegalStateException("cloudId " + cloudId + " did not decode to a cluster identifier correctly");
        }

        // domain name and optional port
        String[] domainAndMaybePort = decodedParts[0].split(":", 2);
        String domain = domainAndMaybePort[0];
        int port;

        if (domainAndMaybePort.length == 2) {
            try {
                port = Integer.parseInt(domainAndMaybePort[1]);
            } catch (NumberFormatException nfe) {
                throw new IllegalStateException("cloudId " + cloudId + " does not contain a valid port number");
            }
        } else {
            port = 443;
        }

        String url = decodedParts[1] + "." + domain;
        return builder(new HttpHost(url, port, "https"));
    }

    /**
     * Returns a new {@link RestClientBuilder} to help with {@link RestClient} creation.
     * Creates a new builder instance and sets the hosts that the client will send requests to.
     * <p>
     * Prefer this to {@link #builder(HttpHost...)} if you have metadata up front about the nodes.
     * If you don't either one is fine.
     *
     * @param nodes The nodes that the client will send requests to.
     */
    public static RestClientBuilder builder(Node... nodes) {
        return new RestClientBuilder(nodes == null ? null : Arrays.asList(nodes));
    }

    /**
     * Returns a new {@link RestClientBuilder} to help with {@link RestClient} creation.
     * Creates a new builder instance and sets the nodes that the client will send requests to.
     * <p>
     * You can use this if you do not have metadata up front about the nodes. If you do, prefer
     * {@link #builder(Node...)}.
     * @see Node#Node(HttpHost)
     *
     * @param hosts The hosts that the client will send requests to.
     */
    public static RestClientBuilder builder(HttpHost... hosts) {
        if (hosts == null || hosts.length == 0) {
            throw new IllegalArgumentException("hosts must not be null nor empty");
        }
        List<Node> nodes = Arrays.stream(hosts).map(Node::new).collect(Collectors.toList());
        return new RestClientBuilder(nodes);
    }

    /**
     * Replaces the nodes with which the client communicates.
     *
     * @param nodes the new nodes to communicate with.
     */
    public synchronized void setNodes(Collection<Node> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be null or empty");
        }
        AuthCache authCache = new BasicAuthCache();

        Map<HttpHost, Node> nodesByHost = new LinkedHashMap<>();
        for (Node node : nodes) {
            Objects.requireNonNull(node, "node cannot be null");
            // TODO should we throw an IAE if we have two nodes with the same host?
            nodesByHost.put(node.getHost(), node);
            authCache.put(node.getHost(), new BasicScheme());
        }
        this.nodeTuple = new NodeTuple<>(Collections.unmodifiableList(new ArrayList<>(nodesByHost.values())), authCache);
        this.denylist.clear();
    }

    /**
     * Get the list of nodes that the client knows about. The list is
     * unmodifiable.
     */
    public List<Node> getNodes() {
        return nodeTuple.nodes;
    }

    /**
     * check client running status
     * @return client running status
     */
    public boolean isRunning() {
        return client.isRunning();
    }

    /**
     * Sends a streaming request to the OpenSearch cluster that the client points to and returns streaming response. <strong>This is an experimental API</strong>.
     * @param request streaming request
     * @return streaming response
     * @throws IOException IOException
     */
    public StreamingResponse<ByteBuffer> streamRequest(StreamingRequest<ByteBuffer> request) throws IOException {
        final InternalStreamingRequest internalRequest = new InternalStreamingRequest(request);

        final StreamingResponse<ByteBuffer> response = new StreamingResponse<>(
            internalRequest.httpRequest.getRequestLine(),
            streamRequest(nextNodes(), internalRequest)
        );

        return response;
    }

    /**
     * Sends a request to the OpenSearch cluster that the client points to.
     * Blocks until the request is completed and returns its response or fails
     * by throwing an exception. Selects a host out of the provided ones in a
     * round-robin fashion. Failing hosts are marked dead and retried after a
     * certain amount of time (minimum 1 minute, maximum 30 minutes), depending
     * on how many times they previously failed (the more failures, the later
     * they will be retried). In case of failures all of the alive nodes (or
     * dead nodes that deserve a retry) are retried until one responds or none
     * of them does, in which case an {@link IOException} will be thrown.
     * <p>
     * This method works by performing an asynchronous call and waiting
     * for the result. If the asynchronous call throws an exception we wrap
     * it and rethrow it so that the stack trace attached to the exception
     * contains the call site. While we attempt to preserve the original
     * exception this isn't always possible and likely haven't covered all of
     * the cases. You can get the original exception from
     * {@link Exception#getCause()}.
     *
     * @param request the request to perform
     * @return the response returned by OpenSearch
     * @throws IOException in case of a problem or the connection was aborted
     * @throws ClientProtocolException in case of an http protocol error
     * @throws ResponseException in case OpenSearch responded with a status code that indicated an error
     */
    public Response performRequest(Request request) throws IOException {
        InternalRequest internalRequest = new InternalRequest(request);
        return performRequest(nextNodes(), internalRequest, null);
    }

    private Response performRequest(final NodeTuple<Iterator<Node>> nodeTuple, final InternalRequest request, Exception previousException)
        throws IOException {
        RequestContext<HttpResponse> context = request.createContextForNextAttempt(nodeTuple.nodes.next(), nodeTuple.authCache);
        HttpResponse httpResponse;
        try {
            httpResponse = client.execute(context.requestProducer(), context.asyncResponseConsumer(), context.context(), null).get();
        } catch (Exception e) {
            RequestLogger.logFailedRequest(logger, request.httpRequest, context.node(), e);
            onFailure(context.node());
            Exception cause = extractAndWrapCause(e);
            addSuppressedException(previousException, cause);
            if (nodeTuple.nodes.hasNext()) {
                return performRequest(nodeTuple, request, cause);
            }
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new IllegalStateException("unexpected exception type: must be either RuntimeException or IOException", cause);
        }
        ResponseOrResponseException responseOrResponseException = convertResponse(request, context.node(), httpResponse);
        if (responseOrResponseException.responseException == null) {
            return responseOrResponseException.response;
        }
        addSuppressedException(previousException, responseOrResponseException.responseException);
        if (nodeTuple.nodes.hasNext()) {
            return performRequest(nodeTuple, request, responseOrResponseException.responseException);
        }
        throw responseOrResponseException.responseException;
    }

    private Publisher<Message<HttpResponse, Publisher<ByteBuffer>>> streamRequest(
        final NodeTuple<Iterator<Node>> nodeTuple,
        final InternalStreamingRequest request
    ) throws IOException {
        return request.cancellable.callIfNotCancelled(() -> {
            final Node node = nodeTuple.nodes.next();

            final Mono<Message<HttpResponse, Publisher<ByteBuffer>>> publisher = Mono.create(emitter -> {
                final RequestContext<HttpResponse> context = request.createContextForNextAttempt(node, nodeTuple.authCache, emitter);
                client.execute(context.requestProducer(), context.asyncResponseConsumer(), context.context(), null);
            });

            return publisher.flatMap(message -> {
                try {
                    final ResponseOrResponseException responseOrResponseException = convertResponse(request, node, message);
                    if (responseOrResponseException.responseException == null) {
                        return Mono.just(
                            new Message<>(
                                message.getHead(),
                                Flux.from(message.getBody()).flatMapSequential(b -> Flux.fromIterable(frame(b)))
                            )
                        );
                    } else {
                        if (nodeTuple.nodes.hasNext()) {
                            return Mono.from(streamRequest(nodeTuple, request));
                        } else {
                            return Mono.error(responseOrResponseException.responseException);
                        }
                    }
                } catch (final Exception ex) {
                    return Mono.error(ex);
                }
            });
        });
    }

    /**
     * Frame the {@link ByteBuffer} into individual chunks that are separated by '\r\n' sequence.
     * @param b {@link ByteBuffer} to split
     * @return individual chunks
     */
    private static Collection<ByteBuffer> frame(ByteBuffer b) {
        final Collection<ByteBuffer> buffers = new ArrayList<>();

        int position = b.position();
        while (b.hasRemaining()) {
            // Skip the chunk separator when it comes right at the beginning
            if (b.get() == '\r' && b.hasRemaining() && b.position() > 1) {
                if (b.get() == '\n') {
                    final byte[] chunk = new byte[b.position() - position];

                    b.position(position);
                    b.get(chunk);

                    // Do not copy the '\r\n' sequence
                    buffers.add(ByteBuffer.wrap(chunk, 0, chunk.length - 2));
                    position = b.position();
                }
            }
        }

        if (buffers.isEmpty()) {
            return Collections.singleton(b);
        }

        // Copy last chunk
        if (position != b.position()) {
            final byte[] chunk = new byte[b.position() - position];

            b.position(position);
            b.get(chunk);

            buffers.add(ByteBuffer.wrap(chunk, 0, chunk.length));
        }

        return buffers;
    }

    private ResponseOrResponseException convertResponse(InternalRequest request, Node node, HttpResponse httpResponse) throws IOException {
        RequestLogger.logResponse(logger, request.httpRequest, node.getHost(), httpResponse);
        int statusCode = httpResponse.getStatusLine().getStatusCode();

        Optional.ofNullable(httpResponse.getEntity())
            .map(HttpEntity::getContentEncoding)
            .map(Header::getValue)
            .filter("gzip"::equalsIgnoreCase)
            .map(gzipHeaderValue -> new GzipDecompressingEntity(httpResponse.getEntity()))
            .ifPresent(httpResponse::setEntity);

        Response response = new Response(request.httpRequest.getRequestLine(), node.getHost(), httpResponse);
        if (isSuccessfulResponse(statusCode) || request.ignoreErrorCodes.contains(response.getStatusLine().getStatusCode())) {
            onResponse(node);
            if (request.warningsHandler.warningsShouldFailRequest(response.getWarnings())) {
                throw new WarningFailureException(response);
            }
            return new ResponseOrResponseException(response);
        }
        ResponseException responseException = new ResponseException(response);
        if (isRetryStatus(statusCode)) {
            // mark host dead and retry against next one
            onFailure(node);
            return new ResponseOrResponseException(responseException);
        }
        // mark host alive and don't retry, as the error should be a request problem
        onResponse(node);
        throw responseException;
    }

    private ResponseOrResponseException convertResponse(
        InternalStreamingRequest request,
        Node node,
        Message<HttpResponse, Publisher<ByteBuffer>> message
    ) throws IOException {

        // Streaming Response could accumulate a lot of data so we may not be able to fully consume it.
        final HttpResponse httpResponse = new BasicHttpResponse(message.getHead().getStatusLine());
        final Response response = new Response(request.httpRequest.getRequestLine(), node.getHost(), httpResponse);

        RequestLogger.logResponse(logger, request.httpRequest, node.getHost(), httpResponse);
        int statusCode = httpResponse.getStatusLine().getStatusCode();

        if (isSuccessfulResponse(statusCode) || request.ignoreErrorCodes.contains(response.getStatusLine().getStatusCode())) {
            onResponse(node);
            if (request.warningsHandler.warningsShouldFailRequest(response.getWarnings())) {
                throw new WarningFailureException(response);
            }
            return new ResponseOrResponseException(response);
        }
        ResponseException responseException = new ResponseException(response);
        if (isRetryStatus(statusCode)) {
            // mark host dead and retry against next one
            onFailure(node);
            return new ResponseOrResponseException(responseException);
        }
        // mark host alive and don't retry, as the error should be a request problem
        onResponse(node);
        throw responseException;
    }

    /**
     * Sends a request to the OpenSearch cluster that the client points to.
     * The request is executed asynchronously and the provided
     * {@link ResponseListener} gets notified upon request completion or
     * failure. Selects a host out of the provided ones in a round-robin
     * fashion. Failing hosts are marked dead and retried after a certain
     * amount of time (minimum 1 minute, maximum 30 minutes), depending on how
     * many times they previously failed (the more failures, the later they
     * will be retried). In case of failures all of the alive nodes (or dead
     * nodes that deserve a retry) are retried until one responds or none of
     * them does, in which case an {@link IOException} will be thrown.
     *
     * @param request the request to perform
     * @param responseListener the {@link ResponseListener} to notify when the
     *      request is completed or fails
     */
    public Cancellable performRequestAsync(Request request, ResponseListener responseListener) {
        try {
            FailureTrackingResponseListener failureTrackingResponseListener = new FailureTrackingResponseListener(responseListener);
            InternalRequest internalRequest = new InternalRequest(request);
            performRequestAsync(nextNodes(), internalRequest, failureTrackingResponseListener);
            return internalRequest.cancellable;
        } catch (Exception e) {
            responseListener.onFailure(e);
            return Cancellable.NO_OP;
        }
    }

    private void performRequestAsync(
        final NodeTuple<Iterator<Node>> nodeTuple,
        final InternalRequest request,
        final FailureTrackingResponseListener listener
    ) {
        request.cancellable.runIfNotCancelled(() -> {
            final RequestContext<HttpResponse> context = request.createContextForNextAttempt(nodeTuple.nodes.next(), nodeTuple.authCache);
            client.execute(
                context.requestProducer(),
                context.asyncResponseConsumer(),
                context.context(),
                new FutureCallback<HttpResponse>() {
                    @Override
                    public void completed(HttpResponse httpResponse) {
                        try {
                            ResponseOrResponseException responseOrResponseException = convertResponse(
                                request,
                                context.node(),
                                httpResponse
                            );
                            if (responseOrResponseException.responseException == null) {
                                listener.onSuccess(responseOrResponseException.response);
                            } else {
                                if (nodeTuple.nodes.hasNext()) {
                                    listener.trackFailure(responseOrResponseException.responseException);
                                    performRequestAsync(nodeTuple, request, listener);
                                } else {
                                    listener.onDefinitiveFailure(responseOrResponseException.responseException);
                                }
                            }
                        } catch (Exception e) {
                            listener.onDefinitiveFailure(e);
                        }
                    }

                    @Override
                    public void failed(Exception failure) {
                        try {
                            RequestLogger.logFailedRequest(logger, request.httpRequest, context.node(), failure);
                            onFailure(context.node());
                            if (nodeTuple.nodes.hasNext()) {
                                listener.trackFailure(failure);
                                performRequestAsync(nodeTuple, request, listener);
                            } else {
                                listener.onDefinitiveFailure(failure);
                            }
                        } catch (Exception e) {
                            listener.onDefinitiveFailure(e);
                        }
                    }

                    @Override
                    public void cancelled() {
                        listener.onDefinitiveFailure(Cancellable.newCancellationException());
                    }
                }
            );
        });
    }

    /**
     * Returns a non-empty {@link Iterator} of nodes to be used for a request
     * that match the {@link NodeSelector}.
     * <p>
     * If there are no living nodes that match the {@link NodeSelector}
     * this will return the dead node that matches the {@link NodeSelector}
     * that is closest to being revived.
     * @throws IOException if no nodes are available
     */
    private NodeTuple<Iterator<Node>> nextNodes() throws IOException {
        NodeTuple<List<Node>> nodeTuple = this.nodeTuple;
        Iterable<Node> hosts = selectNodes(nodeTuple, denylist, lastNodeIndex, nodeSelector);
        return new NodeTuple<>(hosts.iterator(), nodeTuple.authCache);
    }

    /**
     * Select nodes to try and sorts them so that the first one will be tried initially, then the following ones
     * if the previous attempt failed and so on. Package private for testing.
     */
    static Iterable<Node> selectNodes(
        NodeTuple<List<Node>> nodeTuple,
        Map<HttpHost, DeadHostState> denylist,
        AtomicInteger lastNodeIndex,
        NodeSelector nodeSelector
    ) throws IOException {
        /*
         * Sort the nodes into living and dead lists.
         */
        List<Node> livingNodes = new ArrayList<>(Math.max(0, nodeTuple.nodes.size() - denylist.size()));
        List<DeadNode> deadNodes = new ArrayList<>(denylist.size());
        for (Node node : nodeTuple.nodes) {
            DeadHostState deadness = denylist.get(node.getHost());
            if (deadness == null || deadness.shallBeRetried()) {
                livingNodes.add(node);
            } else {
                deadNodes.add(new DeadNode(node, deadness));
            }
        }

        if (false == livingNodes.isEmpty()) {
            /*
             * Normal state: there is at least one living node. If the
             * selector is ok with any over the living nodes then use them
             * for the request.
             */
            List<Node> selectedLivingNodes = new ArrayList<>(livingNodes);
            nodeSelector.select(selectedLivingNodes);
            if (false == selectedLivingNodes.isEmpty()) {
                /*
                 * Rotate the list using a global counter as the distance so subsequent
                 * requests will try the nodes in a different order.
                 */
                Collections.rotate(selectedLivingNodes, lastNodeIndex.getAndIncrement());
                return selectedLivingNodes;
            }
        }

        /*
         * Last resort: there are no good nodes to use, either because
         * the selector rejected all the living nodes or because there aren't
         * any living ones. Either way, we want to revive a single dead node
         * that the NodeSelectors are OK with. We do this by passing the dead
         * nodes through the NodeSelector so it can have its say in which nodes
         * are ok. If the selector is ok with any of the nodes then we will take
         * the one in the list that has the lowest revival time and try it.
         */
        if (false == deadNodes.isEmpty()) {
            final List<DeadNode> selectedDeadNodes = new ArrayList<>(deadNodes);
            /*
             * We'd like NodeSelectors to remove items directly from deadNodes
             * so we can find the minimum after it is filtered without having
             * to compare many things. This saves us a sort on the unfiltered
             * list.
             */
            nodeSelector.select(() -> new DeadNodeIteratorAdapter(selectedDeadNodes.iterator()));
            if (false == selectedDeadNodes.isEmpty()) {
                return singletonList(Collections.min(selectedDeadNodes).node);
            }
        }
        throw new IOException(
            "NodeSelector [" + nodeSelector + "] rejected all nodes, " + "living " + livingNodes + " and dead " + deadNodes
        );
    }

    /**
     * Called after each successful request call.
     * Receives as an argument the host that was used for the successful request.
     */
    private void onResponse(Node node) {
        DeadHostState removedHost = this.denylist.remove(node.getHost());
        if (logger.isDebugEnabled() && removedHost != null) {
            logger.debug("removed [" + node + "] from denylist");
        }
    }

    /**
     * Called after each failed attempt.
     * Receives as an argument the host that was used for the failed attempt.
     */
    private void onFailure(Node node) {
        while (true) {
            DeadHostState previousDeadHostState = denylist.putIfAbsent(
                node.getHost(),
                new DeadHostState(DeadHostState.DEFAULT_TIME_SUPPLIER)
            );
            if (previousDeadHostState == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("added [" + node + "] to denylist");
                }
                break;
            }
            if (denylist.replace(node.getHost(), previousDeadHostState, new DeadHostState(previousDeadHostState))) {
                if (logger.isDebugEnabled()) {
                    logger.debug("updated [" + node + "] already in denylist");
                }
                break;
            }
        }
        failureListener.onFailure(node);
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    private static boolean isSuccessfulResponse(int statusCode) {
        return statusCode < 300;
    }

    private static boolean isRetryStatus(int statusCode) {
        switch (statusCode) {
            case 502:
            case 503:
            case 504:
                return true;
        }
        return false;
    }

    private static void addSuppressedException(Exception suppressedException, Exception currentException) {
        if (suppressedException != null) {
            currentException.addSuppressed(suppressedException);
        }
    }

    private HttpRequestBase createHttpRequest(String method, URI uri, HttpEntity entity) {
        switch (method.toUpperCase(Locale.ROOT)) {
            case HttpDeleteWithEntity.METHOD_NAME:
                return addRequestBody(new HttpDeleteWithEntity(uri), entity);
            case HttpGetWithEntity.METHOD_NAME:
                return addRequestBody(new HttpGetWithEntity(uri), entity);
            case HttpHead.METHOD_NAME:
                return addRequestBody(new HttpHead(uri), entity);
            case HttpOptions.METHOD_NAME:
                return addRequestBody(new HttpOptions(uri), entity);
            case HttpPatch.METHOD_NAME:
                return addRequestBody(new HttpPatch(uri), entity);
            case HttpPost.METHOD_NAME:
                HttpPost httpPost = new HttpPost(uri);
                addRequestBody(httpPost, entity);
                return httpPost;
            case HttpPut.METHOD_NAME:
                return addRequestBody(new HttpPut(uri), entity);
            case HttpTrace.METHOD_NAME:
                return addRequestBody(new HttpTrace(uri), entity);
            default:
                throw new UnsupportedOperationException("http method not supported: " + method);
        }
    }

    private HttpRequestBase addRequestBody(HttpRequestBase httpRequest, HttpEntity entity) {
        if (entity != null) {
            if (httpRequest instanceof HttpEntityEnclosingRequestBase) {
                if (compressionEnabled) {
                    if (chunkedEnabled.isPresent()) {
                        entity = new ContentCompressingEntity(entity, chunkedEnabled.get());
                    } else {
                        entity = new ContentCompressingEntity(entity);
                    }
                } else if (chunkedEnabled.isPresent()) {
                    entity = new ContentHttpEntity(entity, chunkedEnabled.get());
                }
                ((HttpEntityEnclosingRequestBase) httpRequest).setEntity(entity);
            } else {
                throw new UnsupportedOperationException(httpRequest.getMethod() + " with body is not supported");
            }
        }
        return httpRequest;
    }

    static URI buildUri(String pathPrefix, String path, Map<String, String> params) {
        Objects.requireNonNull(path, "path must not be null");
        try {
            String fullPath;
            if (pathPrefix != null && pathPrefix.isEmpty() == false) {
                if (pathPrefix.endsWith("/") && path.startsWith("/")) {
                    fullPath = pathPrefix.substring(0, pathPrefix.length() - 1) + path;
                } else if (pathPrefix.endsWith("/") || path.startsWith("/")) {
                    fullPath = pathPrefix + path;
                } else {
                    fullPath = pathPrefix + "/" + path;
                }
            } else {
                fullPath = path;
            }

            URIBuilder uriBuilder = new URIBuilder(fullPath);
            for (Map.Entry<String, String> param : params.entrySet()) {
                uriBuilder.addParameter(param.getKey(), param.getValue());
            }
            return uriBuilder.build();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    /**
     * Listener used in any async call to wrap the provided user listener (or SyncResponseListener in sync calls).
     * Allows to track potential failures coming from the different retry attempts and returning to the original listener
     * only when we got a response (successful or not to be retried) or there are no hosts to retry against.
     */
    static class FailureTrackingResponseListener {
        private final ResponseListener responseListener;
        private volatile Exception exception;

        FailureTrackingResponseListener(ResponseListener responseListener) {
            this.responseListener = responseListener;
        }

        /**
         * Notifies the caller of a response through the wrapped listener
         */
        void onSuccess(Response response) {
            responseListener.onSuccess(response);
        }

        /**
         * Tracks one last definitive failure and returns to the caller by notifying the wrapped listener
         */
        void onDefinitiveFailure(Exception exception) {
            trackFailure(exception);
            responseListener.onFailure(this.exception);
        }

        /**
         * Tracks an exception, which caused a retry hence we should not return yet to the caller
         */
        void trackFailure(Exception exception) {
            addSuppressedException(this.exception, exception);
            this.exception = exception;
        }
    }

    /**
     * Listener that allows to be notified whenever a failure happens. Useful when sniffing is enabled, so that we can sniff on failure.
     * The default implementation is a no-op.
     */
    public static class FailureListener {
        /**
         * Create a {@link FailureListener} instance.
         */
        public FailureListener() {}

        /**
         * Notifies that the node provided as argument has just failed.
         *
         * @param node The node which has failed.
         */
        public void onFailure(Node node) {}
    }

    /**
     * {@link NodeTuple} enables the {@linkplain Node}s and {@linkplain AuthCache}
     * to be set together in a thread safe, volatile way.
     */
    static class NodeTuple<T> {
        final T nodes;
        final AuthCache authCache;

        NodeTuple(final T nodes, final AuthCache authCache) {
            this.nodes = nodes;
            this.authCache = authCache;
        }
    }

    /**
     * Contains a reference to a denylisted node and the time until it is
     * revived. We use this so we can do a single pass over the denylist.
     */
    private static class DeadNode implements Comparable<DeadNode> {
        final Node node;
        final DeadHostState deadness;

        DeadNode(Node node, DeadHostState deadness) {
            this.node = node;
            this.deadness = deadness;
        }

        @Override
        public String toString() {
            return node.toString();
        }

        @Override
        public int compareTo(DeadNode rhs) {
            return deadness.compareTo(rhs.deadness);
        }
    }

    /**
     * Adapts an <code>Iterator&lt;DeadNodeAndRevival&gt;</code> into an
     * <code>Iterator&lt;Node&gt;</code>.
     */
    private static class DeadNodeIteratorAdapter implements Iterator<Node> {
        private final Iterator<DeadNode> itr;

        private DeadNodeIteratorAdapter(Iterator<DeadNode> itr) {
            this.itr = itr;
        }

        @Override
        public boolean hasNext() {
            return itr.hasNext();
        }

        @Override
        public Node next() {
            return itr.next().node;
        }

        @Override
        public void remove() {
            itr.remove();
        }
    }

    private class InternalStreamingRequest {
        private final StreamingRequest<ByteBuffer> request;
        private final Set<Integer> ignoreErrorCodes;
        private final HttpRequestBase httpRequest;
        private final Cancellable cancellable;
        private final WarningsHandler warningsHandler;

        InternalStreamingRequest(StreamingRequest<ByteBuffer> request) {
            this.request = request;
            Map<String, String> params = new HashMap<>(request.getParameters());
            // ignore is a special parameter supported by the clients, shouldn't be sent to es
            String ignoreString = params.remove("ignore");
            this.ignoreErrorCodes = getIgnoreErrorCodes(ignoreString, request.getMethod());
            URI uri = buildUri(pathPrefix, request.getEndpoint(), params);
            this.httpRequest = createHttpRequest(request.getMethod(), uri, null);
            this.cancellable = Cancellable.fromRequest(httpRequest);
            setHeaders(httpRequest, request.getOptions().getHeaders());
            setRequestConfig(httpRequest, request.getOptions().getRequestConfig());
            this.warningsHandler = request.getOptions().getWarningsHandler() == null
                ? RestClient.this.warningsHandler
                : request.getOptions().getWarningsHandler();
        }

        private void setHeaders(HttpRequest httpRequest, Collection<Header> requestHeaders) {
            // request headers override default headers, so we don't add default headers if they exist as request headers
            final Set<String> requestNames = new HashSet<>(requestHeaders.size());
            for (Header requestHeader : requestHeaders) {
                httpRequest.addHeader(requestHeader);
                requestNames.add(requestHeader.getName());
            }
            for (Header defaultHeader : defaultHeaders) {
                if (requestNames.contains(defaultHeader.getName()) == false) {
                    httpRequest.addHeader(defaultHeader);
                }
            }
            if (compressionEnabled) {
                httpRequest.addHeader("Accept-Encoding", "gzip");
            }
        }

        private void setRequestConfig(HttpRequestBase httpRequest, RequestConfig requestConfig) {
            if (requestConfig != null) {
                httpRequest.setConfig(requestConfig);
            }
        }

        public Publisher<ByteBuffer> getPublisher() {
            return request.getBody();
        }

        RequestContext<HttpResponse> createContextForNextAttempt(
            Node node,
            AuthCache authCache,
            MonoSink<Message<HttpResponse, Publisher<ByteBuffer>>> emitter
        ) {
            this.httpRequest.reset();
            return new ReactiveRequestContext(this, node, authCache, emitter);
        }
    }

    private class InternalRequest {
        private final Request request;
        private final Set<Integer> ignoreErrorCodes;
        private final HttpRequestBase httpRequest;
        private final Cancellable cancellable;
        private final WarningsHandler warningsHandler;

        InternalRequest(Request request) {
            this.request = request;
            Map<String, String> params = new HashMap<>(request.getParameters());
            // ignore is a special parameter supported by the clients, shouldn't be sent to es
            String ignoreString = params.remove("ignore");
            this.ignoreErrorCodes = getIgnoreErrorCodes(ignoreString, request.getMethod());
            URI uri = buildUri(pathPrefix, request.getEndpoint(), params);
            this.httpRequest = createHttpRequest(request.getMethod(), uri, request.getEntity());
            this.cancellable = Cancellable.fromRequest(httpRequest);
            setHeaders(httpRequest, request.getOptions().getHeaders());
            setRequestConfig(httpRequest, request.getOptions().getRequestConfig());
            this.warningsHandler = request.getOptions().getWarningsHandler() == null
                ? RestClient.this.warningsHandler
                : request.getOptions().getWarningsHandler();
        }

        private void setHeaders(HttpRequest httpRequest, Collection<Header> requestHeaders) {
            // request headers override default headers, so we don't add default headers if they exist as request headers
            final Set<String> requestNames = new HashSet<>(requestHeaders.size());
            for (Header requestHeader : requestHeaders) {
                httpRequest.addHeader(requestHeader);
                requestNames.add(requestHeader.getName());
            }
            for (Header defaultHeader : defaultHeaders) {
                if (requestNames.contains(defaultHeader.getName()) == false) {
                    httpRequest.addHeader(defaultHeader);
                }
            }
            if (compressionEnabled) {
                httpRequest.addHeader("Accept-Encoding", "gzip");
            }
        }

        private void setRequestConfig(HttpRequestBase httpRequest, RequestConfig requestConfig) {
            if (requestConfig != null) {
                httpRequest.setConfig(requestConfig);
            }
        }

        RequestContext<HttpResponse> createContextForNextAttempt(Node node, AuthCache authCache) {
            this.httpRequest.reset();
            return new AsyncRequestContext(this, node, authCache);
        }
    }

    private interface RequestContext<T> {
        Node node();

        HttpAsyncRequestProducer requestProducer();

        HttpAsyncResponseConsumer<T> asyncResponseConsumer();

        HttpClientContext context();
    }

    private static class ReactiveRequestContext implements RequestContext<HttpResponse> {
        private final Node node;
        private final HttpAsyncRequestProducer requestProducer;
        private final HttpAsyncResponseConsumer<HttpResponse> asyncResponseConsumer;
        private final HttpClientContext context;

        ReactiveRequestContext(
            InternalStreamingRequest request,
            Node node,
            AuthCache authCache,
            MonoSink<Message<HttpResponse, Publisher<ByteBuffer>>> emitter
        ) {
            this.node = node;
            // we stream the request body if the entity allows for it
            this.requestProducer = new ReactiveRequestProducer(request.httpRequest, node.getHost(), request.getPublisher());
            this.asyncResponseConsumer = new ReactiveResponseConsumer(new FutureCallback<Message<HttpResponse, Publisher<ByteBuffer>>>() {
                @Override
                public void failed(Exception ex) {
                    emitter.error(ex);
                }

                @Override
                public void completed(Message<HttpResponse, Publisher<ByteBuffer>> result) {
                    if (result == null) {
                        emitter.success();
                    } else {
                        emitter.success(result);
                    }
                }

                @Override
                public void cancelled() {
                    failed(new CancellationException("Future cancelled"));
                }
            });
            this.context = HttpClientContext.create();
            context.setAuthCache(authCache);
        }

        @Override
        public HttpAsyncResponseConsumer<HttpResponse> asyncResponseConsumer() {
            return asyncResponseConsumer;
        }

        @Override
        public HttpClientContext context() {
            return context;
        }

        @Override
        public Node node() {
            return node;
        }

        @Override
        public HttpAsyncRequestProducer requestProducer() {
            return requestProducer;
        }
    }

    private static class AsyncRequestContext implements RequestContext<HttpResponse> {
        private final Node node;
        private final HttpAsyncRequestProducer requestProducer;
        private final HttpAsyncResponseConsumer<HttpResponse> asyncResponseConsumer;
        private final HttpClientContext context;

        AsyncRequestContext(InternalRequest request, Node node, AuthCache authCache) {
            this.node = node;
            // we stream the request body if the entity allows for it
            this.requestProducer = HttpAsyncMethods.create(node.getHost(), request.httpRequest);
            this.asyncResponseConsumer = request.request.getOptions()
                .getHttpAsyncResponseConsumerFactory()
                .createHttpAsyncResponseConsumer();
            this.context = HttpClientContext.create();
            context.setAuthCache(authCache);
        }

        @Override
        public HttpAsyncResponseConsumer<HttpResponse> asyncResponseConsumer() {
            return asyncResponseConsumer;
        }

        @Override
        public HttpClientContext context() {
            return context;
        }

        @Override
        public Node node() {
            return node;
        }

        @Override
        public HttpAsyncRequestProducer requestProducer() {
            return requestProducer;
        }
    }

    private static Set<Integer> getIgnoreErrorCodes(String ignoreString, String requestMethod) {
        Set<Integer> ignoreErrorCodes;
        if (ignoreString == null) {
            if (HttpHead.METHOD_NAME.equals(requestMethod)) {
                // 404 never causes error if returned for a HEAD request
                ignoreErrorCodes = Collections.singleton(404);
            } else {
                ignoreErrorCodes = Collections.emptySet();
            }
        } else {
            String[] ignoresArray = ignoreString.split(",");
            ignoreErrorCodes = new HashSet<>();
            if (HttpHead.METHOD_NAME.equals(requestMethod)) {
                // 404 never causes error if returned for a HEAD request
                ignoreErrorCodes.add(404);
            }
            for (String ignoreCode : ignoresArray) {
                try {
                    ignoreErrorCodes.add(Integer.valueOf(ignoreCode));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("ignore value should be a number, found [" + ignoreString + "] instead", e);
                }
            }
        }
        return ignoreErrorCodes;
    }

    private static class ResponseOrResponseException {
        private final Response response;
        private final ResponseException responseException;

        ResponseOrResponseException(Response response) {
            this.response = Objects.requireNonNull(response);
            this.responseException = null;
        }

        ResponseOrResponseException(ResponseException responseException) {
            this.responseException = Objects.requireNonNull(responseException);
            this.response = null;
        }
    }

    /**
     * Wrap the exception so the caller's signature shows up in the stack trace, taking care to copy the original type and message
     * where possible so async and sync code don't have to check different exceptions.
     */
    private static Exception extractAndWrapCause(Exception exception) {
        if (exception instanceof InterruptedException) {
            throw new RuntimeException("thread waiting for the response was interrupted", exception);
        }
        if (exception instanceof ExecutionException) {
            ExecutionException executionException = (ExecutionException) exception;
            Throwable t = executionException.getCause() == null ? executionException : executionException.getCause();
            if (t instanceof Error) {
                throw (Error) t;
            }
            exception = (Exception) t;
        }
        if (exception instanceof ConnectTimeoutException) {
            ConnectTimeoutException e = new ConnectTimeoutException(exception.getMessage());
            e.initCause(exception);
            return e;
        }
        if (exception instanceof SocketTimeoutException) {
            SocketTimeoutException e = new SocketTimeoutException(exception.getMessage());
            e.initCause(exception);
            return e;
        }
        if (exception instanceof ConnectionClosedException) {
            ConnectionClosedException e = new ConnectionClosedException(exception.getMessage());
            e.initCause(exception);
            return e;
        }
        if (exception instanceof SSLHandshakeException) {
            SSLHandshakeException e = new SSLHandshakeException(
                exception.getMessage() + "\nSee https://opensearch.org/docs/latest/clients/java-rest-high-level/ for troubleshooting."
            );
            e.initCause(exception);
            return e;
        }
        if (exception instanceof ConnectException) {
            ConnectException e = new ConnectException(exception.getMessage());
            e.initCause(exception);
            return e;
        }
        if (exception instanceof IOException) {
            return new IOException(exception.getMessage(), exception);
        }
        if (exception instanceof RuntimeException) {
            return new RuntimeException(exception.getMessage(), exception);
        }
        return new RuntimeException("error while performing request", exception);
    }

    /**
     * A gzip compressing entity that also implements {@code getContent()}.
     */
    public static class ContentCompressingEntity extends GzipCompressingEntity {
        private Optional<Boolean> chunkedEnabled;

        /**
         * Creates a {@link ContentCompressingEntity} instance with the provided HTTP entity.
         *
         * @param entity the HTTP entity.
         */
        public ContentCompressingEntity(HttpEntity entity) {
            super(entity);
            this.chunkedEnabled = Optional.empty();
        }

        /**
         * Creates a {@link ContentCompressingEntity} instance with the provided HTTP entity.
         *
         * @param entity the HTTP entity.
         * @param chunkedEnabled force enable/disable chunked transfer-encoding.
         */
        public ContentCompressingEntity(HttpEntity entity, boolean chunkedEnabled) {
            super(entity);
            this.chunkedEnabled = Optional.of(chunkedEnabled);
        }

        @Override
        public InputStream getContent() throws IOException {
            ByteArrayInputOutputStream out = new ByteArrayInputOutputStream(1024);
            try (GZIPOutputStream gzipOut = new GZIPOutputStream(out)) {
                wrappedEntity.writeTo(gzipOut);
            }
            return out.asInput();
        }

        /**
         * A gzip compressing entity doesn't work with chunked encoding with sigv4
         *
         * @return false
         */
        @Override
        public boolean isChunked() {
            return chunkedEnabled.orElseGet(super::isChunked);
        }

        /**
         * A gzip entity requires content length in http headers.
         *
         * @return content length of gzip entity
         */
        @Override
        public long getContentLength() {
            if (chunkedEnabled.isPresent()) {
                if (chunkedEnabled.get()) {
                    return -1L;
                } else {
                    long size = 0;
                    final byte[] buf = new byte[8192];
                    int nread = 0;

                    try (InputStream is = getContent()) {
                        // read to EOF which may read more or less than buffer size
                        while ((nread = is.read(buf)) > 0) {
                            size += nread;
                        }
                    } catch (IOException ex) {
                        size = -1L;
                    }

                    return size;
                }
            } else {
                return super.getContentLength();
            }
        }
    }

    /**
     * An entity that lets the caller specify the return value of {@code isChunked()}.
     */
    public static class ContentHttpEntity extends HttpEntityWrapper {
        private Optional<Boolean> chunkedEnabled;

        /**
         * Creates a {@link ContentHttpEntity} instance with the provided HTTP entity.
         *
         * @param entity the HTTP entity.
         */
        public ContentHttpEntity(HttpEntity entity) {
            super(entity);
            this.chunkedEnabled = Optional.empty();
        }

        /**
         * Creates a {@link ContentHttpEntity} instance with the provided HTTP entity.
         *
         * @param entity the HTTP entity.
         * @param chunkedEnabled force enable/disable chunked transfer-encoding.
         */
        public ContentHttpEntity(HttpEntity entity, boolean chunkedEnabled) {
            super(entity);
            this.chunkedEnabled = Optional.of(chunkedEnabled);
        }

        /**
         * A chunked entity requires transfer-encoding:chunked in http headers
         * which requires isChunked to be true
         *
         * @return true
         */
        @Override
        public boolean isChunked() {
            return chunkedEnabled.orElseGet(super::isChunked);
        }
    }

    /**
     * A ByteArrayOutputStream that can be turned into an input stream without copying the underlying buffer.
     */
    private static class ByteArrayInputOutputStream extends ByteArrayOutputStream {
        ByteArrayInputOutputStream(int size) {
            super(size);
        }

        public InputStream asInput() {
            return new ByteArrayInputStream(this.buf, 0, this.count);
        }
    }
}
