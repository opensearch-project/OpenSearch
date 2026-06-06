/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.internal.httpclient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.ssl.SSLHandshakeException;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.reactivestreams.Publisher;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;

/**
 * Client that connects to an OpenSearch cluster through HTTP.
 * <p>
 * Must be created using {@link RestHttpClientBuilder}, which allows to set all the different options or just rely on defaults.
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
 *
 * Note: This is an experimental API.
 */
public class RestHttpClient implements Closeable {

    private static final Log logger = LogFactory.getLog(RestHttpClient.class);

    private final HttpClient client;
    final Map<String, List<String>> defaultHeaders;
    private final String pathPrefix;
    private final AtomicInteger lastNodeIndex = new AtomicInteger(0);
    private final ConcurrentMap<HttpHost, DeadHostState> denylist = new ConcurrentHashMap<>();
    private final FailureListener failureListener;
    private final NodeSelector nodeSelector;
    private volatile List<Node> nodes;
    private final WarningsHandler warningsHandler;
    private final boolean compressionEnabled;

    RestHttpClient(
        HttpClient client,
        Map<String, List<String>> defaultHeaders,
        List<Node> nodes,
        String pathPrefix,
        FailureListener failureListener,
        NodeSelector nodeSelector,
        boolean strictDeprecationMode,
        boolean compressionEnabled
    ) {
        this.client = client;
        this.defaultHeaders = Collections.unmodifiableMap(defaultHeaders);
        this.failureListener = failureListener;
        this.pathPrefix = pathPrefix;
        this.nodeSelector = nodeSelector;
        this.warningsHandler = strictDeprecationMode ? WarningsHandler.STRICT : WarningsHandler.PERMISSIVE;
        this.compressionEnabled = compressionEnabled;
        setNodes(nodes);
    }

    /**
     * Returns a new {@link RestHttpClientBuilder} to help with {@link RestHttpClient} creation.
     * Creates a new builder instance and sets the nodes that the client will send requests to.
     *
     * @param cloudId a valid elastic cloud cloudId that will route to a cluster. The cloudId is located in
     *                the user console https://cloud.elastic.co and will resemble a string like the following
     *                optionalHumanReadableName:dXMtZWFzdC0xLmF3cy5mb3VuZC5pbyRlbGFzdGljc2VhcmNoJGtpYmFuYQ==
     */
    public static RestHttpClientBuilder builder(String cloudId) {
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
        return builder(new HttpHost("https", url, port));
    }

    /**
     * Returns a new {@link RestHttpClientBuilder} to help with {@link RestHttpClient} creation.
     * Creates a new builder instance and sets the hosts that the client will send requests to.
     * <p>
     * Prefer this to {@link #builder(HttpHost...)} if you have metadata up front about the nodes.
     * If you don't either one is fine.
     *
     * @param nodes The nodes that the client will send requests to.
     */
    public static RestHttpClientBuilder builder(Node... nodes) {
        return new RestHttpClientBuilder(nodes == null ? null : Arrays.asList(nodes));
    }

    /**
     * Returns a new {@link RestHttpClientBuilder} to help with {@link RestHttpClient} creation.
     * Creates a new builder instance and sets the nodes that the client will send requests to.
     * <p>
     * You can use this if you do not have metadata up front about the nodes. If you do, prefer
     * {@link #builder(Node...)}.
     * @see Node#Node(HttpHost)
     *
     * @param hosts The hosts that the client will send requests to.
     */
    public static RestHttpClientBuilder builder(HttpHost... hosts) {
        if (hosts == null || hosts.length == 0) {
            throw new IllegalArgumentException("hosts must not be null nor empty");
        }
        List<Node> nodes = Arrays.stream(hosts).map(Node::new).collect(Collectors.toList());
        return new RestHttpClientBuilder(nodes);
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

        Map<HttpHost, Node> nodesByHost = new LinkedHashMap<>();
        for (Node node : nodes) {
            Objects.requireNonNull(node, "node cannot be null");
            // TODO should we throw an IAE if we have two nodes with the same host?
            nodesByHost.put(node.getHost(), node);
        }
        this.nodes = Collections.unmodifiableList(new ArrayList<>(nodesByHost.values()));
        this.denylist.clear();
    }

    /**
     * Get the list of nodes that the client knows about. The list is
     * unmodifiable.
     */
    public List<Node> getNodes() {
        return nodes;
    }

    /**
     * check client running status
     * @return client running status
     */
    public boolean isRunning() {
        return client.isTerminated() == false;
    }

    /**
     * Sends a streaming request to the OpenSearch cluster that the client points to and returns streaming response. <strong>This is an experimental API</strong>.
     * @param request streaming request
     * @return streaming response
     * @throws IOException IOException
     */
    public StreamingResponse streamRequest(StreamingRequest request) throws IOException {
        final InternalStreamingRequest internalRequest = new InternalStreamingRequest(request);
        return streamRequest(nextNodes(), internalRequest);
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
     * @throws ResponseException in case OpenSearch responded with a status code that indicated an error
     */
    public Response performRequest(Request request) throws IOException {
        InternalRequest internalRequest = new InternalRequest(request);
        return performRequest(nextNodes(), internalRequest, null);
    }

    private Response performRequest(final Iterator<Node> nodes, final InternalRequest request, Exception previousException)
        throws IOException {
        Node node = nodes.next();
        RequestContext<List<ByteBuffer>> context = request.createContextForNextAttempt(node);
        HttpResponse<List<ByteBuffer>> httpResponse;
        try {
            httpResponse = client.send(context.requestProducer(), context.asyncResponseConsumer());
        } catch (Exception e) {
            RequestLogger.logFailedRequest(logger, request.httpRequest, context.node(), e);
            onFailure(context.node());
            Exception cause = extractAndWrapCause(e);
            addSuppressedException(previousException, cause);
            if (nodes.hasNext()) {
                return performRequest(nodes, request, cause);
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
        if (nodes.hasNext()) {
            return performRequest(nodes, request, responseOrResponseException.responseException);
        }
        throw responseOrResponseException.responseException;
    }

    private Publisher<HttpResponse<Flow.Publisher<List<ByteBuffer>>>> streamRequest(
        final Node node,
        final Iterator<Node> nodes,
        final InternalStreamingRequest request
    ) throws IOException {
        return request.cancellable.callIfNotCancelled(() -> {
            final RequestContext<Flow.Publisher<List<ByteBuffer>>> context = request.createContextForNextAttempt(node);
            final CompletableFuture<HttpResponse<Flow.Publisher<List<ByteBuffer>>>> future = client.sendAsync(
                context.requestProducer(),
                context.asyncResponseConsumer()
            );
            request.setCancellable(future);

            final Mono<HttpResponse<Flow.Publisher<List<ByteBuffer>>>> publisher = Mono.fromCompletionStage(future).flatMap(response -> {
                try {
                    final ResponseOrResponseException responseOrResponseException = convertResponse(request, node, response);
                    if (responseOrResponseException.responseException == null) {
                        return Mono.just(response);
                    } else {
                        if (nodes.hasNext()) {
                            return Mono.from(streamRequest(nodes.next(), nodes, request));
                        } else {
                            return Mono.error(responseOrResponseException.responseException);
                        }
                    }
                } catch (final Exception ex) {
                    return Mono.error(ex);
                }
            });

            return publisher;
        });
    }

    private StreamingResponse streamRequest(final Iterator<Node> nodes, final InternalStreamingRequest request) throws IOException {
        return request.cancellable.callIfNotCancelled(() -> {
            final Node node = nodes.next();
            return new StreamingResponse(new RequestLine(request.httpRequest.apply(node)), streamRequest(node, nodes, request));
        });
    }

    private ResponseOrResponseException convertResponse(InternalRequest request, Node node, HttpResponse<List<ByteBuffer>> httpResponse)
        throws IOException {

        final HttpRequest httpRequest = request.httpRequest.apply(node);
        RequestLogger.logResponse(logger, httpRequest, node.getHost(), httpResponse);
        int statusCode = httpResponse.statusCode();

        Response response = Response.from(new RequestLine(httpRequest), node.getHost(), httpResponse);
        if (isSuccessfulResponse(statusCode) || request.ignoreErrorCodes.contains(response.statusLine().statusCode())) {
            onResponse(node);
            if (request.warningsHandler.warningsShouldFailRequest(response.warnings())) {
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
        HttpResponse<Flow.Publisher<List<ByteBuffer>>> httpResponse
    ) throws IOException {

        // Streaming Response could accumulate a lot of data so we may not be able to fully consume it.
        final HttpRequest httpRequest = request.httpRequest.apply(node);
        final Response response = Response.fromStreaming(new RequestLine(httpRequest), node.getHost(), httpResponse);

        RequestLogger.logStreamingResponse(logger, request.httpRequest.apply(node), node.getHost(), httpResponse);
        int statusCode = httpResponse.statusCode();

        if (isSuccessfulResponse(statusCode) || request.ignoreErrorCodes.contains(response.statusLine().statusCode())) {
            onResponse(node);
            if (request.warningsHandler.warningsShouldFailRequest(response.warnings())) {
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
        final Iterator<Node> nodes,
        final InternalRequest request,
        final FailureTrackingResponseListener listener
    ) {
        request.cancellable.runIfNotCancelled(() -> {
            final RequestContext<List<ByteBuffer>> context = request.createContextForNextAttempt(nodes.next());
            CompletableFuture<HttpResponse<List<ByteBuffer>>> future = client.sendAsync(
                context.requestProducer(),
                context.asyncResponseConsumer()
            );

            request.setCancellable(future);
            future.whenComplete((httpResponse, throwable) -> {
                if (httpResponse != null) {
                    try {
                        ResponseOrResponseException responseOrResponseException = convertResponse(request, context.node(), httpResponse);
                        if (responseOrResponseException.responseException == null) {
                            listener.onSuccess(responseOrResponseException.response);
                        } else {
                            if (nodes.hasNext()) {
                                listener.trackFailure(responseOrResponseException.responseException);
                                performRequestAsync(nodes, request, listener);
                            } else {
                                listener.onDefinitiveFailure(responseOrResponseException.responseException);
                            }
                        }
                    } catch (Exception e) {
                        listener.onDefinitiveFailure(e);
                    }
                } else if (throwable instanceof Exception failure) {
                    if (failure instanceof CancellationException) {
                        listener.onDefinitiveFailure(Cancellable.newCancellationException());
                    } else {
                        Exception cause = failure;
                        if (failure instanceof CompletionException && failure.getCause() instanceof Exception ce) {
                            cause = ce;
                        }
                        try {
                            RequestLogger.logFailedRequest(logger, request.httpRequest, context.node(), failure);
                            onFailure(context.node());
                            if (nodes.hasNext()) {
                                listener.trackFailure(failure);
                                performRequestAsync(nodes, request, listener);
                            } else {
                                listener.onDefinitiveFailure(cause);
                            }
                        } catch (Exception e) {
                            listener.onDefinitiveFailure(e);
                        }
                    }
                }
            });
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
    private Iterator<Node> nextNodes() throws IOException {
        List<Node> nodes = this.nodes;
        Iterable<Node> hosts = selectNodes(nodes, denylist, lastNodeIndex, nodeSelector);
        return hosts.iterator();
    }

    /**
     * Select nodes to try and sorts them so that the first one will be tried initially, then the following ones
     * if the previous attempt failed and so on. Package private for testing.
     */
    static Iterable<Node> selectNodes(
        List<Node> nodes,
        Map<HttpHost, DeadHostState> denylist,
        AtomicInteger lastNodeIndex,
        NodeSelector nodeSelector
    ) throws IOException {
        /*
         * Sort the nodes into living and dead lists.
         */
        List<Node> livingNodes = new ArrayList<>(Math.max(0, nodes.size() - denylist.size()));
        List<DeadNode> deadNodes = new ArrayList<>(denylist.size());
        for (Node node : nodes) {
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

    /**
     * Close the underlying {@link HttpClient} instance
     */
    @Override
    public void close() throws IOException {
        client.shutdownNow();
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

    private HttpRequest.Builder createHttpRequest(
        Node node,
        String method,
        URI uri,
        BodyPublisher body,
        Duration timeout,
        boolean compressed
    ) {
        return HttpRequest.newBuilder()
            .uri(URI.create(node.getHost().toString()).resolve(uri))
            .timeout(timeout)
            .method(
                method,
                (body == null) ? BodyPublishers.noBody()
                    : compressed == false ? body
                    : BodyPublishers.fromPublisher(
                        JdkFlowAdapter.publisherToFlowPublisher(
                            JdkFlowAdapter.flowPublisherToFlux(body).buffer().map(BodyUtils::compress).flatMap(Flux::fromIterable)
                        )
                    )
            );
    }

    private HttpRequest.Builder createStreamingHttpRequest(
        Node node,
        String method,
        URI uri,
        Publisher<ByteBuffer> body,
        Duration timeout,
        boolean compressed
    ) {
        return HttpRequest.newBuilder()
            .uri(URI.create(node.getHost().toString()).resolve(uri))
            .timeout(timeout)
            .method(
                method,
                (body == null) ? BodyPublishers.noBody()
                    : compressed == false ? BodyPublishers.fromPublisher(JdkFlowAdapter.publisherToFlowPublisher(body))
                    : BodyPublishers.fromPublisher(JdkFlowAdapter.publisherToFlowPublisher(Flux.from(body).map(BodyUtils::compress)))
            );
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

            final String additionalQuery = params.entrySet()
                .stream()
                .filter(e -> e.getValue() != null)
                .map(e -> e.getKey() + "=" + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&"));
            final URI uri = URI.create(fullPath);

            String newQuery = uri.getQuery();
            if (newQuery == null) {
                newQuery = additionalQuery;
            } else {
                newQuery += "&" + additionalQuery;
            }

            return new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), newQuery, uri.getFragment());
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
        private final Set<Integer> ignoreErrorCodes;
        private final Function<Node, HttpRequest> httpRequest;
        private final WarningsHandler warningsHandler;
        private volatile Cancellable cancellable = Cancellable.fromFuture(new CompletableFuture<>());

        InternalStreamingRequest(StreamingRequest request) {
            Map<String, String> params = new HashMap<>(request.parameters());
            // ignore is a special parameter supported by the clients, shouldn't be sent to es
            String ignoreString = params.remove("ignore");
            this.ignoreErrorCodes = getIgnoreErrorCodes(ignoreString, request.method());

            this.httpRequest = node -> {
                URI uri = buildUri(pathPrefix, request.endpoint(), params);
                HttpRequest.Builder builder = createStreamingHttpRequest(
                    node,
                    request.method(),
                    uri,
                    request.body(),
                    request.options().getTimeout(),
                    compressionEnabled
                );
                setHeaders(builder, request.options().getHeaders());
                return builder.build();
            };

            this.warningsHandler = request.options().getWarningsHandler() == null
                ? RestHttpClient.this.warningsHandler
                : request.options().getWarningsHandler();
        }

        private void setHeaders(HttpRequest.Builder httpRequest, Map<String, List<String>> requestHeaders) {
            // request headers override default headers, so we don't add default headers if they exist as request headers
            final Set<String> requestNames = new HashSet<>(requestHeaders.size());
            for (Map.Entry<String, List<String>> requestHeader : requestHeaders.entrySet()) {
                requestHeader.getValue().forEach(v -> httpRequest.header(requestHeader.getKey(), v));
                requestNames.add(requestHeader.getKey());
            }
            for (Map.Entry<String, List<String>> defaultHeader : defaultHeaders.entrySet()) {
                if (requestNames.contains(defaultHeader.getKey()) == false) {
                    defaultHeader.getValue().forEach(v -> httpRequest.header(defaultHeader.getKey(), v));
                }
            }
            if (compressionEnabled) {
                httpRequest.header("Content-Encoding", "gzip");
                httpRequest.header("Accept-Encoding", "gzip");
            }
        }

        private void setCancellable(Future<?> f) {
            cancellable = Cancellable.fromFuture(f);
        }

        RequestContext<Flow.Publisher<List<ByteBuffer>>> createContextForNextAttempt(Node node) {
            return new ReactiveRequestContext(this, node);
        }
    }

    private class InternalRequest {
        private final Set<Integer> ignoreErrorCodes;
        private final Function<Node, HttpRequest> httpRequest;
        private final WarningsHandler warningsHandler;
        private volatile Cancellable cancellable = Cancellable.fromFuture(new CompletableFuture<>());

        InternalRequest(Request request) {
            Map<String, String> params = new HashMap<>(request.parameters());
            // ignore is a special parameter supported by the clients, shouldn't be sent to es
            String ignoreString = params.remove("ignore");
            this.ignoreErrorCodes = getIgnoreErrorCodes(ignoreString, request.method());
            this.httpRequest = node -> {
                URI uri = buildUri(pathPrefix, request.endpoint(), params);
                final HttpRequest.Builder builder = createHttpRequest(
                    node,
                    request.method(),
                    uri,
                    request.entity(),
                    request.options().getTimeout(),
                    compressionEnabled
                );
                setHeaders(builder, request.options().getHeaders());
                return builder.build();
            };
            this.warningsHandler = request.options().getWarningsHandler() == null
                ? RestHttpClient.this.warningsHandler
                : request.options().getWarningsHandler();
        }

        private void setCancellable(Future<?> f) {
            cancellable = Cancellable.fromFuture(f);
        }

        private void setHeaders(HttpRequest.Builder httpRequest, Map<String, List<String>> requestHeaders) {
            // request headers override default headers, so we don't add default headers if they exist as request headers
            final Set<String> requestNames = new HashSet<>(requestHeaders.size());

            for (Map.Entry<String, List<String>> requestHeader : requestHeaders.entrySet()) {
                requestHeader.getValue().forEach(v -> httpRequest.header(requestHeader.getKey(), v));
                requestNames.add(requestHeader.getKey());
            }
            for (Map.Entry<String, List<String>> defaultHeader : defaultHeaders.entrySet()) {
                if (requestNames.contains(defaultHeader.getKey()) == false) {
                    defaultHeader.getValue().forEach(v -> httpRequest.header(defaultHeader.getKey(), v));
                }
            }
            if (compressionEnabled) {
                httpRequest.header("Content-Encoding", "gzip");
                httpRequest.header("Accept-Encoding", "gzip");
            }
        }

        RequestContext<List<ByteBuffer>> createContextForNextAttempt(Node node) {
            return new AsyncRequestContext(this, node);
        }
    }

    private interface RequestContext<T> {
        Node node();

        HttpRequest requestProducer();

        BodyHandler<T> asyncResponseConsumer();
    }

    private static class ReactiveRequestContext implements RequestContext<Flow.Publisher<List<ByteBuffer>>> {
        private final Node node;
        private final HttpRequest requestProducer;
        private final BodyHandler<Flow.Publisher<List<ByteBuffer>>> asyncResponseConsumer;

        ReactiveRequestContext(InternalStreamingRequest request, Node node) {
            this.node = node;
            // we stream the request body if the entity allows for it
            this.requestProducer = request.httpRequest.apply(node);
            this.asyncResponseConsumer = BodyHandlers.ofPublisher();
        }

        @Override
        public BodyHandler<Flow.Publisher<List<ByteBuffer>>> asyncResponseConsumer() {
            return asyncResponseConsumer;
        }

        @Override
        public Node node() {
            return node;
        }

        @Override
        public HttpRequest requestProducer() {
            return requestProducer;
        }
    }

    private static class AsyncRequestContext implements RequestContext<List<ByteBuffer>> {
        private final Node node;
        private final HttpRequest requestProducer;
        private final BodyHandler<List<ByteBuffer>> asyncResponseConsumer;

        AsyncRequestContext(InternalRequest request, Node node) {
            this.node = node;
            this.requestProducer = request.httpRequest.apply(node);
            this.asyncResponseConsumer = BodyHandlers.fromSubscriber(new AsyncResponseProducer(), AsyncResponseProducer::getResult);
        }

        @Override
        public BodyHandler<List<ByteBuffer>> asyncResponseConsumer() {
            return asyncResponseConsumer;
        }

        @Override
        public Node node() {
            return node;
        }

        @Override
        public HttpRequest requestProducer() {
            return requestProducer;
        }
    }

    private static Set<Integer> getIgnoreErrorCodes(String ignoreString, String requestMethod) {
        Set<Integer> ignoreErrorCodes;
        if (ignoreString == null) {
            if ("HEAD".equalsIgnoreCase(requestMethod)) {
                // 404 never causes error if returned for a HEAD request
                ignoreErrorCodes = Collections.singleton(404);
            } else {
                ignoreErrorCodes = Collections.emptySet();
            }
        } else {
            String[] ignoresArray = ignoreString.split(",");
            ignoreErrorCodes = new HashSet<>();
            if ("HEAD".equalsIgnoreCase(requestMethod)) {
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
        if (exception instanceof ExecutionException || exception instanceof CompletionException) {
            Throwable t = exception.getCause() == null ? exception : exception.getCause();
            if (t instanceof Error) {
                throw (Error) t;
            }
            exception = (Exception) t;
        }
        if (exception instanceof HttpTimeoutException) {
            HttpTimeoutException e = new HttpTimeoutException(exception.getMessage());
            e.initCause(exception);
            return e;
        }
        if (exception instanceof ClosedChannelException) {
            ClosedChannelException e = new ClosedChannelException();
            e.initCause(exception);
            return e;
        }
        if (exception instanceof SocketTimeoutException) {
            SocketTimeoutException e = new SocketTimeoutException(exception.getMessage());
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
}
