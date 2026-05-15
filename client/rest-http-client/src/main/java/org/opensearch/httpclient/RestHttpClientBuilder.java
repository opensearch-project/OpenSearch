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

package org.opensearch.httpclient;

import org.opensearch.httpclient.internal.Node;
import org.opensearch.httpclient.internal.NodeSelector;

import javax.net.ssl.SSLContext;

import java.net.Authenticator;
import java.net.http.HttpClient;
import java.security.AccessController;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Helps creating a new {@link RestHttpClient}. Allows to set the most common http client configuration options when internally
 * creating the underlying {@link HttpClient}. Also allows to provide an externally created
 * {@link HttpClient} in case additional customization is needed.
 *
 * Note: This is an experimental API.
 */
public final class RestHttpClientBuilder {
    /**
     * The default connection timeout in milliseconds.
     */
    public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 1000;

    /**
     * The default response timeout in milliseconds.
     */
    public static final int DEFAULT_RESPONSE_TIMEOUT_MILLIS = 30000;

    private final List<Node> nodes;
    private Map<String, List<String>> defaultHeaders = Map.of();
    private RestHttpClient.FailureListener failureListener;
    private HttpClientConfigCallback httpClientConfigCallback;
    private String pathPrefix;
    private NodeSelector nodeSelector = NodeSelector.ANY;
    private boolean strictDeprecationMode = false;
    private boolean compressionEnabled = false;

    /**
     * Creates a new builder instance and sets the hosts that the client will send requests to.
     *
     * @throws IllegalArgumentException if {@code nodes} is {@code null} or empty.
     */
    RestHttpClientBuilder(List<Node> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be null or empty");
        }
        for (Node node : nodes) {
            if (node == null) {
                throw new IllegalArgumentException("node cannot be null");
            }
        }
        this.nodes = nodes;
    }

    /**
     * Sets the default request headers, which will be sent along with each request.
     * <p>
     * Request-time headers will always overwrite any default headers.
     *
     * @param defaultHeaders array of default header
     * @throws NullPointerException if {@code defaultHeaders} or any header is {@code null}.
     */
    public RestHttpClientBuilder setDefaultHeaders(Map<String, List<String>> defaultHeaders) {
        Objects.requireNonNull(defaultHeaders, "defaultHeaders must not be null");
        this.defaultHeaders = Collections.unmodifiableMap(defaultHeaders);
        return this;
    }

    /**
     * Sets the {@link RestHttpClient.FailureListener} to be notified for each request failure
     *
     * @param failureListener the {@link RestHttpClient.FailureListener} for each failure
     * @throws NullPointerException if {@code failureListener} is {@code null}.
     */
    public RestHttpClientBuilder setFailureListener(RestHttpClient.FailureListener failureListener) {
        Objects.requireNonNull(failureListener, "failureListener must not be null");
        this.failureListener = failureListener;
        return this;
    }

    /**
     * Sets the {@link HttpClientConfigCallback} to be used to customize http client configuration
     *
     * @param httpClientConfigCallback the {@link HttpClientConfigCallback} to be used
     * @throws NullPointerException if {@code httpClientConfigCallback} is {@code null}.
     */
    public RestHttpClientBuilder setHttpClientConfigCallback(HttpClientConfigCallback httpClientConfigCallback) {
        Objects.requireNonNull(httpClientConfigCallback, "httpClientConfigCallback must not be null");
        this.httpClientConfigCallback = httpClientConfigCallback;
        return this;
    }

    /**
     * Sets the path's prefix for every request used by the http client.
     * <p>
     * For example, if this is set to "/my/path", then any client request will become <code>"/my/path/" + endpoint</code>.
     * <p>
     * In essence, every request's {@code endpoint} is prefixed by this {@code pathPrefix}. The path prefix is useful for when
     * OpenSearch is behind a proxy that provides a base path or a proxy that requires all paths to start with '/';
     * it is not intended for other purposes and it should not be supplied in other scenarios.
     *
     * @param pathPrefix the path prefix for every request.
     * @throws NullPointerException if {@code pathPrefix} is {@code null}.
     * @throws IllegalArgumentException if {@code pathPrefix} is empty, or ends with more than one '/'.
     */
    public RestHttpClientBuilder setPathPrefix(String pathPrefix) {
        this.pathPrefix = cleanPathPrefix(pathPrefix);
        return this;
    }

    /**
     * Cleans up the given path prefix to ensure that looks like "/base/path".
     *
     * @param pathPrefix the path prefix to be cleaned up.
     * @return the cleaned up path prefix.
     * @throws NullPointerException if {@code pathPrefix} is {@code null}.
     * @throws IllegalArgumentException if {@code pathPrefix} is empty, or ends with more than one '/'.
     */
    public static String cleanPathPrefix(String pathPrefix) {
        Objects.requireNonNull(pathPrefix, "pathPrefix must not be null");

        if (pathPrefix.isEmpty()) {
            throw new IllegalArgumentException("pathPrefix must not be empty");
        }

        String cleanPathPrefix = pathPrefix;
        if (cleanPathPrefix.startsWith("/") == false) {
            cleanPathPrefix = "/" + cleanPathPrefix;
        }

        // best effort to ensure that it looks like "/base/path" rather than "/base/path/"
        if (cleanPathPrefix.endsWith("/") && cleanPathPrefix.length() > 1) {
            cleanPathPrefix = cleanPathPrefix.substring(0, cleanPathPrefix.length() - 1);

            if (cleanPathPrefix.endsWith("/")) {
                throw new IllegalArgumentException("pathPrefix is malformed. too many trailing slashes: [" + pathPrefix + "]");
            }
        }
        return cleanPathPrefix;
    }

    /**
     * Sets the {@link NodeSelector} to be used for all requests.
     *
     * @param nodeSelector the {@link NodeSelector} to be used
     * @throws NullPointerException if the provided nodeSelector is null
     */
    public RestHttpClientBuilder setNodeSelector(NodeSelector nodeSelector) {
        Objects.requireNonNull(nodeSelector, "nodeSelector must not be null");
        this.nodeSelector = nodeSelector;
        return this;
    }

    /**
     * Whether the REST client should return any response containing at least
     * one warning header as a failure.
     *
     * @param strictDeprecationMode flag for enabling strict deprecation mode
     */
    public RestHttpClientBuilder setStrictDeprecationMode(boolean strictDeprecationMode) {
        this.strictDeprecationMode = strictDeprecationMode;
        return this;
    }

    /**
     * Whether the REST client should compress requests using gzip content encoding and add the "Accept-Encoding: gzip"
     * header to receive compressed responses.
     *
     * @param compressionEnabled flag for enabling compression
     */
    public RestHttpClientBuilder setCompressionEnabled(boolean compressionEnabled) {
        this.compressionEnabled = compressionEnabled;
        return this;
    }

    /**
     * Creates a new {@link RestHttpClient} based on the provided configuration.
     */
    public RestHttpClient build() {
        if (failureListener == null) {
            failureListener = new RestHttpClient.FailureListener();
        }
        @SuppressWarnings("removal")
        HttpClient httpClient = AccessController.doPrivileged((PrivilegedAction<HttpClient>) this::createHttpClient);

        return new RestHttpClient(
            httpClient,
            defaultHeaders,
            nodes,
            pathPrefix,
            failureListener,
            nodeSelector,
            strictDeprecationMode,
            compressionEnabled
        );
    }

    @SuppressWarnings("removal")
    private HttpClient createHttpClient() {
        try {
            HttpClient.Builder httpClientBuilder = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(DEFAULT_CONNECT_TIMEOUT_MILLIS))
                .sslContext(SSLContext.getDefault());

            if (httpClientConfigCallback != null) {
                httpClientBuilder = httpClientConfigCallback.customizeHttpClient(httpClientBuilder);
            }

            final HttpClient.Builder finalBuilder = httpClientBuilder;
            return AccessController.doPrivileged((PrivilegedAction<HttpClient>) finalBuilder::build);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("could not create the default ssl context", e);
        }
    }

    /**
     * Callback used to customize the {@link HttpClient} instance used by a {@link RestHttpClient} instance.
     */
    public interface HttpClientConfigCallback {
        /**
         * Allows to customize the {@link HttpClient} being created and used by the {@link RestHttpClient}.
         * Commonly used to customize the default {@link Authenticator} for authentication for communication
         * through TLS/SSL without losing any other useful default value that the {@link RestHttpClientBuilder} internally
         * sets, like connection pooling.
         *
         * @param httpClientBuilder the {@link HttpClient.Builder} for customizing the client instance.
         */
        HttpClient.Builder customizeHttpClient(HttpClient.Builder httpClientBuilder);
    }
}
