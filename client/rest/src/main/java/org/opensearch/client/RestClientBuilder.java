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

import org.apache.hc.client5.http.async.HttpAsyncClient;
import org.apache.hc.client5.http.auth.CredentialsProvider;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.DefaultAuthenticationStrategy;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.function.Factory;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.reactor.ssl.TlsDetails;
import org.apache.hc.core5.util.Timeout;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import java.security.AccessController;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Helps creating a new {@link RestClient}. Allows to set the most common http client configuration options when internally
 * creating the underlying {@link HttpAsyncClient}. Also allows to provide an externally created
 * {@link HttpAsyncClient} in case additional customization is needed.
 */
public final class RestClientBuilder {
    /**
     * The default connection timeout in milliseconds.
     */
    public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 1000;

    /**
     * The default response timeout in milliseconds.
     */
    public static final int DEFAULT_RESPONSE_TIMEOUT_MILLIS = 30000;

    /**
     * The default maximum of connections per route.
     */
    public static final int DEFAULT_MAX_CONN_PER_ROUTE = 10;

    /**
     * The default maximum total connections.
     */
    public static final int DEFAULT_MAX_CONN_TOTAL = 30;

    private static final Header[] EMPTY_HEADERS = new Header[0];

    private final List<Node> nodes;
    private Header[] defaultHeaders = EMPTY_HEADERS;
    private RestClient.FailureListener failureListener;
    private HttpClientConfigCallback httpClientConfigCallback;
    private RequestConfigCallback requestConfigCallback;
    private String pathPrefix;
    private NodeSelector nodeSelector = NodeSelector.ANY;
    private boolean strictDeprecationMode = false;
    private boolean compressionEnabled = false;
    private Optional<Boolean> chunkedEnabled;

    /**
     * Creates a new builder instance and sets the hosts that the client will send requests to.
     *
     * @throws IllegalArgumentException if {@code nodes} is {@code null} or empty.
     */
    RestClientBuilder(List<Node> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be null or empty");
        }
        for (Node node : nodes) {
            if (node == null) {
                throw new IllegalArgumentException("node cannot be null");
            }
        }
        this.nodes = nodes;
        this.chunkedEnabled = Optional.empty();
    }

    /**
     * Sets the default request headers, which will be sent along with each request.
     * <p>
     * Request-time headers will always overwrite any default headers.
     *
     * @param defaultHeaders array of default header
     * @throws NullPointerException if {@code defaultHeaders} or any header is {@code null}.
     */
    public RestClientBuilder setDefaultHeaders(Header[] defaultHeaders) {
        Objects.requireNonNull(defaultHeaders, "defaultHeaders must not be null");
        for (Header defaultHeader : defaultHeaders) {
            Objects.requireNonNull(defaultHeader, "default header must not be null");
        }
        this.defaultHeaders = defaultHeaders;
        return this;
    }

    /**
     * Sets the {@link RestClient.FailureListener} to be notified for each request failure
     *
     * @param failureListener the {@link RestClient.FailureListener} for each failure
     * @throws NullPointerException if {@code failureListener} is {@code null}.
     */
    public RestClientBuilder setFailureListener(RestClient.FailureListener failureListener) {
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
    public RestClientBuilder setHttpClientConfigCallback(HttpClientConfigCallback httpClientConfigCallback) {
        Objects.requireNonNull(httpClientConfigCallback, "httpClientConfigCallback must not be null");
        this.httpClientConfigCallback = httpClientConfigCallback;
        return this;
    }

    /**
     * Sets the {@link RequestConfigCallback} to be used to customize http client configuration
     *
     * @param requestConfigCallback the {@link RequestConfigCallback} to be used
     * @throws NullPointerException if {@code requestConfigCallback} is {@code null}.
     */
    public RestClientBuilder setRequestConfigCallback(RequestConfigCallback requestConfigCallback) {
        Objects.requireNonNull(requestConfigCallback, "requestConfigCallback must not be null");
        this.requestConfigCallback = requestConfigCallback;
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
    public RestClientBuilder setPathPrefix(String pathPrefix) {
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
    public RestClientBuilder setNodeSelector(NodeSelector nodeSelector) {
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
    public RestClientBuilder setStrictDeprecationMode(boolean strictDeprecationMode) {
        this.strictDeprecationMode = strictDeprecationMode;
        return this;
    }

    /**
     * Whether the REST client should compress requests using gzip content encoding and add the "Accept-Encoding: gzip"
     * header to receive compressed responses.
     *
     * @param compressionEnabled flag for enabling compression
     */
    public RestClientBuilder setCompressionEnabled(boolean compressionEnabled) {
        this.compressionEnabled = compressionEnabled;
        return this;
    }

    /**
     * Whether the REST client should use Transfer-Encoding: chunked for requests or not"
     *
     * @param chunkedEnabled force enable/disable chunked transfer-encoding.
     */
    public RestClientBuilder setChunkedEnabled(boolean chunkedEnabled) {
        this.chunkedEnabled = Optional.of(chunkedEnabled);
        return this;
    }

    /**
     * Creates a new {@link RestClient} based on the provided configuration.
     */
    public RestClient build() {
        if (failureListener == null) {
            failureListener = new RestClient.FailureListener();
        }
        CloseableHttpAsyncClient httpClient = AccessController.doPrivileged(
            (PrivilegedAction<CloseableHttpAsyncClient>) this::createHttpClient
        );

        RestClient restClient = null;

        if (chunkedEnabled.isPresent()) {
            restClient = new RestClient(
                httpClient,
                defaultHeaders,
                nodes,
                pathPrefix,
                failureListener,
                nodeSelector,
                strictDeprecationMode,
                compressionEnabled,
                chunkedEnabled.get()
            );
        } else {
            restClient = new RestClient(
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

        httpClient.start();
        return restClient;
    }

    private CloseableHttpAsyncClient createHttpClient() {
        // default timeouts are all infinite
        RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
            .setConnectTimeout(Timeout.ofMilliseconds(DEFAULT_CONNECT_TIMEOUT_MILLIS))
            .setResponseTimeout(Timeout.ofMilliseconds(DEFAULT_RESPONSE_TIMEOUT_MILLIS));
        if (requestConfigCallback != null) {
            requestConfigBuilder = requestConfigCallback.customizeRequestConfig(requestConfigBuilder);
        }

        try {
            final TlsStrategy tlsStrategy = ClientTlsStrategyBuilder.create()
                .setSslContext(SSLContext.getDefault())
                // See https://issues.apache.org/jira/browse/HTTPCLIENT-2219
                .setTlsDetailsFactory(new Factory<SSLEngine, TlsDetails>() {
                    @Override
                    public TlsDetails create(final SSLEngine sslEngine) {
                        return new TlsDetails(sslEngine.getSession(), sslEngine.getApplicationProtocol());
                    }
                })
                .build();

            final PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
                .setMaxConnPerRoute(DEFAULT_MAX_CONN_PER_ROUTE)
                .setMaxConnTotal(DEFAULT_MAX_CONN_TOTAL)
                .setTlsStrategy(tlsStrategy)
                .build();

            HttpAsyncClientBuilder httpClientBuilder = HttpAsyncClientBuilder.create()
                .setDefaultRequestConfig(requestConfigBuilder.build())
                .setConnectionManager(connectionManager)
                .setTargetAuthenticationStrategy(DefaultAuthenticationStrategy.INSTANCE)
                .disableAutomaticRetries();
            if (httpClientConfigCallback != null) {
                httpClientBuilder = httpClientConfigCallback.customizeHttpClient(httpClientBuilder);
            }

            final HttpAsyncClientBuilder finalBuilder = httpClientBuilder;
            return AccessController.doPrivileged((PrivilegedAction<CloseableHttpAsyncClient>) finalBuilder::build);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("could not create the default ssl context", e);
        }
    }

    /**
     * Callback used the default {@link RequestConfig} being set to the {@link CloseableHttpClient}
     * @see HttpClientBuilder#setDefaultRequestConfig
     */
    public interface RequestConfigCallback {
        /**
         * Allows to customize the {@link RequestConfig} that will be used with each request.
         * It is common to customize the different timeout values through this method without losing any other useful default
         * value that the {@link RestClientBuilder} internally sets.
         *
         * @param requestConfigBuilder the {@link RestClientBuilder} for customizing the request configuration.
         */
        RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder);
    }

    /**
     * Callback used to customize the {@link CloseableHttpClient} instance used by a {@link RestClient} instance.
     * Allows to customize default {@link RequestConfig} being set to the client and any parameter that
     * can be set through {@link HttpClientBuilder}
     */
    public interface HttpClientConfigCallback {
        /**
         * Allows to customize the {@link CloseableHttpAsyncClient} being created and used by the {@link RestClient}.
         * Commonly used to customize the default {@link CredentialsProvider} for authentication for communication
         * through TLS/SSL without losing any other useful default value that the {@link RestClientBuilder} internally
         * sets, like connection pooling.
         *
         * @param httpClientBuilder the {@link HttpClientBuilder} for customizing the client instance.
         */
        HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder);
    }
}
