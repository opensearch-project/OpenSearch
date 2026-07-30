/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.internal.httpclient;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.reactivestreams.Publisher;

import static java.util.Collections.unmodifiableMap;

/**
 * HTTP Streaming Request to OpenSearch.
 * Note: This is an experimental API.
 */
public final class StreamingRequest {
    private final String method;
    private final String endpoint;
    private final Map<String, String> parameters = new HashMap<>();
    private final RequestOptions options;
    private final Publisher<ByteBuffer> publisher;

    /**
     * Constructor
     * @param method method
     * @param endpoint endpoint
     * @param publisher publisher
     */
    private StreamingRequest(String method, String endpoint, Publisher<ByteBuffer> publisher) {
        this(method, endpoint, Map.of(), publisher, RequestOptions.DEFAULT);
    }

    /**
     * Constructor
     * @param method method
     * @param endpoint endpoint
     * @param builder request options builder
     * @param publisher publisher
     */
    private StreamingRequest(
        String method,
        String endpoint,
        Map<String, String> parameters,
        Publisher<ByteBuffer> publisher,
        RequestOptions.Builder builder
    ) {
        this(method, endpoint, parameters, publisher, builder.build());
    }

    /**
     * Constructor
     * @param method method
     * @param endpoint endpoint
     * @param options request options
     * @param publisher publisher
     */
    private StreamingRequest(
        String method,
        String endpoint,
        Map<String, String> parameters,
        Publisher<ByteBuffer> publisher,
        RequestOptions options
    ) {
        this.method = method;
        this.endpoint = endpoint;
        this.publisher = publisher;
        this.options = options;
    }

    public static StreamingRequest.Builder newRequest(String method, String endpoint, Publisher<ByteBuffer> publisher) {
        return new StreamingRequest.Builder(method, endpoint, publisher);
    }

    /**
     * Get endpoint
     * @return endpoint
     */
    public String endpoint() {
        return endpoint;
    }

    /**
     * Get method
     * @return method
     */
    public String method() {
        return method;
    }

    /**
     * Get options
     * @return options
     */
    public RequestOptions options() {
        return options;
    }

    /**
     * Get parameters
     * @return parameters
     */
    public Map<String, String> parameters() {
        if (options.getParameters().isEmpty()) {
            return unmodifiableMap(parameters);
        } else {
            Map<String, String> combinedParameters = new HashMap<>(parameters);
            combinedParameters.putAll(options.getParameters());
            return unmodifiableMap(combinedParameters);
        }
    }

    /**
     * Body publisher
     * @return body publisher
     */
    public Publisher<ByteBuffer> body() {
        return publisher;
    }

    public static final class Builder {
        private final String method;
        private final String endpoint;
        private final Map<String, String> parameters = new HashMap<>();
        private final Publisher<ByteBuffer> publisher;
        private RequestOptions options = RequestOptions.DEFAULT;

        private Builder(String method, String endpoint, Publisher<ByteBuffer> publisher) {
            this.method = Objects.requireNonNull(method, "method cannot be null");
            this.endpoint = Objects.requireNonNull(endpoint, "endpoint cannot be null");
            this.publisher = Objects.requireNonNull(publisher, "publisher cannot be null");
        }

        /**
         * Set the portion of an HTTP request to OpenSearch that can be
         * manipulated without changing OpenSearch's behavior.
         *
         * @param options the options to be set.
         * @throws NullPointerException if {@code options} is null.
         */
        public void setOptions(RequestOptions options) {
            Objects.requireNonNull(options, "options cannot be null");
            this.options = options;
        }

        /**
         * Add a query string parameter.
         * @param name the name of the url parameter. Must not be null.
         * @param value the value of the url url parameter. If {@code null} then
         *      the parameter is sent as {@code name} rather than {@code name=value}
         * @throws IllegalArgumentException if a parameter with that name has
         *      already been set
         */
        public Builder withParameter(String name, String value) {
            Objects.requireNonNull(name, "url parameter name cannot be null");
            if (parameters.containsKey(name)) {
                throw new IllegalArgumentException("url parameter [" + name + "] has already been set to [" + parameters.get(name) + "]");
            } else {
                parameters.put(name, value);
            }
            return this;
        }

        /**
         * Add query parameters using the provided map of key value pairs.
         *
         * @param paramSource a map of key value pairs where the key is the url parameter.
         * @throws IllegalArgumentException if a parameter with that name has already been set.
         */
        public Builder withParameters(Map<String, String> paramSource) {
            paramSource.forEach(this::withParameter);
            return this;
        }

        /**
         * Set the portion of an HTTP request to OpenSearch that can be
         * manipulated without changing OpenSearch's behavior.
         *
         * @param options the options to be set.
         * @throws NullPointerException if {@code options} is null.
         */
        public Builder withOptions(RequestOptions.Builder options) {
            Objects.requireNonNull(options, "options cannot be null");
            this.options = options.build();
            return this;
        }

        /**
         * Set the portion of an HTTP request to OpenSearch that can be
         * manipulated without changing OpenSearch's behavior.
         *
         * @param options the options to be set.
         * @throws NullPointerException if {@code options} is null.
         */
        public Builder withOptions(RequestOptions options) {
            Objects.requireNonNull(options, "options cannot be null");
            this.options = options;
            return this;
        }

        public StreamingRequest build() {
            return new StreamingRequest(method, endpoint, parameters, publisher, options);
        }
    }
}
