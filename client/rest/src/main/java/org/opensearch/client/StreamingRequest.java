/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.reactivestreams.Publisher;

import static java.util.Collections.unmodifiableMap;

/**
 * HTTP Streaming Request to OpenSearch. <strong>This is an experimental API.</strong>
 */
public class StreamingRequest<T> {
    private final String method;
    private final String endpoint;
    private final Map<String, String> parameters = new HashMap<>();

    private RequestOptions options = RequestOptions.DEFAULT;
    private final Publisher<T> publisher;

    /**
     * Constructor
     * @param method method
     * @param endpoint endpoint
     * @param publisher publisher
     */
    public StreamingRequest(String method, String endpoint, Publisher<T> publisher) {
        this.method = method;
        this.endpoint = endpoint;
        this.publisher = publisher;
    }

    /**
     * Get endpoint
     * @return endpoint
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * Get method
     * @return method
     */
    public String getMethod() {
        return method;
    }

    /**
     * Get options
     * @return options
     */
    public RequestOptions getOptions() {
        return options;
    }

    /**
     * Get parameters
     * @return parameters
     */
    public Map<String, String> getParameters() {
        if (options.getParameters().isEmpty()) {
            return unmodifiableMap(parameters);
        } else {
            Map<String, String> combinedParameters = new HashMap<>(parameters);
            combinedParameters.putAll(options.getParameters());
            return unmodifiableMap(combinedParameters);
        }
    }

    /**
     * Add a query string parameter.
     * @param name the name of the url parameter. Must not be null.
     * @param value the value of the url url parameter. If {@code null} then
     *      the parameter is sent as {@code name} rather than {@code name=value}
     * @throws IllegalArgumentException if a parameter with that name has
     *      already been set
     */
    public void addParameter(String name, String value) {
        Objects.requireNonNull(name, "url parameter name cannot be null");
        if (parameters.containsKey(name)) {
            throw new IllegalArgumentException("url parameter [" + name + "] has already been set to [" + parameters.get(name) + "]");
        } else {
            parameters.put(name, value);
        }
    }

    /**
     * Add query parameters using the provided map of key value pairs.
     *
     * @param paramSource a map of key value pairs where the key is the url parameter.
     * @throws IllegalArgumentException if a parameter with that name has already been set.
     */
    public void addParameters(Map<String, String> paramSource) {
        paramSource.forEach(this::addParameter);
    }

    /**
     * Body publisher
     * @return body publisher
     */
    public Publisher<T> getBody() {
        return publisher;
    }
}
