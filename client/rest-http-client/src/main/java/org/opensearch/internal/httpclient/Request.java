/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.internal.httpclient;

import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableMap;

/**
 * HTTP Request to OpenSearch.
 * Note: This is an experimental API.
 */
public record Request(String method, String endpoint, Map<String, String> parameters, BodyPublisher entity, RequestOptions options) {

    public Request {
        method = Objects.requireNonNull(method, "method cannot be null");
        endpoint = Objects.requireNonNull(endpoint, "endpoint cannot be null");
        parameters = parameters == null ? new HashMap<>() : parameters;
        options = options == null ? RequestOptions.DEFAULT : options;
    }

    /**
     * Query string parameters. The returned map is an unmodifiable view of the
     * map in the request so calls to {@link Map#put(Object, Object)}
     * will change it.
     */
    @Override
    public Map<String, String> parameters() {
        if (options.getParameters().isEmpty()) {
            return unmodifiableMap(parameters);
        } else {
            Map<String, String> combinedParameters = new HashMap<>(parameters);
            combinedParameters.putAll(options.getParameters());
            return unmodifiableMap(combinedParameters);
        }
    }

    public static Request.Builder newRequest(String method, String endpoint) {
        return new Request.Builder(method, endpoint);
    }

    public static final class Builder {
        private final String method;
        private final String endpoint;
        private final Map<String, String> parameters = new HashMap<>();
        private BodyPublisher entity;
        private RequestOptions options = RequestOptions.DEFAULT;

        private Builder(String method, String endpoint) {
            this.method = Objects.requireNonNull(method, "method cannot be null");
            this.endpoint = Objects.requireNonNull(endpoint, "endpoint cannot be null");
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

        /**
         * Set the body of the request. If not set or set to {@code null} then no
         * body is sent with the request.
         *
         * @param entity the {@link BodyPublisher} to be set as the body of the request.
         */
        public Builder withEntity(BodyPublisher entity) {
            this.entity = entity;
            return this;
        }

        /**
         * Set the body of the request to a string. If not set or set to
         * {@code null} then no body is sent with the request. The
         * {@code Content-Type} will be sent as {@code application/json}.
         * If you need a different content type then use
         * {@link #withEntity(BodyPublisher)}.
         *
         * @param entity JSON string to be set as the entity body of the request.
         */
        public Builder withEntity(String entity) {
            withEntity(entity == null ? BodyPublishers.noBody() : BodyPublishers.ofString(entity));
            return this;
        }

        public Request build() {
            return new Request(method, endpoint, parameters, entity, options);
        }
    }
}
