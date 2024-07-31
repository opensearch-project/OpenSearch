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

import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableMap;

/**
 * HTTP Request to OpenSearch.
 */
public final class Request {
    private final String method;
    private final String endpoint;
    private final Map<String, String> parameters = new HashMap<>();

    private HttpEntity entity;
    private RequestOptions options = RequestOptions.DEFAULT;

    /**
     * Create the {@linkplain Request}.
     * @param method the HTTP method
     * @param endpoint the path of the request (without scheme, host, port, or prefix)
     */
    public Request(String method, String endpoint) {
        this.method = Objects.requireNonNull(method, "method cannot be null");
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint cannot be null");
    }

    /**
     * The HTTP method.
     */
    public String getMethod() {
        return method;
    }

    /**
     * The path of the request (without scheme, host, port, or prefix).
     */
    public String getEndpoint() {
        return endpoint;
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
     * Query string parameters. The returned map is an unmodifiable view of the
     * map in the request so calls to {@link #addParameter(String, String)}
     * will change it.
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
     * Set the body of the request. If not set or set to {@code null} then no
     * body is sent with the request.
     *
     * @param entity the {@link HttpEntity} to be set as the body of the request.
     */
    public void setEntity(HttpEntity entity) {
        this.entity = entity;
    }

    /**
     * Set the body of the request to a string. If not set or set to
     * {@code null} then no body is sent with the request. The
     * {@code Content-Type} will be sent as {@code application/json}.
     * If you need a different content type then use
     * {@link #setEntity(HttpEntity)}.
     *
     * @param entity JSON string to be set as the entity body of the request.
     */
    public void setJsonEntity(String entity) {
        setEntity(entity == null ? null : new StringEntity(entity, ContentType.APPLICATION_JSON));
    }

    /**
     * The body of the request. If {@code null} then no body
     * is sent with the request.
     */
    public HttpEntity getEntity() {
        return entity;
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
     * Set the portion of an HTTP request to OpenSearch that can be
     * manipulated without changing OpenSearch's behavior.
     *
     * @param options the options to be set.
     * @throws NullPointerException if {@code options} is null.
     */
    public void setOptions(RequestOptions.Builder options) {
        Objects.requireNonNull(options, "options cannot be null");
        this.options = options.build();
    }

    /**
     * Get the portion of an HTTP request to OpenSearch that can be
     * manipulated without changing OpenSearch's behavior.
     */
    public RequestOptions getOptions() {
        return options;
    }

    /**
     * Convert request to string representation
     */
    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("Request{");
        b.append("method='").append(method).append('\'');
        b.append(", endpoint='").append(endpoint).append('\'');
        if (false == parameters.isEmpty()) {
            b.append(", params=").append(parameters);
        }
        if (entity != null) {
            b.append(", entity=").append(entity);
        }
        b.append(", options=").append(options);
        return b.append('}').toString();
    }

    /**
     * Compare two requests for equality
     * @param obj request instance to compare with
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null || (obj.getClass() != getClass())) {
            return false;
        }
        if (obj == this) {
            return true;
        }

        Request other = (Request) obj;
        return method.equals(other.method)
            && endpoint.equals(other.endpoint)
            && parameters.equals(other.parameters)
            && Objects.equals(entity, other.entity)
            && options.equals(other.options);
    }

    /**
     * Calculate the hash code of the request
     */
    @Override
    public int hashCode() {
        return Objects.hash(method, endpoint, parameters, entity, options);
    }
}
