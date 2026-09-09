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

package org.opensearch.internal.httpclient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The portion of an HTTP request to OpenSearch that can be
 * manipulated without changing OpenSearch's behavior.
 * Note: This is an experimental API.
 */
public final class RequestOptions {
    /**
     * Default request options.
     */
    public static final RequestOptions DEFAULT = new Builder(
        Collections.emptyMap(),
        Collections.emptyMap(),
        null,
        Duration.ofMillis(RestHttpClientBuilder.DEFAULT_RESPONSE_TIMEOUT_MILLIS)
    ).build();

    private final Map<String, List<String>> headers;
    private final Map<String, String> parameters;
    private final WarningsHandler warningsHandler;
    private final Duration timeout;

    private RequestOptions(Builder builder) {
        this.headers = Collections.unmodifiableMap(new HashMap<>(builder.headers));
        this.parameters = Collections.unmodifiableMap(new HashMap<>(builder.parameters));
        this.warningsHandler = builder.warningsHandler;
        this.timeout = builder.timeout;
    }

    /**
     * Create a builder that contains these options but can be modified.
     */
    public Builder toBuilder() {
        return new Builder(headers, parameters, warningsHandler, timeout);
    }

    /**
     * Headers to attach to the request.
     */
    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    /**
     * Query parameters to attach to the request. Any parameters present here
     * will override matching parameters in the {@link Request}, if they exist.
     */
    public Map<String, String> getParameters() {
        return parameters;
    }

    /**
     * How this request should handle warnings. If null (the default) then
     * this request will default to the behavior dictacted by
     * {@link RestHttpClientBuilder#setStrictDeprecationMode}.
     * <p>
     * This can be set to {@link WarningsHandler#PERMISSIVE} if the client
     * should ignore all warnings which is the same behavior as setting
     * strictDeprecationMode to true. It can be set to
     * {@link WarningsHandler#STRICT} if the client should fail if there are
     * any warnings which is the same behavior as settings
     * strictDeprecationMode to false.
     * <p>
     * It can also be set to a custom implementation of
     * {@linkplain WarningsHandler} to permit only certain warnings or to
     * fail the request if the warnings returned don't
     * <strong>exactly</strong> match some set.
     */
    public WarningsHandler getWarningsHandler() {
        return warningsHandler;
    }

    /**
     * Gets the request timeout
     * @return request timeout
     */
    public Duration getTimeout() {
        return timeout;
    }

    /**
     * Convert request options to string representation
     */
    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("RequestOptions{");
        boolean comma = false;
        if (headers.size() > 0) {
            comma = true;
            b.append("headers=");
            b.append(headers.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(",")));
        }
        if (timeout != null) {
            if (comma) b.append(", ");
            comma = true;
            b.append("timeout=").append(timeout.toMillis()).append("ms");
        }
        if (parameters.size() > 0) {
            if (comma) b.append(", ");
            comma = true;
            b.append("parameters=");
            b.append(parameters.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(",")));
        }
        if (warningsHandler != null) {
            if (comma) b.append(", ");
            comma = true;
            b.append("warningsHandler=").append(warningsHandler);
        }
        return b.append('}').toString();
    }

    /**
     * Compare two request options for equality
     * @param obj request options instance to compare with
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null || (obj.getClass() != getClass())) {
            return false;
        }
        if (obj == this) {
            return true;
        }

        RequestOptions other = (RequestOptions) obj;
        return headers.equals(other.headers)
            && parameters.equals(other.parameters)
            && Objects.equals(timeout, other.timeout)
            && Objects.equals(warningsHandler, other.warningsHandler);
    }

    /**
     * Calculate the hash code of the request options
     */
    @Override
    public int hashCode() {
        return Objects.hash(headers, parameters, warningsHandler);
    }

    /**
     * Builds {@link RequestOptions}. Get one by calling
     * {@link RequestOptions#toBuilder} on {@link RequestOptions#DEFAULT} or
     * any other {@linkplain RequestOptions}.
     */
    public static class Builder {
        private final Map<String, List<String>> headers;
        private final Map<String, String> parameters;
        private WarningsHandler warningsHandler;
        private Duration timeout = Duration.ofMillis(RestHttpClientBuilder.DEFAULT_RESPONSE_TIMEOUT_MILLIS);

        private Builder(
            Map<String, List<String>> headers,
            Map<String, String> parameters,
            WarningsHandler warningsHandler,
            Duration timeout
        ) {
            this.headers = new HashMap<>(headers);
            this.parameters = new HashMap<>(parameters);
            this.warningsHandler = warningsHandler;
            this.timeout = timeout;
        }

        /**
         * Build the {@linkplain RequestOptions}.
         */
        public RequestOptions build() {
            return new RequestOptions(this);
        }

        /**
         * Sets request timeout
         *
         * @param timeout timeout
         */
        public void setTimeout(Duration timeout) {
            Objects.requireNonNull(timeout, "timeout cannot be null");
            this.timeout = timeout;
        }

        /**
         * Add the provided headers to the request.
         *
         * @param headers headers to add
         * @throws NullPointerException if {@code name} or {@code value} is null.
         */
        public Builder addHeaders(Map<String, List<String>> headers) {
            Objects.requireNonNull(headers, "headers cannot be null");
            for (Map.Entry<String, List<String>> header : headers.entrySet()) {
                header.getValue().forEach(v -> addHeader(header.getKey(), v));
            }
            return this;
        }

        /**
         * Add the provided header to the request.
         *
         * @param name  the header name
         * @param value the header value
         * @throws NullPointerException if {@code name} or {@code value} is null.
         */
        public Builder addHeader(String name, String value) {
            Objects.requireNonNull(name, "header name cannot be null");
            Objects.requireNonNull(value, "header value cannot be null");
            this.headers.computeIfAbsent(name, v -> new ArrayList<>()).add(value);
            return this;
        }

        /**
         * Add the provided query parameter to the request. Any parameters added here
         * will override matching parameters in the {@link Request}, if they exist.
         *
         * @param name  the query parameter name
         * @param value the query parameter value
         * @throws NullPointerException if {@code name} or {@code value} is null.
         */
        public Builder addParameter(String name, String value) {
            Objects.requireNonNull(name, "query parameter name cannot be null");
            Objects.requireNonNull(value, "query parameter value cannot be null");
            this.parameters.put(name, value);
            return this;
        }

        /**
         * How this request should handle warnings. If null (the default) then
         * this request will default to the behavior dictacted by
         * {@link RestHttpClientBuilder#setStrictDeprecationMode}.
         * <p>
         * This can be set to {@link WarningsHandler#PERMISSIVE} if the client
         * should ignore all warnings which is the same behavior as setting
         * strictDeprecationMode to true. It can be set to
         * {@link WarningsHandler#STRICT} if the client should fail if there are
         * any warnings which is the same behavior as settings
         * strictDeprecationMode to false.
         * <p>
         * It can also be set to a custom implementation of
         * {@linkplain WarningsHandler} to permit only certain warnings or to
         * fail the request if the warnings returned don't
         * <strong>exactly</strong> match some set.
         *
         * @param warningsHandler the {@link WarningsHandler} to be used
         */
        public void setWarningsHandler(WarningsHandler warningsHandler) {
            this.warningsHandler = warningsHandler;
        }
    }
}
