package org.opensearch.client;

import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable HTTP Request model for OpenSearch.
 */
public final class Request {

    private final String method;
    private final String endpoint;
    private final Map<String, String> parameters;
    private final HttpEntity entity;
    private final RequestOptions options;

    private Request(Builder builder) {
        this.method = builder.method;
        this.endpoint = builder.endpoint;
        this.parameters = Collections.unmodifiableMap(new HashMap<>(builder.parameters));
        this.entity = builder.entity;
        this.options = builder.options;
    }

    public String getMethod() {
        return method;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public Map<String, String> getParameters() {
        if (options.getParameters().isEmpty()) {
            return parameters;
        }
        Map<String, String> combined = new HashMap<>(parameters);
        combined.putAll(options.getParameters());
        return Collections.unmodifiableMap(combined);
    }

    public HttpEntity getEntity() {
        return entity;
    }

    public RequestOptions getOptions() {
        return options;
    }

    @Override
    public String toString() {
        return "Request{" +
            "method='" + method + '\'' +
            ", endpoint='" + endpoint + '\'' +
            ", parameters=" + parameters +
            ", entity=" + entity +
            ", options=" + options +
            '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(method, endpoint, parameters, entity, options);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Request other)) return false;
        return Objects.equals(method, other.method) &&
            Objects.equals(endpoint, other.endpoint) &&
            Objects.equals(parameters, other.parameters) &&
            Objects.equals(entity, other.entity) &&
            Objects.equals(options, other.options);
    }

    // ------------ BUILDER ----------------

    public static class Builder {
        private final String method;
        private final String endpoint;
        private Map<String, String> parameters = new HashMap<>();
        private HttpEntity entity;
        private RequestOptions options = RequestOptions.DEFAULT;

        public Builder(String method, String endpoint) {
            this.method = Objects.requireNonNull(method, "method cannot be null");
            this.endpoint = Objects.requireNonNull(endpoint, "endpoint cannot be null");
        }

        public Builder addParameter(String name, String value) {
            Objects.requireNonNull(name, "parameter name cannot be null");
            parameters.put(name, value);
            return this;
        }

        public Builder addParameters(Map<String, String> source) {
            Objects.requireNonNull(source, "parameter map cannot be null");
            parameters.putAll(source);
            return this;
        }

        public Builder jsonEntity(String body) {
            return body == null ? this : entity(new StringEntity(body, ContentType.APPLICATION_JSON));
        }

        public Builder entity(HttpEntity entity) {
            this.entity = entity;
            return this;
        }

        public Builder options(RequestOptions options) {
            this.options = Objects.requireNonNull(options);
            return this;
        }

        public Request build() {
            return new Request(this);
        }
    }
}

