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

package org.opensearch.index.mapper;

import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Abstracts KNN Algorithm segment of dense_vector field type
 */
public class KnnAlgorithmContext implements ToXContentFragment, Writeable {

    private static final String PARAMETERS = "parameters";
    private static final String NAME = "name";

    private final Method method;
    private final Map<String, Object> parameters;

    public KnnAlgorithmContext(Method method, Map<String, Object> parameters) {
        this.method = method;
        this.parameters = parameters;
    }

    public Method getMethod() {
        return method;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public static KnnAlgorithmContext parse(Object in) {
        if (!(in instanceof Map<?, ?>)) {
            throw new MapperParsingException("Unable to parse [algorithm] component");
        }
        @SuppressWarnings("unchecked")
        final Map<String, Object> methodMap = (Map<String, Object>) in;
        Method method = Method.HNSW;
        Map<String, Object> parameters = Map.of();

        for (Map.Entry<String, Object> methodEntry : methodMap.entrySet()) {
            final String key = methodEntry.getKey();
            final Object value = methodEntry.getValue();
            if (NAME.equals(key)) {
                if (!(value instanceof String)) {
                    throw new MapperParsingException("Component [name] should be a string");
                }
                try {
                    Method.fromName((String) value);
                } catch (IllegalArgumentException illegalArgumentException) {
                    throw new MapperParsingException(
                        String.format(Locale.ROOT, "[algorithm name] value [%s] is invalid or not supported", value)
                    );
                }
            } else if (PARAMETERS.equals(key)) {
                if (value == null) {
                    parameters = null;
                    continue;
                }
                if (!(value instanceof Map)) {
                    throw new MapperParsingException("Unable to parse [parameters] for algorithm");
                }
                // Check to interpret map parameters as sub-methodComponentContexts
                parameters = ((Map<String, Object>) value).entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> {
                    Object v = e.getValue();
                    if (v instanceof Map) {
                        throw new MapperParsingException(
                            String.format(Locale.ROOT, "Unable to parse parameter [%s] for [algorithm]", e.getValue())
                        );
                    }
                    return v;
                }));

            } else {
                throw new MapperParsingException(String.format(Locale.ROOT, "Invalid parameter %s for [algorithm]", key));
            }
        }
        return KnnAlgorithmContextFactory.createContext(method, parameters);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(NAME, method.name());
        if (parameters == null) {
            builder.field(PARAMETERS, (String) null);
        } else {
            builder.startObject(PARAMETERS);
            parameters.forEach((key, value) -> {
                try {
                    builder.field(key, value);
                } catch (IOException ioe) {
                    throw new RuntimeException("Unable to generate xcontent for method component");
                }

            });
            builder.endObject();
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.method.name());
        if (this.parameters != null) {
            out.writeMap(this.parameters, StreamOutput::writeString, new ParameterMapValueWriter());
        }
    }

    private static class ParameterMapValueWriter implements Writer<Object> {

        private ParameterMapValueWriter() {}

        @Override
        public void write(StreamOutput out, Object o) throws IOException {
            if (o instanceof KnnAlgorithmContext) {
                out.writeBoolean(true);
                ((KnnAlgorithmContext) o).writeTo(out);
            } else {
                out.writeBoolean(false);
                out.writeGenericValue(o);
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        KnnAlgorithmContext that = (KnnAlgorithmContext) obj;
        return method == that.method && this.parameters.equals(that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(method, parameters);
    }

    /**
     * Abstracts supported search methods for KNN
     */
    public enum Method {
        HNSW;

        private static final Map<String, Method> STRING_TO_METHOD = Map.of("hnsw", HNSW);

        public static Method fromName(String methodName) {
            return Optional.ofNullable(STRING_TO_METHOD.get(methodName.toLowerCase(Locale.ROOT)))
                .orElseThrow(
                    () -> new IllegalArgumentException(String.format(Locale.ROOT, "Provided knn method %s is not supported", methodName))
                );
        }
    }
}
