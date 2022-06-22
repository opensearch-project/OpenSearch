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

import org.opensearch.index.mapper.KnnAlgorithmContext.Method;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Class abstracts creation of KNN Algorithm context
 */
public class KnnAlgorithmContextFactory {

    public static final String HNSW_PARAMETER_MAX_CONNECTIONS = "max_connections";
    public static final String HNSW_PARAMETER_BEAM_WIDTH = "beam_width";

    protected static final int MAX_CONNECTIONS_DEFAULT_VALUE = 16;
    protected static final int MAX_CONNECTIONS_MAX_VALUE = 16;
    protected static final int BEAM_WIDTH_DEFAULT_VALUE = 100;
    protected static final int BEAM_WIDTH_MAX_VALUE = 512;

    private static final Map<Method, KnnAlgorithmContext> DEFAULT_CONTEXTS = Map.of(
        Method.HNSW,
        createContext(
            Method.HNSW,
            Map.of(HNSW_PARAMETER_MAX_CONNECTIONS, MAX_CONNECTIONS_DEFAULT_VALUE, HNSW_PARAMETER_BEAM_WIDTH, BEAM_WIDTH_DEFAULT_VALUE)
        )
    );

    public static KnnAlgorithmContext defaultContext(Method method) {
        return Optional.ofNullable(DEFAULT_CONTEXTS.get(method))
            .orElseThrow(
                () -> new IllegalArgumentException(
                    String.format(Locale.ROOT, "Invalid knn method provided [%s], only HNSW is supported", method.name())
                )
            );
    }

    public static KnnAlgorithmContext createContext(Method method, Map<String, Object> parameters) {
        Map<Method, Function<Map<String, Object>, KnnAlgorithmContext>> methodToContextSupplier = Map.of(Method.HNSW, hnswContext());

        return Optional.ofNullable(methodToContextSupplier.get(method))
            .orElseThrow(
                () -> new IllegalArgumentException(
                    String.format(Locale.ROOT, "Invalid knn method provided [%s], only HNSW is supported", method.name())
                )
            )
            .apply(parameters);
    }

    private static Function<Map<String, Object>, KnnAlgorithmContext> hnswContext() {
        Function<Map<String, Object>, KnnAlgorithmContext> supplierFunction = params -> {
            validateForSupportedParameters(Set.of(HNSW_PARAMETER_MAX_CONNECTIONS, HNSW_PARAMETER_BEAM_WIDTH), params);

            int maxConnections = getParameter(
                params,
                HNSW_PARAMETER_MAX_CONNECTIONS,
                MAX_CONNECTIONS_DEFAULT_VALUE,
                MAX_CONNECTIONS_MAX_VALUE
            );
            int beamWidth = getParameter(params, HNSW_PARAMETER_BEAM_WIDTH, BEAM_WIDTH_DEFAULT_VALUE, BEAM_WIDTH_MAX_VALUE);

            KnnAlgorithmContext hnswKnnMethodContext = new KnnAlgorithmContext(
                Method.HNSW,
                Map.of(HNSW_PARAMETER_MAX_CONNECTIONS, maxConnections, HNSW_PARAMETER_BEAM_WIDTH, beamWidth)
            );
            return hnswKnnMethodContext;
        };
        return supplierFunction;
    }

    private static int getParameter(Map<String, Object> parameters, String paramName, int defaultValue, int maxValue) {
        int value = defaultValue;
        if (isNullOrEmpty(parameters)) {
            return value;
        }
        if (parameters.containsKey(paramName)) {
            if (!(parameters.get(paramName) instanceof Integer)) {
                throw new MapperParsingException(
                    String.format(Locale.ROOT, "Invalid value for field %s, it must be an integer number", paramName)
                );
            }
            value = (int) parameters.get(paramName);
        }
        if (value > maxValue) {
            throw new MapperParsingException(String.format(Locale.ROOT, "%s value cannot be greater than %d", paramName, maxValue));
        }
        if (value <= 0) {
            throw new MapperParsingException(String.format(Locale.ROOT, "%s value must be greater than 0", paramName));
        }
        return value;
    }

    static void validateForSupportedParameters(Set<String> supportedParams, Map<String, Object> actualParams) {
        if (isNullOrEmpty(actualParams)) {
            return;
        }
        Optional<String> unsupportedParam = actualParams.keySet()
            .stream()
            .filter(actualParamName -> !supportedParams.contains(actualParamName))
            .findAny();
        if (unsupportedParam.isPresent()) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Algorithm parameter [%s] is not supported", unsupportedParam.get())
            );
        }
    }

    private static boolean isNullOrEmpty(Map<String, Object> parameters) {
        return parameters == null || parameters.isEmpty();
    }
}
