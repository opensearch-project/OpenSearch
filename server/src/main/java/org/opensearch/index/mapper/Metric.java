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

import org.apache.lucene.index.VectorSimilarityFunction;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Abstracts supported metrics for dense_vector and KNN search
 */
public enum Metric {
    L2,
    COSINE,
    DOT_PRODUCT;

    private static final Map<String, Metric> STRING_TO_METRIC = Map.of("l2", L2, "cosine", COSINE, "dot_product", DOT_PRODUCT);

    private static final Map<Metric, VectorSimilarityFunction> METRIC_TO_VECTOR_SIMILARITY_FUNCTION = Map.of(
        L2,
        VectorSimilarityFunction.EUCLIDEAN,
        COSINE,
        VectorSimilarityFunction.COSINE,
        DOT_PRODUCT,
        VectorSimilarityFunction.DOT_PRODUCT
    );

    public static Metric fromName(String metricName) {
        return Optional.ofNullable(STRING_TO_METRIC.get(metricName.toLowerCase(Locale.ROOT)))
            .orElseThrow(
                () -> new IllegalArgumentException(String.format(Locale.ROOT, "Provided [metric] %s is not supported", metricName))
            );
    }

    /**
     * Convert from our Metric type to Lucene VectorSimilarityFunction type. Only Euclidean metric is supported
     */
    public static VectorSimilarityFunction toSimilarityFunction(Metric metric) {
        return Optional.ofNullable(METRIC_TO_VECTOR_SIMILARITY_FUNCTION.get(metric))
            .orElseThrow(
                () -> new IllegalArgumentException(
                    String.format(Locale.ROOT, "Provided metric %s cannot be converted to vector similarity function", metric.name())
                )
            );
    }
}
