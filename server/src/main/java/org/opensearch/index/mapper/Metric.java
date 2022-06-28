/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
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
