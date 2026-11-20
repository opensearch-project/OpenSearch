/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * Aggregator supplier interface for the boxplot aggregation.
 *
 * @opensearch.internal
 */
public interface BoxplotAggregatorSupplier {

    /**
     * Builds the aggregator that computes the boxplot of the given values source.
     */
    Aggregator build(
        String name,
        ValuesSource valuesSource,
        DocValueFormat format,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata,
        double compression
    ) throws IOException;
}
