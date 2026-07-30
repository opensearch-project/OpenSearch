/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.opensearch.search.aggregations.AggregationBuilder;

import java.util.Map;

/**
 * Base type interface for aggregation translators.
 * Provides type identification for the {@link AggregationRegistry}.
 * Bucket and metric subtypes define their own contracts.
 */
public interface AggregationTranslator<T extends AggregationBuilder> {

    /** Returns the concrete AggregationBuilder class this type handles. */
    Class<T> getAggregationType();

    /**
     * Returns the user-supplied {@code meta} map from the request, or null when absent so the
     * response omits the {@code meta} section entirely (classic search renders {@code meta}
     * only when supplied). {@link AggregationBuilder#getMetadata()} masks "absent" as an empty
     * map, so an explicitly supplied empty {@code "meta": {}} is also treated as absent here.
     */
    static Map<String, Object> userMetadata(AggregationBuilder agg) {
        Map<String, Object> metadata = agg.getMetadata();
        return metadata == null || metadata.isEmpty() ? null : metadata;
    }
}
