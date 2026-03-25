/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.opensearch.search.aggregations.AggregationBuilder;

/**
 * Base type interface for aggregation translators.
 * Provides type identification for the {@link AggregationRegistry}.
 * Bucket and metric subtypes define their own contracts.
 */
public interface AggregationType<T extends AggregationBuilder> {

    /** Returns the concrete AggregationBuilder class this type handles. */
    Class<T> getAggregationType();
}
