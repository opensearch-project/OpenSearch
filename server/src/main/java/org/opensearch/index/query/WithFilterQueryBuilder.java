/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

/**
 * Interface for query builders that contain a filter component. This interface enables
 * context-aware searching by allowing plugin queries to expose their filter queries
 * for criteria extraction.
 *
 * <p>Query builders implementing this interface indicate they contain a filter query
 * that should be considered during context-aware criteria extraction. This allows
 * custom query types to participate in segment-level filtering optimizations.
 *
 * @opensearch.internal
 */
public interface WithFilterQueryBuilder {

    /**
     * Returns the filter query component of this query builder.
     *
     * @return The QueryBuilder representing the filter part of this query.
     *         This filter will be analyzed during context-aware criteria extraction.
     */
    QueryBuilder filterQueryBuilder();
}
