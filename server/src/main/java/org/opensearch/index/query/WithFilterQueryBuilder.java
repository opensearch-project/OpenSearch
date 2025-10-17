/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

/**
 * Interface for QueryBuilders with a filter query builder method
 *
 * @opensearch.internal
 */
public interface WithFilterQueryBuilder {

    /**
     * returns filter query
     * @return filterQueryBuilder
     */
    QueryBuilder filterQueryBuilder();
}
