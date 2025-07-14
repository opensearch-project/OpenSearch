/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Arguments for {@link QueryCollectorContextSpecRegistry}
 */
@ExperimentalApi
public final class QueryCollectorArguments {
    private final boolean hasFilterCollector;

    private QueryCollectorArguments(final boolean hasFilterCollector) {
        this.hasFilterCollector = hasFilterCollector;
    }

    /**
     * Whether the query has a filter collector.
     * @return true if the query has a filter collector, false otherwise
     */
    public boolean hasFilterCollector() {
        return hasFilterCollector;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryCollectorArguments queryCollectorArguments = (QueryCollectorArguments) o;
        return hasFilterCollector == queryCollectorArguments.hasFilterCollector;
    }

    @Override
    public int hashCode() {
        return Boolean.hashCode(hasFilterCollector);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "QueryCollectorArguments[hasFilterCollector=" + hasFilterCollector + "]";
    }

    /**
     * Builder for {@link QueryCollectorArguments}
     */
    public static class Builder {
        private boolean hasFilterCollector;

        /**
         * Set the flag for query has a filter collector.
         * @param hasFilterCollector true if the query has a filter collector, false otherwise
         * @return Builder instance
         */
        public Builder hasFilterCollector(boolean hasFilterCollector) {
            this.hasFilterCollector = hasFilterCollector;
            return this;
        }

        /**
         * Build the arguments for the query collector context spec registry.
         * @return QueryCollectorArguments instance
         */
        public QueryCollectorArguments build() {
            return new QueryCollectorArguments(hasFilterCollector);
        }
    }
}
