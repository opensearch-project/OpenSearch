/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

public class QueryCollectorArguments {
    private final boolean hasFilterCollector;

    private QueryCollectorArguments(final boolean hasFilterCollector) {
        this.hasFilterCollector = hasFilterCollector;
    }

    public boolean hasFilterCollector() {
        return hasFilterCollector;
    }

    public static class Builder {
        private boolean hasFilterCollector;

        public Builder hasFilterCollector(boolean hasFilterCollector) {
            this.hasFilterCollector = hasFilterCollector;
            return this;
        }

        public QueryCollectorArguments build() {
            return new QueryCollectorArguments(hasFilterCollector);
        }
    }
}
