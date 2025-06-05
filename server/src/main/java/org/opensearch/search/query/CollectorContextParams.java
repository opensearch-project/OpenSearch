/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.search.internal.SearchContext;

import java.util.HashMap;
import java.util.Map;

public class CollectorContextParams {
    private SearchContext searchContext;
    private boolean hasFilterCollector;
    // Add any additional parameters here
    private Map<String, Object> additionalParams;

    public static class Builder {
        private CollectorContextParams params;

        public Builder() {
            params = new CollectorContextParams();
            params.additionalParams = new HashMap<>();
        }

        public Builder withSearchContext(SearchContext searchContext) {
            params.searchContext = searchContext;
            return this;
        }

        public Builder withHasFilterCollector(boolean hasFilterCollector) {
            params.hasFilterCollector = hasFilterCollector;
            return this;
        }

        public Builder withAdditionalParam(String key, Object value) {
            params.additionalParams.put(key, value);
            return this;
        }

        public CollectorContextParams build() {
            return params;
        }
    }

    // Getters
    public SearchContext getSearchContext() {
        return searchContext;
    }

    public boolean getHasFilterCollector() {
        return hasFilterCollector;
    }

    public Object getAdditionalParam(String key) {
        return additionalParams.get(key);
    }
}
