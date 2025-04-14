/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

/**
 * Class that holds the low level information of hybrid query in the form of context
 */
public class HybridQueryContext {
    private Integer paginationDepth;

    private HybridQueryContext(Integer paginationDepth) {
        this.paginationDepth = paginationDepth;
    }

    public Integer getPaginationDepth() {
        return paginationDepth;
    }

    public static HybridQueryContextBuilder builder() {
        return new HybridQueryContextBuilder();
    }

    public static class HybridQueryContextBuilder {
        private Integer paginationDepth;

        public HybridQueryContextBuilder paginationDepth(Integer paginationDepth) {
            this.paginationDepth = paginationDepth;
            return this;
        }

        public HybridQueryContext build() {
            return new HybridQueryContext(paginationDepth);
        }
    }
}
