/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;

/**
 * Integration tests for DSL search_after pagination.
 */
public class DslSearchAfterIT extends DslIntegTestBase {

    public void testSearchAfterWithSingleSort() {
        createTestIndex();
        assertOk(search(
            new SearchSourceBuilder()
                .sort("price", SortOrder.ASC)
                .searchAfter(new Object[]{1000})
        ));
    }

    public void testSearchAfterWithMultipleSorts() {
        createTestIndex();
        assertOk(search(
            new SearchSourceBuilder()
                .sort("brand", SortOrder.ASC)
                .sort("price", SortOrder.DESC)
                .searchAfter(new Object[]{"brandX", 1200})
        ));
    }

    public void testSearchAfterWithQuery() {
        createTestIndex();
        assertOk(search(
            new SearchSourceBuilder()
                .query(org.opensearch.index.query.QueryBuilders.matchAllQuery())
                .sort("rating", SortOrder.DESC)
                .searchAfter(new Object[]{4.0})
        ));
    }
}
