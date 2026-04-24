/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;

/**
 * Integration tests for DSL sort and pagination conversion.
 * Uses matchAllQuery; focus is on sort/from/size behavior.
 */
@AwaitsFix(bugUrl = "analytics engine pipeline not E2E complete: FieldStorageResolver fails with 'Field [...] has no storage in any format' (no backend registers storage for index fields); shard execution + Arrow Flight drain also not yet wired")
public class DslSortIT extends DslIntegTestBase {

    public void testDefaultPagination() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder()));
    }

    public void testSortAscending() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().sort("name", SortOrder.ASC)));
    }

    public void testSortDescending() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().sort("price", SortOrder.DESC)));
    }

    public void testMultipleSortFields() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().sort("brand", SortOrder.ASC).sort("price", SortOrder.DESC)));
    }

    public void testCustomSize() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().size(5)));
    }

    public void testFromAndSize() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().from(0).size(5)));
    }

    public void testFromOffset() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().from(10).size(5)));
    }
}
