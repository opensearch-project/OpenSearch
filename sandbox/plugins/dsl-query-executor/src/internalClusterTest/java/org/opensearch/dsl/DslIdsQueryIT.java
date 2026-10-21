/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Integration tests for DSL ids query conversion.
 * Parked: SearchResponseBuilder.build() returns SearchHits.empty(true),
 * so no query returns hits end-to-end yet.
 */
@AwaitsFix(bugUrl = "SearchResponseBuilder returns empty hits — ids query E2E blocked until response stubbing removed")
public class DslIdsQueryIT extends DslIntegTestBase {

    public void testIdsQuerySingle() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(QueryBuilders.idsQuery().addIds("1"))));
    }

    public void testIdsQueryMultiple() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(QueryBuilders.idsQuery().addIds("1", "2", "3"))));
    }

    public void testIdsQueryEmpty() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(QueryBuilders.idsQuery())));
    }
}
