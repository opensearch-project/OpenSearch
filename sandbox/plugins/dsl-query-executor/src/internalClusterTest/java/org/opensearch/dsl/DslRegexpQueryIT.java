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
import org.opensearch.index.query.RegexpFlag;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Integration tests for regexp query conversion and cross-engine delegation.
 */
@AwaitsFix(bugUrl = "analytics engine pipeline not E2E complete: fragment conversion + shard execution + Arrow Flight drain not yet wired")
public class DslRegexpQueryIT extends DslIntegTestBase {

    public void testRegexpQueryOnKeywordField() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(QueryBuilders.regexpQuery("name", "lap.*"))));
    }

    public void testRegexpQueryWithCaseInsensitive() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(QueryBuilders.regexpQuery("name", "LAP.*").caseInsensitive(true))));
    }

    public void testRegexpQueryWithFlags() {
        createTestIndex();
        assertOk(
            search(
                new SearchSourceBuilder().query(
                    QueryBuilders.regexpQuery("name", "lap.*").flags(RegexpFlag.COMPLEMENT, RegexpFlag.INTERVAL)
                )
            )
        );
    }

    public void testCrossEngineRegexpAndRange() {
        createTestIndex();
        assertOk(
            search(
                new SearchSourceBuilder().query(
                    QueryBuilders.boolQuery()
                        .filter(QueryBuilders.regexpQuery("name", "lap.*"))
                        .filter(QueryBuilders.rangeQuery("price").gte(500).lte(2000))
                )
            )
        );
    }
}
