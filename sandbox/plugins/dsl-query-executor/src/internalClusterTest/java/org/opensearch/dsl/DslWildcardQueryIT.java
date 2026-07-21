/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Integration tests for wildcard query conversion to Calcite LIKE expressions.
 */
@AwaitsFix(bugUrl = "analytics engine pipeline not E2E complete: fragment conversion + shard execution + Arrow Flight drain not yet wired")
public class DslWildcardQueryIT extends DslIntegTestBase {

    @Override
    protected void createTestIndex() {
        createIndex(INDEX);
        ensureGreen();

        client().prepareIndex(INDEX)
            .setId("1")
            .setSource("{\"name\":\"laptop\",\"model\":\"MacBook Pro\",\"sku\":\"MB-2021-001\"}", XContentType.JSON)
            .get();
        client().prepareIndex(INDEX)
            .setId("2")
            .setSource("{\"name\":\"phone\",\"model\":\"Galaxy S21\",\"sku\":\"GS-2021-002\"}", XContentType.JSON)
            .get();
        client().prepareIndex(INDEX)
            .setId("3")
            .setSource("{\"name\":\"tablet\",\"model\":\"iPad Air\",\"sku\":\"IP-2020-003\"}", XContentType.JSON)
            .get();
        refresh(INDEX);
    }

    public void testWildcardWithAsterisk() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(QueryBuilders.wildcardQuery("name", "lap*"))));
    }

    public void testWildcardWithQuestionMark() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(QueryBuilders.wildcardQuery("name", "p?one"))));
    }

    public void testWildcardWithBothWildcards() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(QueryBuilders.wildcardQuery("model", "?acBook*"))));
    }

    public void testWildcardCaseInsensitive() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(QueryBuilders.wildcardQuery("model", "MACBOOK*").caseInsensitive(true))));
    }

    public void testWildcardWithMultipleAsterisks() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(QueryBuilders.wildcardQuery("sku", "*-2021-*"))));
    }

    public void testWildcardMatchAll() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(QueryBuilders.wildcardQuery("name", "*"))));
    }

    public void testWildcardInBoolQuery() {
        createTestIndex();
        assertOk(
            search(
                new SearchSourceBuilder().query(
                    QueryBuilders.boolQuery()
                        .must(QueryBuilders.wildcardQuery("name", "lap*"))
                        .should(QueryBuilders.wildcardQuery("model", "*Pro"))
                )
            )
        );
    }
}
