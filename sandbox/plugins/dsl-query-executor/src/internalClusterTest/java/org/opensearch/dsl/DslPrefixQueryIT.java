/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Integration tests for prefix query conversion to Calcite LIKE expressions.
 */
public class DslPrefixQueryIT extends DslIntegTestBase {

    @Override
    protected void createTestIndex() {
        createIndex(INDEX);
        ensureGreen();
        
        client().prepareIndex(INDEX)
            .setId("1")
            .setSource("{\"name\":\"laptop\",\"brand\":\"Apple\",\"model\":\"MacBook Pro\"}", XContentType.JSON)
            .get();
        client().prepareIndex(INDEX)
            .setId("2")
            .setSource("{\"name\":\"phone\",\"brand\":\"Samsung\",\"model\":\"Galaxy S21\"}", XContentType.JSON)
            .get();
        client().prepareIndex(INDEX)
            .setId("3")
            .setSource("{\"name\":\"tablet\",\"brand\":\"apple\",\"model\":\"iPad Air\"}", XContentType.JSON)
            .get();
        refresh(INDEX);
    }

    public void testBasicPrefixQuery() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.prefixQuery("name", "lap")
        )));
    }

    public void testPrefixQueryCaseSensitive() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.prefixQuery("brand", "Apple")
        )));
    }

    public void testPrefixQueryCaseInsensitive() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.prefixQuery("brand", "apple").caseInsensitive(true)
        )));
    }

    public void testPrefixQueryWithEmptyString() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.prefixQuery("name", "")
        )));
    }

    public void testPrefixQueryWithMultipleWords() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.prefixQuery("model", "MacBook")
        )));
    }

    public void testPrefixQueryInBoolQuery() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.prefixQuery("name", "lap"))
                .should(QueryBuilders.prefixQuery("brand", "App"))
        )));
    }

    public void testPrefixQueryWithSpecialCharacters() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.prefixQuery("model", "Galaxy S")
        )));
    }
}
