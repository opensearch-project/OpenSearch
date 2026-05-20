/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.query;

import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

public class SemanticVersionFieldQueryTests extends OpenSearchSingleNodeTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        setupIndex();
        ensureGreen("test");
    }

    protected XContentBuilder createMapping() throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("version")
            .field("type", "version")
            .endObject()
            .endObject()
            .endObject();
    }

    protected void setupIndex() throws IOException {
        XContentBuilder mapping = createMapping();
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        String doc1 = """
            {
                "version": "1.0.0"
            }""";
        client().index(new IndexRequest("test").id("1").source(doc1, MediaTypeRegistry.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        String doc2 = """
            {
                "version": "1.0.1"
            }""";
        client().index(new IndexRequest("test").id("2").source(doc2, MediaTypeRegistry.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        String doc3 = """
            {
                "version": "1.1.0"
            }""";
        client().index(new IndexRequest("test").id("3").source(doc3, MediaTypeRegistry.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        String doc4 = """
            {
                "version": "2.0.0"
            }""";
        client().index(new IndexRequest("test").id("4").source(doc4, MediaTypeRegistry.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        String doc5 = """
            {
                "version": "1.0.0-alpha"
            }""";
        client().index(new IndexRequest("test").id("5").source(doc5, MediaTypeRegistry.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        String doc6 = """
            {
                "version": "1.0.0-beta"
            }""";
        client().index(new IndexRequest("test").id("6").source(doc6, MediaTypeRegistry.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        String doc7 = """
            {
                "version": "1.0.0+build.123"
            }""";
        client().index(new IndexRequest("test").id("7").source(doc7, MediaTypeRegistry.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();
    }

    /**
     * Expected behavior:
     * 1.0.0-alpha (Match)
     * 1.0.0-beta (Match)
     * 1.0.0+build.123 (Match)
     * 1.0.0 (Match)
     * 1.0.1 (Match)
     * 1.1.0 (Match)
     * 2.0.0 (Match)
     * @throws IOException
     */
    public void testExistsQuery() throws IOException {
        // Test exists query
        SearchResponse response = client().prepareSearch("test").setQuery(QueryBuilders.existsQuery("version")).get();

        assertSearchResponse(response);
        assertHitCount(response, 7);  // Should match all documents with a version field

        // Test with non-existent field
        response = client().prepareSearch("test").setQuery(QueryBuilders.existsQuery("non_existent_field")).get();

        assertSearchResponse(response);
        assertHitCount(response, 0);
    }

    /**
     * 1.0.0-alpha (Ignore)
     * 1.0.0-beta (Ignore)
     * 1.0.0+build.123 (Ignore)
     * 1.0.0 (Ignore)
     * 1.0.1 (Match)
     * 1.1.0 (Match)
     * 2.0.0 (Ignore)
     * @throws IOException
     */
    public void testMatchQuery() throws IOException {
        SearchResponse response = client().prepareSearch("test").setQuery(QueryBuilders.matchQuery("version", "1.0.0")).get();

        assertSearchResponse(response);
        assertHitCount(response, 1); // Should match "1.0.0" if analyzed, or 0 if not.
    }

    /**
     * 1.0.0-alpha (Ignore)
     * 1.0.0-beta (Ignore)
     * 1.0.0+build.123 (Ignore)
     * 1.0.0 (Ignore)
     * 1.0.1 (Match)
     * 1.1.0 (Match)
     * 2.0.0 (Ignore)
     * @throws IOException
     */
    public void testMultiMatchQuery() throws IOException {
        SearchResponse response = client().prepareSearch("test")
            .setQuery(QueryBuilders.multiMatchQuery("1.0.0", "version", "non_existent_field"))
            .get();

        assertSearchResponse(response);
        assertHitCount(response, 1); // Match only "1.0.0" in "version"
    }

    /**
     * 1.0.0-alpha (Ignore)
     * 1.0.0-beta (Ignore)
     * 1.0.0+build.123 (Ignore)
     * 1.0.0 (Match)
     * 1.0.1 (Ignore)
     * 1.1.0 (Ignore)
     * 2.0.0 (Ignore)
     * @throws IOException if the search request fails
     */
    public void testTermQuery() throws IOException {
        GetResponse getResponse = client().prepareGet("test", "1").get();
        assertTrue("Document should exist", getResponse.isExists());
        assertEquals("1.0.0", getResponse.getSourceAsMap().get("version"));

        // Term query
        SearchResponse response = client().prepareSearch("test").setQuery(QueryBuilders.termQuery("version", "1.0.0")).get();

        assertSearchResponse(response);
        assertHitCount(response, 1);
    }

    /**
     * 1.0.0-alpha (Ignore)
     * 1.0.0-beta (Ignore)
     * 1.0.0+build.123 (Ignore)
     * 1.0.0 (Match)
     * 1.0.1 (Match)
     * 1.1.0 (Ignore)
     * 2.0.0 (Ignore)
     * @throws IOException
     */
    public void testTermsQuery() throws IOException {
        SearchResponse response = client().prepareSearch("test").setQuery(QueryBuilders.termsQuery("version", "1.0.0", "1.0.1")).get();

        assertSearchResponse(response);
        assertHitCount(response, 2);
    }

    /**
     * 1.0.0-alpha (Ignore)
     * 1.0.0-beta (Ignore)
     * 1.0.0+build.123 (Ignore)
     * 1.0.0 (Ignore)
     * 1.0.1 (Match)
     * 1.1.0 (Match)
     * 2.0.0 (Ignore)
     * @throws IOException
     */
    public void testRangeQuery() throws IOException {
        SearchResponse response = client().prepareSearch("test")
            .setQuery(QueryBuilders.rangeQuery("version").gt("1.0.0").lt("2.0.0"))
            .get();
        assertSearchResponse(response);
        assertHitCount(response, 2);
    }

    /**
     * 1.0.0-alpha (Match)
     * 1.0.0-beta (Match)
     * 1.0.0+build.123 (Match)
     * 1.0.0 (Match)
     * 1.0.1 (Ignore)
     * 1.1.0 (Ignore)
     * 2.0.0 (Ignore)
     * @throws IOException
     */
    public void testRangeQueryIncludingPreRelease() throws IOException {
        SearchResponse response = client().prepareSearch("test")
            .setQuery(QueryBuilders.rangeQuery("version").gte("1.0.0-alpha").lt("1.0.1"))
            .get();

        assertSearchResponse(response);
        assertHitCount(response, 4);
    }

    /**
     * 1.0.0-alpha (Match)
     * 1.0.0-beta (Match)
     * 1.0.0+build.123 (Match)
     * 1.0.0 (Match)
     * 1.0.1 (Match)
     * 1.1.0 (Match)
     * 2.0.0 (Ignore)
     * @throws IOException
     */
    public void testPrefixQuery() throws IOException {
        SearchResponse response = client().prepareSearch("test").setQuery(QueryBuilders.prefixQuery("version", "1.")).get();
        assertSearchResponse(response);
        assertHitCount(response, 6);
    }

    /**
     * 1.0.0-alpha (Ignore)
     * 1.0.0-beta (Ignore)
     * 1.0.0+build.123 (Ignore)
     * 1.0.0 (Match)
     * 1.0.1 (Ignore)
     * 1.1.0 (Ignore)
     * 2.0.0 (Match)
     * @throws IOException
     */
    public void testWildcardQuery() throws IOException {
        SearchResponse response = client().prepareSearch("test").setQuery(QueryBuilders.wildcardQuery("version", "*.0.0")).get();
        assertSearchResponse(response);
        assertHitCount(response, 2);
    }

    /**
     * 1.0.0-alpha (Ignore)
     * 1.0.0-beta (Ignore)
     * 1.0.0+build.123 (Ignore)
     * 1.0.0 (Match)
     * 1.0.1 (Ignore)
     * 1.1.0 (Match)
     * 2.0.0 (Ignore)
     * @throws IOException
     */
    public void testRegexpQuery() throws IOException {
        SearchResponse response = client().prepareSearch("test").setQuery(QueryBuilders.regexpQuery("version", "1\\..*\\.0")).get();
        assertSearchResponse(response);
        assertHitCount(response, 2);
    }

    /**
     * 1.0.0-alpha (Ignore)
     * 1.0.0-beta (Ignore)
     * 1.0.0+build.123 (Ignore)
     * 1.0.0 (Match)
     * 1.0.1 (Match)
     * 1.1.0 (Ignore)
     * 2.0.0 (Ignore)
     * @throws IOException
     */
    public void testFuzzyQuery() throws IOException {
        SearchResponse response = client().prepareSearch("test")
            .setQuery(QueryBuilders.fuzzyQuery("version", "1.0.1").fuzziness(Fuzziness.ONE))
            .get();
        assertSearchResponse(response);
        assertHitCount(response, 2);
    }

    /**
     * 1.0.0-alpha — excluded because of dash (mustNot)
     * 1.0.0-beta — excluded (mustNot)
     * 1.0.0+build.123 — no dash, starts with 1. → included
     * 1.0.0 — included
     * 1.0.1 — included
     * 1.1.0 — included (starts with 1. and no dash)
     * 2.0.0 — excluded (does not start with 1.)
     * @throws IOException
     */
    public void testComplexQuery() throws IOException {
        SearchResponse response = client().prepareSearch("test")
            .setQuery(
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.prefixQuery("version", "1."))
                    .mustNot(QueryBuilders.regexpQuery("version", ".*-.*"))
                    .should(QueryBuilders.termQuery("version", "1.0.0"))
            )
            .get();
        assertSearchResponse(response);
        assertHitCount(response, 4);
    }

    /**
     * Should match documents with IDs 1 and 2 regardless of version:
     * Document 1 - version 1.0.0
     * Document 2 - version 1.0.1
     * @throws IOException
     */
    public void testIdsQuery() throws IOException {
        SearchResponse response = client().prepareSearch("test").setQuery(QueryBuilders.idsQuery().addIds("1", "2")).get();

        assertSearchResponse(response);
        assertHitCount(response, 2);
    }

    /**
     * 1.0.0-alpha (Ignore)
     * 1.0.0-beta (Ignore)
     * 1.0.0+build.123 (Ignore)
     * 1.0.0 (Match)
     * 1.0.1 (Ignore)
     * 1.1.0 (Ignore)
     * 2.0.0 (Ignore)
     * @throws IOException
     */
    public void testConstantScoreQuery() throws IOException {
        SearchResponse response = client().prepareSearch("test")
            .setQuery(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("version", "1.0.0")))
            .get();

        assertSearchResponse(response);
        assertHitCount(response, 1);
    }

    /**
     * 1.0.0-alpha (Ignore)
     * 1.0.0-beta (Ignore)
     * 1.0.0+build.123 (Ignore)
     * 1.0.0 (Match)
     * 1.0.1 (Ignore)
     * 1.1.0 (Ignore)
     * 2.0.0 (Ignore)
     * @throws IOException
     */
    public void testFunctionScoreQuery() throws IOException {
        SearchResponse response = client().prepareSearch("test")
            .setQuery(QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("version", "1.0.0")))
            .get();

        assertSearchResponse(response);
        assertHitCount(response, 1);
    }

    /**
     * 1.0.0-alpha (Ignore)
     * 1.0.0-beta (Ignore)
     * 1.0.0+build.123 (Ignore)
     * 1.0.0 (Ignore)
     * 1.0.1 (Match)
     * 1.1.0 (Match)
     * 2.0.0 (Ignore)
     * @throws IOException
     */
    public void testMatchPhraseQuery() throws IOException {
        SearchResponse response = client().prepareSearch("test").setQuery(QueryBuilders.matchPhraseQuery("version", "1.0.0")).get();

        assertSearchResponse(response);
        assertHitCount(response, 1);
    }

    /**
     * Sorted ascending:
     * First hit: 1.0.0-alpha
     * Second hit: 1.0.0-beta
     * Third hit: 1.0.0
     * Fourth hit: 1.0.0+build.123
     * Fifth hit: 1.0.1
     * Sixth hit: 1.1.0
     * Seventh hit: 2.0.0
     * @throws IOException
     */
    public void testSortByVersion() throws IOException {
        SearchResponse response = client().prepareSearch("test")
            .addSort("version", org.opensearch.search.sort.SortOrder.ASC)
            .setQuery(QueryBuilders.matchAllQuery())
            .get();

        assertSearchResponse(response);
        assertHitCount(response, 7);
        assertEquals("1.0.0-alpha", response.getHits().getAt(0).getSourceAsMap().get("version"));
    }

    /**
     * Test that a term query for an invalid version returns no hits.
     * @throws IOException
     */
    public void testTermQueryInvalidVersion() throws IOException {
        SearchResponse response = client().prepareSearch("test").setQuery(QueryBuilders.termQuery("version", "invalid-version")).get();

        assertSearchResponse(response);
        assertHitCount(response, 0);
    }
}
