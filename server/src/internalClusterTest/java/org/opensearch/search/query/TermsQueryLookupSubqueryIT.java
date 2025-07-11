package org.opensearch.search.query;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.indices.TermsLookup;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.HashSet;
import java.util.Set;

public class TermsQueryLookupSubqueryIT extends OpenSearchIntegTestCase {

    public void testAsyncTermsLookupWithSubqueryFetchesTerms() throws Exception {
        // Create students index and add students
        createIndex("students");
        client().index(new IndexRequest("students").id("1")
            .source("{\"name\": \"Jane Doe\", \"student_id\": \"111\"}", XContentType.JSON)).actionGet();
        client().index(new IndexRequest("students").id("2")
            .source("{\"name\": \"Mary Major\", \"student_id\": \"222\"}", XContentType.JSON)).actionGet();

        // Create classes index and add a class with enrolled students
        createIndex("classes");
        client().index(new IndexRequest("classes").id("101")
            .source("{\"name\": \"CS101\", \"enrolled\": [\"111\", \"222\"]}", XContentType.JSON)).actionGet();

        refresh();

        // Build the subquery to find the class named "CS101"
        QueryBuilder subquery = QueryBuilders.matchQuery("name", "CS101");

        // Build the TermsLookup with subquery
        TermsLookup termsLookup = new TermsLookup("classes", null, "enrolled", subquery);

        // Build the terms query for students using the above lookup
        TermsQueryBuilder tqb = QueryBuilders.termsLookupQuery("student_id", termsLookup);

        // Search students that are enrolled in CS101
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(tqb);
        SearchRequest searchRequest = new SearchRequest("students").source(sourceBuilder);

        SearchResponse response = client().search(searchRequest).actionGet();

        // Assert that both students are matched
        assertEquals(2, response.getHits().getTotalHits().value());
        Set<String> foundIds = new HashSet<>();
        response.getHits().forEach(hit -> foundIds.add(hit.getId()));
        assertTrue(foundIds.contains("1"));
        assertTrue(foundIds.contains("2"));
    }
}
