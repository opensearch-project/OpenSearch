/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.opensearch.OpenSearchParseException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.AbstractBuilderTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class HierarchicalRoutingSearchProcessorTests extends AbstractBuilderTestCase {

    public void testTermQueryRouting() throws Exception {
        HierarchicalRoutingSearchProcessor processor = createProcessor("path_field", 2, "/", true);

        QueryBuilder query = new TermQueryBuilder("path_field", "/company/engineering/team/file.txt");
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest transformedRequest = processor.processRequest(request);

        assertThat(transformedRequest.routing(), notNullValue());
        // Routing should be deterministic for same input
        String expectedRouting = computeExpectedRouting("company/engineering", "/");
        assertThat(transformedRequest.routing(), equalTo(expectedRouting));
    }

    public void testPrefixQueryRouting() throws Exception {
        HierarchicalRoutingSearchProcessor processor = createProcessor("path_field", 2, "/", true);

        QueryBuilder query = new PrefixQueryBuilder("path_field", "/company/engineering");
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest transformedRequest = processor.processRequest(request);

        assertThat(transformedRequest.routing(), notNullValue());
        String expectedRouting = computeExpectedRouting("company/engineering", "/");
        assertThat(transformedRequest.routing(), equalTo(expectedRouting));
    }

    public void testWildcardQueryRouting() throws Exception {
        HierarchicalRoutingSearchProcessor processor = createProcessor("path_field", 2, "/", true);

        QueryBuilder query = new WildcardQueryBuilder("path_field", "/company/engineering/*");
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest transformedRequest = processor.processRequest(request);

        assertThat(transformedRequest.routing(), notNullValue());
        String expectedRouting = computeExpectedRouting("company/engineering", "/");
        assertThat(transformedRequest.routing(), equalTo(expectedRouting));
    }

    public void testBoolQueryRouting() throws Exception {
        HierarchicalRoutingSearchProcessor processor = createProcessor("path_field", 2, "/", true);

        QueryBuilder pathFilter = new PrefixQueryBuilder("path_field", "/company/engineering");
        QueryBuilder textQuery = new TermQueryBuilder("content", "presentation");
        QueryBuilder boolQuery = new BoolQueryBuilder().must(textQuery).filter(pathFilter);

        SearchSourceBuilder source = new SearchSourceBuilder().query(boolQuery);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest transformedRequest = processor.processRequest(request);

        assertThat(transformedRequest.routing(), notNullValue());
        String expectedRouting = computeExpectedRouting("company/engineering", "/");
        assertThat(transformedRequest.routing(), equalTo(expectedRouting));
    }

    public void testTermsQueryRouting() throws Exception {
        HierarchicalRoutingSearchProcessor processor = createProcessor("path_field", 2, "/", true);

        QueryBuilder query = new TermsQueryBuilder(
            "path_field",
            "/company/engineering/team1/file.txt",
            "/company/engineering/team2/file.txt"
        );
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest transformedRequest = processor.processRequest(request);

        assertThat(transformedRequest.routing(), notNullValue());
        // Both paths should result in same routing since they share same anchor
        String expectedRouting = computeExpectedRouting("company/engineering", "/");
        assertThat(transformedRequest.routing(), equalTo(expectedRouting));
    }

    public void testDifferentAnchorDepths() throws Exception {
        String path = "/company/engineering/team/project/file.txt";

        // Test depth 1
        HierarchicalRoutingSearchProcessor processor1 = createProcessor("path_field", 1, "/", true);
        QueryBuilder query1 = new TermQueryBuilder("path_field", path);
        SearchRequest request1 = new SearchRequest().source(new SearchSourceBuilder().query(query1));
        SearchRequest result1 = processor1.processRequest(request1);

        // Test depth 2
        HierarchicalRoutingSearchProcessor processor2 = createProcessor("path_field", 2, "/", true);
        QueryBuilder query2 = new TermQueryBuilder("path_field", path);
        SearchRequest request2 = new SearchRequest().source(new SearchSourceBuilder().query(query2));
        SearchRequest result2 = processor2.processRequest(request2);

        // Different depths should produce different routing values
        assertThat(result1.routing(), notNullValue());
        assertThat(result2.routing(), notNullValue());
        assertNotEquals(result1.routing(), result2.routing());

        // Verify expected routing values
        assertThat(result1.routing(), equalTo(computeExpectedRouting("company", "/")));
        assertThat(result2.routing(), equalTo(computeExpectedRouting("company/engineering", "/")));
    }

    public void testCustomSeparator() throws Exception {
        HierarchicalRoutingSearchProcessor processor = createProcessor("path_field", 2, "\\", true);

        QueryBuilder query = new TermQueryBuilder("path_field", "company\\engineering\\team\\file.txt");
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest transformedRequest = processor.processRequest(request);

        assertThat(transformedRequest.routing(), notNullValue());
        String expectedRouting = computeExpectedRouting("company\\engineering", "\\");
        assertThat(transformedRequest.routing(), equalTo(expectedRouting));
    }

    public void testShouldAndMustNotClausesIgnored() throws Exception {
        HierarchicalRoutingSearchProcessor processor = createProcessor("path_field", 2, "/", true);

        // Query with paths only in should and must_not clauses
        QueryBuilder query = new BoolQueryBuilder().should(new PrefixQueryBuilder("path_field", "/company/engineering"))
            .mustNot(new PrefixQueryBuilder("path_field", "/company/marketing"));

        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest transformedRequest = processor.processRequest(request);

        // Should not add routing since paths are only in should/must_not clauses
        assertNull("Should not add routing for should/must_not clauses", transformedRequest.routing());
    }

    public void testNoPathFieldInQuery() throws Exception {
        HierarchicalRoutingSearchProcessor processor = createProcessor("path_field", 2, "/", true);

        QueryBuilder query = new TermQueryBuilder("content", "some text");
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest transformedRequest = processor.processRequest(request);

        // No routing should be added if no path field is found
        assertThat(transformedRequest.routing(), nullValue());
    }

    public void testExistingRoutingPreserved() throws Exception {
        HierarchicalRoutingSearchProcessor processor = createProcessor("path_field", 2, "/", true);

        QueryBuilder query = new TermQueryBuilder("path_field", "/company/engineering/team/file.txt");
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(source).routing("existing_routing");

        SearchRequest transformedRequest = processor.processRequest(request);

        // Existing routing should be preserved
        assertThat(transformedRequest.routing(), equalTo("existing_routing"));
    }

    public void testEmptySearchRequest() throws Exception {
        HierarchicalRoutingSearchProcessor processor = createProcessor("path_field", 2, "/", true);

        SearchRequest request = new SearchRequest();
        SearchRequest transformedRequest = processor.processRequest(request);

        // No routing should be added for empty request
        assertThat(transformedRequest.routing(), nullValue());
    }

    public void testWildcardPrefixExtraction() throws Exception {
        HierarchicalRoutingSearchProcessor processor = createProcessor("path_field", 2, "/", true);

        // Test various wildcard patterns
        String[][] testCases = {
            { "/company/engineering/*", "/company/engineering" },
            { "/company/engineering/team?", "/company/engineering/team" },
            { "/company/*", "/company" },
            { "*.txt", "" },
            { "/company/engineering/exact", "/company/engineering/exact" } };

        for (String[] testCase : testCases) {
            String pattern = testCase[0];
            String expectedPrefix = testCase[1];

            QueryBuilder query = new WildcardQueryBuilder("path_field", pattern);
            SearchSourceBuilder source = new SearchSourceBuilder().query(query);
            SearchRequest request = new SearchRequest().source(source);

            SearchRequest transformedRequest = processor.processRequest(request);

            if (expectedPrefix.isEmpty()) {
                assertThat("Pattern: " + pattern, transformedRequest.routing(), nullValue());
            } else {
                assertThat("Pattern: " + pattern, transformedRequest.routing(), notNullValue());
            }
        }
    }

    public void testMultiplePathsInQuery() throws Exception {
        HierarchicalRoutingSearchProcessor processor = createProcessor("path_field", 2, "/", true);

        // Query with multiple path filters - using filter clauses which do affect routing
        QueryBuilder query = new BoolQueryBuilder().filter(
            new TermsQueryBuilder("path_field", "/company/engineering/team1/file.txt", "/company/marketing/campaigns/q1.pdf")
        );

        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest transformedRequest = processor.processRequest(request);

        assertThat(transformedRequest.routing(), notNullValue());
        // Should contain routing for both paths (engineering and marketing have different anchors at depth 2)
        String routing = transformedRequest.routing();
        assertTrue("Should contain comma-separated routing values", routing.contains(","));
    }

    public void testFactory() throws Exception {
        HierarchicalRoutingSearchProcessor.Factory factory = new HierarchicalRoutingSearchProcessor.Factory();

        Map<String, Object> config = new HashMap<>();
        config.put("path_field", "file_path");
        config.put("anchor_depth", 3);
        config.put("path_separator", "\\");
        config.put("enable_auto_detection", false);

        HierarchicalRoutingSearchProcessor processor = factory.create(
            Collections.emptyMap(),
            "test",
            "test processor",
            false,
            config,
            null
        );

        assertThat(processor.getType(), equalTo(HierarchicalRoutingSearchProcessor.TYPE));
    }

    public void testFactoryValidation() throws Exception {
        HierarchicalRoutingSearchProcessor.Factory factory = new HierarchicalRoutingSearchProcessor.Factory();

        // Test missing path_field
        Map<String, Object> config1 = new HashMap<>();
        config1.put("anchor_depth", 2);
        OpenSearchParseException exception = expectThrows(
            OpenSearchParseException.class,
            () -> factory.create(Collections.emptyMap(), "test", null, false, config1, null)
        );
        assertThat(exception.getMessage(), containsString("path_field"));

        // Test invalid anchor_depth
        Map<String, Object> config2 = new HashMap<>();
        config2.put("path_field", "path");
        config2.put("anchor_depth", 0);
        exception = expectThrows(
            OpenSearchParseException.class,
            () -> factory.create(Collections.emptyMap(), "test", null, false, config2, null)
        );
        assertThat(exception.getMessage(), containsString("must be greater than 0"));

        // Test empty path_separator
        Map<String, Object> config3 = new HashMap<>();
        config3.put("path_field", "path");
        config3.put("path_separator", "");
        exception = expectThrows(
            OpenSearchParseException.class,
            () -> factory.create(Collections.emptyMap(), "test", null, false, config3, null)
        );
        assertThat(exception.getMessage(), containsString("cannot be null or empty"));
    }

    // Helper methods
    private HierarchicalRoutingSearchProcessor createProcessor(
        String pathField,
        int anchorDepth,
        String separator,
        boolean enableAutoDetection
    ) {
        return new HierarchicalRoutingSearchProcessor(
            "test",
            "test processor",
            false,
            pathField,
            anchorDepth,
            separator,
            enableAutoDetection
        );
    }

    private String computeExpectedRouting(String anchor, String separator) {
        // This mirrors the logic in HierarchicalRoutingSearchProcessor
        byte[] anchorBytes = anchor.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        long hash = org.opensearch.common.hash.MurmurHash3.hash128(
            anchorBytes,
            0,
            anchorBytes.length,
            0,
            new org.opensearch.common.hash.MurmurHash3.Hash128()
        ).h1;
        return String.valueOf(Math.abs(hash));
    }
}
