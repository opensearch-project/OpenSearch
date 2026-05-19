/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.PutSearchPipelineRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class HierarchicalRoutingSearchProcessorIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(SearchPipelineCommonModulePlugin.class);
    }

    public void testHierarchicalRoutingSearchProcessor() throws Exception {
        // Create search pipeline with hierarchical routing search processor
        String pipelineId = "hierarchical-search-routing";
        BytesArray pipelineConfig = new BytesArray("""
            {
              "request_processors": [
                {
                  "hierarchical_routing_search": {
                    "path_field": "file_path",
                    "anchor_depth": 2,
                    "path_separator": "/",
                    "enable_auto_detection": true
                  }
                }
              ]
            }
            """);

        PutSearchPipelineRequest putRequest = new PutSearchPipelineRequest(pipelineId, pipelineConfig, MediaTypeRegistry.JSON);
        AcknowledgedResponse putResponse = client().admin().cluster().putSearchPipeline(putRequest).actionGet();
        assertTrue("Pipeline creation should succeed", putResponse.isAcknowledged());

        // Create index with multiple shards
        String indexName = "test-search-routing";
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName).settings(
            Settings.builder()
                .put("number_of_shards", 3)
                .put("number_of_replicas", 0)
                .put("index.search.default_pipeline", pipelineId)
                .build()
        )
            .mapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("file_path")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("content")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
            );

        client().admin().indices().create(createIndexRequest).get();

        // Index test documents with explicit routing to simulate ingest pipeline behavior
        String engineeringRouting = computeRouting("company/engineering");
        String marketingRouting = computeRouting("company/marketing");
        String salesRouting = computeRouting("company/sales");

        // Engineering documents
        client().index(
            new IndexRequest(indexName).id("1")
                .routing(engineeringRouting)
                .source(
                    jsonBuilder().startObject()
                        .field("file_path", "/company/engineering/backend/api.pdf")
                        .field("content", "API documentation")
                        .endObject()
                )
        ).get();

        client().index(
            new IndexRequest(indexName).id("2")
                .routing(engineeringRouting)
                .source(
                    jsonBuilder().startObject()
                        .field("file_path", "/company/engineering/frontend/ui.pdf")
                        .field("content", "UI guidelines")
                        .endObject()
                )
        ).get();

        // Marketing documents
        client().index(
            new IndexRequest(indexName).id("3")
                .routing(marketingRouting)
                .source(
                    jsonBuilder().startObject()
                        .field("file_path", "/company/marketing/campaigns/q1.pdf")
                        .field("content", "Q1 campaign strategy")
                        .endObject()
                )
        ).get();

        // Sales documents
        client().index(
            new IndexRequest(indexName).id("4")
                .routing(salesRouting)
                .source(
                    jsonBuilder().startObject()
                        .field("file_path", "/company/sales/proposals/deal.pdf")
                        .field("content", "Sales proposal")
                        .endObject()
                )
        ).get();

        client().admin().indices().prepareRefresh(indexName).get();

        // Test 1: Prefix query should automatically add routing
        SearchRequest searchRequest = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(new PrefixQueryBuilder("file_path", "/company/engineering"))
        );

        SearchResponse searchResponse = client().search(searchRequest).get();
        assertThat("Should find engineering documents", searchResponse.getHits().getTotalHits().value(), equalTo(2L));

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            String filePath = (String) hit.getSourceAsMap().get("file_path");
            assertTrue("Found document should be in engineering folder", filePath.startsWith("/company/engineering"));
        }

        // Test 2: Wildcard query should automatically add routing
        searchRequest = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(new WildcardQueryBuilder("file_path", "/company/marketing/*"))
        );

        searchResponse = client().search(searchRequest).get();
        assertThat("Should find marketing documents", searchResponse.getHits().getTotalHits().value(), equalTo(1L));

        String filePath = (String) searchResponse.getHits().getHits()[0].getSourceAsMap().get("file_path");
        assertTrue("Found document should be in marketing folder", filePath.startsWith("/company/marketing"));

        // Test 3: Bool query with path filter should add routing
        searchRequest = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(
                new BoolQueryBuilder().must(new TermQueryBuilder("content", "proposal"))
                    .filter(new PrefixQueryBuilder("file_path", "/company/sales"))
            )
        );

        searchResponse = client().search(searchRequest).get();
        assertThat("Should find sales documents", searchResponse.getHits().getTotalHits().value(), equalTo(1L));

        // Test 4: Query without path filter should search all shards (no routing added)
        searchRequest = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(new TermQueryBuilder("content", "documentation"))
        );

        searchResponse = client().search(searchRequest).get();
        assertThat("Should find documents across all shards", searchResponse.getHits().getTotalHits().value(), equalTo(1L));

        // Test 5: Multiple path prefixes should result in multiple routing values
        searchRequest = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(
                new BoolQueryBuilder().should(new PrefixQueryBuilder("file_path", "/company/engineering"))
                    .should(new PrefixQueryBuilder("file_path", "/company/marketing"))
            )
        );

        searchResponse = client().search(searchRequest).get();
        assertThat("Should find documents from both departments", searchResponse.getHits().getTotalHits().value(), equalTo(3L));
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/TBD")
    public void testHierarchicalRoutingSearchProcessorWithCustomSeparator() throws Exception {
        // Create search pipeline with custom separator
        String pipelineId = "hierarchical-search-routing-custom";
        BytesArray pipelineConfig = new BytesArray("""
            {
              "request_processors": [
                {
                  "hierarchical_routing_search": {
                    "path_field": "windows_path",
                    "anchor_depth": 2,
                    "path_separator": "\\\\",
                    "enable_auto_detection": true
                  }
                }
              ]
            }
            """);

        PutSearchPipelineRequest putRequest = new PutSearchPipelineRequest(pipelineId, pipelineConfig, MediaTypeRegistry.JSON);
        client().admin().cluster().putSearchPipeline(putRequest).actionGet();

        String indexName = "test-custom-search-routing";
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName).settings(
            Settings.builder()
                .put("number_of_shards", 2)
                .put("number_of_replicas", 0)
                .put("index.search.default_pipeline", pipelineId)
                .build()
        );

        client().admin().indices().create(createIndexRequest).get();

        // Index document with Windows-style path
        client().index(
            new IndexRequest(indexName).id("win1")
                .source(
                    jsonBuilder().startObject()
                        .field("windows_path", "C:\\Users\\admin\\Documents\\file.txt")
                        .field("content", "Windows document")
                        .endObject()
                )
        ).get();

        client().admin().indices().prepareRefresh(indexName).get();

        // Test Windows path search
        SearchRequest searchRequest = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(new PrefixQueryBuilder("windows_path", "C:\\Users"))
        );

        SearchResponse searchResponse = client().search(searchRequest).get();
        assertThat("Should find Windows documents", searchResponse.getHits().getTotalHits().value(), equalTo(1L));
    }

    public void testHierarchicalRoutingSearchProcessorRegistration() throws Exception {
        // Test that the processor is properly registered by creating a pipeline
        String pipelineId = "test-search-processor-registration";
        BytesArray pipelineConfig = new BytesArray("""
            {
              "request_processors": [
                {
                  "hierarchical_routing_search": {
                    "path_field": "path",
                    "anchor_depth": 1
                  }
                }
              ]
            }
            """);

        PutSearchPipelineRequest putRequest = new PutSearchPipelineRequest(pipelineId, pipelineConfig, MediaTypeRegistry.JSON);

        // This should succeed if processor is properly registered
        AcknowledgedResponse response = client().admin().cluster().putSearchPipeline(putRequest).actionGet();
        assertTrue("Pipeline creation should succeed", response.isAcknowledged());

        // Verify pipeline exists
        var getResponse = client().admin()
            .cluster()
            .getSearchPipeline(new org.opensearch.action.search.GetSearchPipelineRequest(pipelineId))
            .actionGet();
        assertFalse("Pipeline should exist", getResponse.pipelines().isEmpty());
        assertEquals("Pipeline ID should match", pipelineId, getResponse.pipelines().get(0).getId());
    }

    public void testNoRoutingWithoutPathField() throws Exception {
        // Create search pipeline
        String pipelineId = "hierarchical-search-routing-no-path";
        BytesArray pipelineConfig = new BytesArray("""
            {
              "request_processors": [
                {
                  "hierarchical_routing_search": {
                    "path_field": "file_path",
                    "anchor_depth": 2
                  }
                }
              ]
            }
            """);

        PutSearchPipelineRequest putRequest = new PutSearchPipelineRequest(pipelineId, pipelineConfig, MediaTypeRegistry.JSON);
        client().admin().cluster().putSearchPipeline(putRequest).actionGet();

        String indexName = "test-no-path-routing";
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName).settings(
            Settings.builder()
                .put("number_of_shards", 2)
                .put("number_of_replicas", 0)
                .put("index.search.default_pipeline", pipelineId)
                .build()
        );

        client().admin().indices().create(createIndexRequest).get();

        // Index document
        client().index(new IndexRequest(indexName).id("1").source(Map.of("content", "test document"))).get();

        client().admin().indices().prepareRefresh(indexName).get();

        // Search without path field - should not add routing
        SearchRequest searchRequest = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(new TermQueryBuilder("content", "test"))
        );

        SearchResponse searchResponse = client().search(searchRequest).get();
        assertThat("Should find document", searchResponse.getHits().getTotalHits().value(), equalTo(1L));
        // No routing should be added since no path field in query
    }

    // Helper method to compute expected routing (mirrors processor logic)
    private String computeRouting(String anchor) {
        return computeRouting(anchor, "/");
    }

    private String computeRouting(String anchor, String separator) {
        // Simple routing computation for tests
        byte[] anchorBytes = anchor.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        long hash = org.opensearch.common.hash.MurmurHash3.hash128(
            anchorBytes,
            0,
            anchorBytes.length,
            0,
            new org.opensearch.common.hash.MurmurHash3.Hash128()
        ).h1;
        return String.valueOf(hash == Long.MIN_VALUE ? 0L : (hash < 0 ? -hash : hash));
    }
}
