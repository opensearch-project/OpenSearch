/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class AclRoutingSearchProcessorIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(SearchPipelineCommonModulePlugin.class);
    }

    @Override
    protected int numberOfShards() {
        return 3;
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    public void testSearchProcessorExtractsRouting() throws Exception {
        String indexName = "test-search-acl-routing";
        
        // Create index
        assertAcked(prepareCreate(indexName).setSettings(Settings.builder().put("number_of_shards", 3)));
        
        // Create search pipeline
        String pipelineId = "acl-search-pipeline";
        client().admin().cluster().preparePutSearchPipeline(pipelineId,
            "{\n" +
            "  \"request_processors\": [\n" +
            "    {\n" +
            "      \"acl_routing_search\": {\n" +
            "        \"acl_field\": \"team\",\n" +
            "        \"extract_from_query\": true\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}", XContentType.JSON).get();
        
        // Index some documents with routing
        String team1 = "team-alpha";
        String team2 = "team-beta";
        
        for (int i = 0; i < 3; i++) {
            Map<String, Object> doc1 = new HashMap<>();
            doc1.put("team", team1);
            doc1.put("content", "alpha document " + i);
            
            IndexRequest indexRequest1 = new IndexRequest(indexName)
                .id("alpha-doc-" + i)
                .source(doc1)
                .routing(team1);
            
            client().index(indexRequest1).get();
            
            Map<String, Object> doc2 = new HashMap<>();
            doc2.put("team", team2);
            doc2.put("content", "beta document " + i);
            
            IndexRequest indexRequest2 = new IndexRequest(indexName)
                .id("beta-doc-" + i)
                .source(doc2)
                .routing(team2);
            
            client().index(indexRequest2).get();
        }
        
        client().admin().indices().prepareRefresh(indexName).get();
        
        // Search with team filter using search pipeline
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(new SearchSourceBuilder().query(
            QueryBuilders.termQuery("team", team1)
        ));
        searchRequest.pipeline(pipelineId);
        
        SearchResponse searchResponse = client().search(searchRequest).get();
        assertHitCount(searchResponse, 3);
        
        // Verify we get the right documents
        for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
            Map<String, Object> source = searchResponse.getHits().getAt(i).getSourceAsMap();
            assertEquals(team1, source.get("team"));
        }
    }

    public void testSearchProcessorWithBoolQuery() throws Exception {
        String indexName = "test-search-bool-query";
        
        assertAcked(prepareCreate(indexName).setSettings(Settings.builder().put("number_of_shards", 2)));
        
        String pipelineId = "acl-bool-search-pipeline";
        client().admin().cluster().preparePutSearchPipeline(pipelineId,
            "{\n" +
            "  \"request_processors\": [\n" +
            "    {\n" +
            "      \"acl_routing_search\": {\n" +
            "        \"acl_field\": \"department\",\n" +
            "        \"extract_from_query\": true\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}", XContentType.JSON).get();
        
        // Index documents
        String dept = "engineering";
        for (int i = 0; i < 2; i++) {
            Map<String, Object> doc = new HashMap<>();
            doc.put("department", dept);
            doc.put("title", "Engineer " + i);
            
            IndexRequest indexRequest = new IndexRequest(indexName)
                .id("eng-doc-" + i)
                .source(doc)
                .routing(dept);
            
            client().index(indexRequest).get();
        }
        
        client().admin().indices().prepareRefresh(indexName).get();
        
        // Search with bool query
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("department", dept))
                .filter(QueryBuilders.existsQuery("title"))
        ));
        searchRequest.pipeline(pipelineId);
        
        SearchResponse searchResponse = client().search(searchRequest).get();
        assertHitCount(searchResponse, 2);
    }

    public void testSearchProcessorWithoutAclInQuery() throws Exception {
        String indexName = "test-search-no-acl";
        
        assertAcked(prepareCreate(indexName).setSettings(Settings.builder().put("number_of_shards", 2)));
        
        String pipelineId = "acl-no-match-pipeline";
        client().admin().cluster().preparePutSearchPipeline(pipelineId,
            "{\n" +
            "  \"request_processors\": [\n" +
            "    {\n" +
            "      \"acl_routing_search\": {\n" +
            "        \"acl_field\": \"team\",\n" +
            "        \"extract_from_query\": true\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}", XContentType.JSON).get();
        
        // Index a document
        Map<String, Object> doc = new HashMap<>();
        doc.put("content", "some content");
        
        IndexRequest indexRequest = new IndexRequest(indexName)
            .id("doc-1")
            .source(doc);
        
        client().index(indexRequest).get();
        client().admin().indices().prepareRefresh(indexName).get();
        
        // Search without team filter
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(new SearchSourceBuilder().query(
            QueryBuilders.matchAllQuery()
        ));
        searchRequest.pipeline(pipelineId);
        
        SearchResponse searchResponse = client().search(searchRequest).get();
        assertHitCount(searchResponse, 1);
    }

    public void testSearchProcessorDisabled() throws Exception {
        String indexName = "test-search-disabled";
        
        assertAcked(prepareCreate(indexName).setSettings(Settings.builder().put("number_of_shards", 2)));
        
        String pipelineId = "acl-disabled-pipeline";
        client().admin().cluster().preparePutSearchPipeline(pipelineId,
            "{\n" +
            "  \"request_processors\": [\n" +
            "    {\n" +
            "      \"acl_routing_search\": {\n" +
            "        \"acl_field\": \"team\",\n" +
            "        \"extract_from_query\": false\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}", XContentType.JSON).get();
        
        // Index a document
        Map<String, Object> doc = new HashMap<>();
        doc.put("team", "engineering");
        doc.put("content", "engineering content");
        
        IndexRequest indexRequest = new IndexRequest(indexName)
            .id("doc-1")
            .source(doc);
        
        client().index(indexRequest).get();
        client().admin().indices().prepareRefresh(indexName).get();
        
        // Search with team filter but extraction disabled
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(new SearchSourceBuilder().query(
            QueryBuilders.termQuery("team", "engineering")
        ));
        searchRequest.pipeline(pipelineId);
        
        SearchResponse searchResponse = client().search(searchRequest).get();
        assertHitCount(searchResponse, 1);
    }
}