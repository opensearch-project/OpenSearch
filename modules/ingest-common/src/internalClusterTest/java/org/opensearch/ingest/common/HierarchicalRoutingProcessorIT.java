/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class HierarchicalRoutingProcessorIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IngestCommonModulePlugin.class);
    }

    public void testHierarchicalRoutingProcessor() throws Exception {
        // Create ingest pipeline with hierarchical routing processor
        String pipelineId = "hierarchical-routing-test";
        BytesReference pipelineConfig = BytesReference.bytes(
            jsonBuilder().startObject()
                .startArray("processors")
                .startObject()
                .startObject("hierarchical_routing")
                .field("path_field", "file_path")
                .field("anchor_depth", 2)
                .field("path_separator", "/")
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );

        client().admin().cluster().preparePutPipeline(pipelineId, pipelineConfig, MediaTypeRegistry.JSON).get();

        // Create index with multiple shards
        String indexName = "test-hierarchical-routing";
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName).settings(
            Map.of("number_of_shards", 3, "number_of_replicas", 0, "index.default_pipeline", pipelineId)
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

        // Index documents with same hierarchical prefix
        BulkRequest bulkRequest = new BulkRequest();

        // Engineering documents (should get same routing)
        bulkRequest.add(
            new IndexRequest(indexName).id("1")
                .source(
                    jsonBuilder().startObject()
                        .field("file_path", "/company/engineering/backend/api.pdf")
                        .field("content", "API documentation")
                        .endObject()
                )
        );

        bulkRequest.add(
            new IndexRequest(indexName).id("2")
                .source(
                    jsonBuilder().startObject()
                        .field("file_path", "/company/engineering/frontend/ui.pdf")
                        .field("content", "UI guidelines")
                        .endObject()
                )
        );

        // Marketing documents (should get different routing)
        bulkRequest.add(
            new IndexRequest(indexName).id("3")
                .source(
                    jsonBuilder().startObject()
                        .field("file_path", "/company/marketing/campaigns/q1.pdf")
                        .field("content", "Q1 campaign")
                        .endObject()
                )
        );

        BulkResponse bulkResponse = client().bulk(bulkRequest).get();
        assertFalse("Bulk indexing should succeed", bulkResponse.hasFailures());

        // Refresh to make documents searchable
        client().admin().indices().prepareRefresh(indexName).get();

        // Verify documents were routed correctly
        // We need to calculate the expected routing values to retrieve the documents
        String engineeringRouting = computeRouting("company/engineering");
        String marketingRouting = computeRouting("company/marketing");

        GetResponse doc1 = client().prepareGet(indexName, "1").setRouting(engineeringRouting).get();
        GetResponse doc2 = client().prepareGet(indexName, "2").setRouting(engineeringRouting).get();
        GetResponse doc3 = client().prepareGet(indexName, "3").setRouting(marketingRouting).get();

        assertTrue("Document 1 should exist", doc1.isExists());
        assertTrue("Document 2 should exist", doc2.isExists());
        assertTrue("Document 3 should exist", doc3.isExists());

        // Check that routing was applied
        DocumentField doc1Routing = doc1.getField("_routing");
        DocumentField doc2Routing = doc2.getField("_routing");
        DocumentField doc3Routing = doc3.getField("_routing");

        assertThat("Document 1 should have routing", doc1Routing, notNullValue());
        assertThat("Document 2 should have routing", doc2Routing, notNullValue());
        assertThat("Document 3 should have routing", doc3Routing, notNullValue());

        // Documents 1 and 2 should have same routing (same anchor: /company/engineering)
        assertThat("Documents with same anchor should have same routing", doc1Routing.getValue(), equalTo(doc2Routing.getValue()));

        // Document 3 should have different routing (different anchor: /company/marketing)
        assertNotEquals("Documents with different anchors should have different routing", doc1Routing.getValue(), doc3Routing.getValue());

        // Test search functionality
        SearchResponse searchResponse = client().prepareSearch(indexName)
            .setSource(new SearchSourceBuilder().query(new PrefixQueryBuilder("file_path", "/company/engineering")))
            .get();

        assertThat("Should find engineering documents", searchResponse.getHits().getTotalHits().value(), equalTo(2L));

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            String filePath = (String) hit.getSourceAsMap().get("file_path");
            assertTrue("Found document should be in engineering folder", filePath.startsWith("/company/engineering"));
        }
    }

    public void testHierarchicalRoutingWithCustomSeparator() throws Exception {
        // Create pipeline with custom separator
        String pipelineId = "hierarchical-routing-custom-sep";
        BytesReference pipelineConfig = BytesReference.bytes(
            jsonBuilder().startObject()
                .startArray("processors")
                .startObject()
                .startObject("hierarchical_routing")
                .field("path_field", "windows_path")
                .field("anchor_depth", 2)
                .field("path_separator", "\\")
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );

        client().admin().cluster().preparePutPipeline(pipelineId, pipelineConfig, MediaTypeRegistry.JSON).get();

        String indexName = "test-custom-separator";
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName).settings(
            Map.of("number_of_shards", 2, "number_of_replicas", 0, "index.default_pipeline", pipelineId)
        );

        client().admin().indices().create(createIndexRequest).get();

        // Index document with Windows-style path
        IndexRequest indexRequest = new IndexRequest(indexName).id("win1")
            .source(
                jsonBuilder().startObject()
                    .field("windows_path", "C:\\Users\\admin\\Documents\\file.txt")
                    .field("content", "Windows document")
                    .endObject()
            );

        client().index(indexRequest).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // Calculate expected routing for Windows path
        String windowsRouting = computeRouting("C:\\Users", "\\");

        GetResponse doc = client().prepareGet(indexName, "win1").setRouting(windowsRouting).get();
        assertTrue("Document should exist", doc.isExists());
        DocumentField routing = doc.getField("_routing");
        assertThat("Document should have routing", routing, notNullValue());
    }

    public void testHierarchicalRoutingWithMissingField() throws Exception {
        // Create pipeline with ignore_missing = true
        String pipelineId = "hierarchical-routing-ignore-missing";
        BytesReference pipelineConfig = BytesReference.bytes(
            jsonBuilder().startObject()
                .startArray("processors")
                .startObject()
                .startObject("hierarchical_routing")
                .field("path_field", "nonexistent_field")
                .field("anchor_depth", 2)
                .field("ignore_missing", true)
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );

        client().admin().cluster().preparePutPipeline(pipelineId, pipelineConfig, MediaTypeRegistry.JSON).get();

        String indexName = "test-ignore-missing";
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName).settings(
            Map.of("number_of_shards", 2, "number_of_replicas", 0, "index.default_pipeline", pipelineId)
        );

        client().admin().indices().create(createIndexRequest).get();

        // Index document without the required field
        IndexRequest indexRequest = new IndexRequest(indexName).id("missing1")
            .source(
                jsonBuilder().startObject().field("other_field", "some value").field("content", "Document without path field").endObject()
            );

        client().index(indexRequest).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // Document without path field should not have routing, so no routing needed for get
        GetResponse doc = client().prepareGet(indexName, "missing1").get();
        assertTrue("Document should be indexed even with missing field", doc.isExists());
        // Routing should be null since field was missing and ignored
    }

    public void testHierarchicalRoutingProcessorRegistration() throws Exception {
        // Verify processor is registered by attempting to create a pipeline
        String pipelineId = "test-processor-registration";
        BytesReference pipelineConfig = BytesReference.bytes(
            jsonBuilder().startObject()
                .startArray("processors")
                .startObject()
                .startObject("hierarchical_routing")
                .field("path_field", "path")
                .field("anchor_depth", 1)
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );

        // This should succeed if processor is properly registered
        client().admin().cluster().preparePutPipeline(pipelineId, pipelineConfig, MediaTypeRegistry.JSON).get();

        // Verify pipeline was created
        var getPipelineResponse = client().admin().cluster().prepareGetPipeline(pipelineId).get();
        assertTrue("Pipeline should be created successfully", getPipelineResponse.isFound());

        // Clean up
        client().admin().cluster().prepareDeletePipeline(pipelineId).get();
    }

    // Helper method to compute expected routing (mirrors processor logic)
    private String computeRouting(String anchor) {
        return computeRouting(anchor, "/");
    }

    private String computeRouting(String anchor, String separator) {
        // This mirrors the logic in HierarchicalRoutingProcessor
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
