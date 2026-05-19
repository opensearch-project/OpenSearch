/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class AclRoutingProcessorIT extends OpenSearchIntegTestCase {

    private static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder().withoutPadding();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IngestCommonModulePlugin.class);
    }

    public void testAclRoutingProcessor() throws Exception {
        // Create ingest pipeline with ACL routing processor
        String pipelineId = "acl-routing-test";
        BytesReference pipelineConfig = BytesReference.bytes(
            jsonBuilder().startObject()
                .startArray("processors")
                .startObject()
                .startObject("acl_routing")
                .field("acl_field", "team")
                .field("target_field", "_routing")
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );

        client().admin().cluster().preparePutPipeline(pipelineId, pipelineConfig, MediaTypeRegistry.JSON).get();

        // Create index with multiple shards - don't set default pipeline, use explicit pipeline parameter
        String indexName = "test-acl-routing";
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName).settings(
            Settings.builder().put("number_of_shards", 3).put("number_of_replicas", 0).build()
        )
            .mapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("team")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("content")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
            );

        client().admin().indices().create(createIndexRequest).get();

        // Index documents with explicit pipeline parameter
        client().index(
            new IndexRequest(indexName).id("1")
                .source(jsonBuilder().startObject().field("team", "team-alpha").field("content", "Alpha content 1").endObject())
                .setPipeline(pipelineId)
        ).get();

        client().index(
            new IndexRequest(indexName).id("2")
                .source(jsonBuilder().startObject().field("team", "team-alpha").field("content", "Alpha content 2").endObject())
                .setPipeline(pipelineId)
        ).get();

        client().index(
            new IndexRequest(indexName).id("3")
                .source(jsonBuilder().startObject().field("team", "team-beta").field("content", "Beta content").endObject())
                .setPipeline(pipelineId)
        ).get();

        // Refresh to make documents searchable
        client().admin().indices().prepareRefresh(indexName).get();

        // Test search functionality - documents should be searchable
        SearchResponse searchResponse = client().prepareSearch(indexName)
            .setSource(new SearchSourceBuilder().query(new TermQueryBuilder("team", "team-alpha")))
            .get();

        assertThat("Should find alpha team documents", searchResponse.getHits().getTotalHits().value(), equalTo(2L));

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            String team = (String) hit.getSourceAsMap().get("team");
            assertEquals("Found document should be from team alpha", "team-alpha", team);
        }
    }

    public void testAclRoutingWithIgnoreMissing() throws Exception {
        // Create pipeline with ignore_missing = true
        String pipelineId = "acl-routing-ignore-missing";
        BytesReference pipelineConfig = BytesReference.bytes(
            jsonBuilder().startObject()
                .startArray("processors")
                .startObject()
                .startObject("acl_routing")
                .field("acl_field", "nonexistent_field")
                .field("target_field", "_routing")
                .field("ignore_missing", true)
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );

        client().admin().cluster().preparePutPipeline(pipelineId, pipelineConfig, MediaTypeRegistry.JSON).get();

        String indexName = "test-ignore-missing";
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName).settings(
            Settings.builder().put("number_of_shards", 2).put("number_of_replicas", 0).put("index.default_pipeline", pipelineId).build()
        );

        client().admin().indices().create(createIndexRequest).get();

        // Index document without the ACL field
        IndexRequest indexRequest = new IndexRequest(indexName).id("missing1")
            .source(
                jsonBuilder().startObject().field("other_field", "some value").field("content", "Document without ACL field").endObject()
            )
            .setPipeline(pipelineId);

        client().index(indexRequest).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // Document should be indexed without routing since field was missing and ignored
        GetResponse doc = client().prepareGet(indexName, "missing1").get();
        assertTrue("Document should be indexed even with missing ACL field", doc.isExists());
    }

    public void testAclRoutingWithCustomTargetField() throws Exception {
        // Create pipeline with custom target field
        String pipelineId = "acl-routing-custom-target";
        BytesReference pipelineConfig = BytesReference.bytes(
            jsonBuilder().startObject()
                .startArray("processors")
                .startObject()
                .startObject("acl_routing")
                .field("acl_field", "department")
                .field("target_field", "custom_routing")
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );

        client().admin().cluster().preparePutPipeline(pipelineId, pipelineConfig, MediaTypeRegistry.JSON).get();

        String indexName = "test-custom-target";
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName).settings(
            Settings.builder().put("number_of_shards", 2).put("number_of_replicas", 0).put("index.default_pipeline", pipelineId).build()
        );

        client().admin().indices().create(createIndexRequest).get();

        // Index document
        IndexRequest indexRequest = new IndexRequest(indexName).id("custom1")
            .source(jsonBuilder().startObject().field("department", "engineering").field("content", "Engineering document").endObject())
            .setPipeline(pipelineId);

        client().index(indexRequest).get();
        client().admin().indices().prepareRefresh(indexName).get();

        GetResponse doc = client().prepareGet(indexName, "custom1").get();
        assertTrue("Document should exist", doc.isExists());

        // Check that custom routing field was set
        Map<String, Object> source = doc.getSource();
        assertNotNull("Custom routing field should be set", source.get("custom_routing"));
        assertEquals("Custom routing should match expected value", generateRoutingValue("engineering"), source.get("custom_routing"));
    }

    public void testAclRoutingProcessorRegistration() throws Exception {
        // Verify processor is registered by attempting to create a pipeline
        String pipelineId = "test-acl-processor-registration";
        BytesReference pipelineConfig = BytesReference.bytes(
            jsonBuilder().startObject()
                .startArray("processors")
                .startObject()
                .startObject("acl_routing")
                .field("acl_field", "team")
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

    // Helper method to generate routing value (mirrors processor logic)
    private String generateRoutingValue(String aclValue) {
        // Use MurmurHash3 for consistent hashing (same as processor)
        byte[] bytes = aclValue.getBytes(StandardCharsets.UTF_8);
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(bytes, 0, bytes.length, 0, new MurmurHash3.Hash128());

        // Convert to base64 for routing value
        byte[] hashBytes = new byte[16];
        System.arraycopy(longToBytes(hash.h1), 0, hashBytes, 0, 8);
        System.arraycopy(longToBytes(hash.h2), 0, hashBytes, 8, 8);

        return BASE64_ENCODER.encodeToString(hashBytes);
    }

    private byte[] longToBytes(long value) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte) (value & 0xFF);
            value >>= 8;
        }
        return result;
    }
}
