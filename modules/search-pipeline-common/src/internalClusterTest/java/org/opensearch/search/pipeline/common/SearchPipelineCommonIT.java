/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.DeleteSearchPipelineRequest;
import org.opensearch.action.search.GetSearchPipelineRequest;
import org.opensearch.action.search.GetSearchPipelineResponse;
import org.opensearch.action.search.PutSearchPipelineRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.ingest.PipelineConfiguration;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class SearchPipelineCommonIT extends OpenSearchIntegTestCase {

    private static final String TEST_INDEX = "myindex";
    private static final String PIPELINE_NAME = "test_pipeline";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(SearchPipelineCommonModulePlugin.class);
    }

    @Before
    public void setup() throws Exception {
        createIndex(TEST_INDEX);

        IndexRequest doc1 = new IndexRequest(TEST_INDEX).id("doc1").source(Map.of("field", "value"));
        IndexRequest doc2 = new IndexRequest(TEST_INDEX).id("doc2").source(Map.of("field", "something else"));

        IndexResponse ir = client().index(doc1).actionGet();
        assertSame(RestStatus.CREATED, ir.status());
        ir = client().index(doc2).actionGet();
        assertSame(RestStatus.CREATED, ir.status());

        RefreshResponse refRsp = client().admin().indices().refresh(new RefreshRequest(TEST_INDEX)).actionGet();
        assertSame(RestStatus.OK, refRsp.getStatus());
    }

    @After
    public void cleanup() throws Exception {
        internalCluster().wipeIndices(TEST_INDEX);
    }

    public void testFilterQuery() {
        // Create a pipeline with a filter_query processor.
        createPipeline();

        // Search without the pipeline. Should see both documents.
        SearchRequest req = new SearchRequest(TEST_INDEX).source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));
        SearchResponse rsp = client().search(req).actionGet();
        assertEquals(2, rsp.getHits().getTotalHits().value);

        // Search with the pipeline. Should only see document with "field":"value".
        req.pipeline(PIPELINE_NAME);
        rsp = client().search(req).actionGet();
        assertEquals(1, rsp.getHits().getTotalHits().value);

        // Clean up.
        deletePipeline();
    }

    public void testSearchWithTemporaryPipeline() throws Exception {

        // Search without the pipeline. Should see both documents.
        SearchRequest req = new SearchRequest(TEST_INDEX).source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));
        SearchResponse rsp = client().search(req).actionGet();
        assertEquals(2, rsp.getHits().getTotalHits().value);

        // Search with temporary pipeline
        Map<String, Object> pipelineSourceMap = new HashMap<>();
        Map<String, Object> requestProcessorConfig = new HashMap<>();

        Map<String, Object> filterQuery = new HashMap<>();
        filterQuery.put("query", Map.of("term", Map.of("field", "value")));
        requestProcessorConfig.put("filter_query", filterQuery);
        pipelineSourceMap.put("request_processors", List.of(requestProcessorConfig));

        req = new SearchRequest(TEST_INDEX).source(
            new SearchSourceBuilder().query(new MatchAllQueryBuilder()).searchPipelineSource(pipelineSourceMap)
        );

        SearchResponse rspWithTempPipeline = client().search(req).actionGet();
        assertEquals(1, rspWithTempPipeline.getHits().getTotalHits().value);
    }

    public void testSearchWithDefaultPipeline() throws Exception {
        // Create pipeline
        createPipeline();

        // Search without the pipeline. Should see both documents.
        SearchRequest req = new SearchRequest(TEST_INDEX).source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));
        SearchResponse rsp = client().search(req).actionGet();
        assertEquals(2, rsp.getHits().getTotalHits().value);

        // Set pipeline as default for the index
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(TEST_INDEX);
        updateSettingsRequest.settings(Settings.builder().put("index.search.default_pipeline", PIPELINE_NAME));
        AcknowledgedResponse updateSettingsResponse = client().admin().indices().updateSettings(updateSettingsRequest).actionGet();
        assertTrue(updateSettingsResponse.isAcknowledged());

        // Search with the default pipeline. Should only see document with "field":"value".
        rsp = client().search(req).actionGet();
        assertEquals(1, rsp.getHits().getTotalHits().value);

        // Clean up: Remove default pipeline setting
        updateSettingsRequest = new UpdateSettingsRequest(TEST_INDEX);
        updateSettingsRequest.settings(Settings.builder().putNull("index.search.default_pipeline"));
        updateSettingsResponse = client().admin().indices().updateSettings(updateSettingsRequest).actionGet();
        assertTrue(updateSettingsResponse.isAcknowledged());

        // Clean up.
        deletePipeline();
    }

    public void testUpdateSearchPipeline() throws Exception {
        // Create initial pipeline
        createPipeline();

        // Verify initial pipeline
        SearchRequest req = new SearchRequest(TEST_INDEX).source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));
        req.pipeline(PIPELINE_NAME);
        SearchResponse initialRsp = client().search(req).actionGet();
        assertEquals(1, initialRsp.getHits().getTotalHits().value);

        BytesReference pipelineConfig = new BytesArray(
            "{"
                + "\"description\": \"Updated pipeline\","
                + "\"request_processors\": ["
                + "{"
                + "\"filter_query\" : {"
                + "\"query\": {"
                + "\"term\" : {"
                + "\"field\" : \"something else\""
                + "}"
                + "}"
                + "}"
                + "}"
                + "]"
                + "}"
        );

        PipelineConfiguration pipeline = new PipelineConfiguration(PIPELINE_NAME, pipelineConfig, MediaTypeRegistry.JSON);

        // Update pipeline
        PutSearchPipelineRequest updateRequest = new PutSearchPipelineRequest(pipeline.getId(), pipelineConfig, MediaTypeRegistry.JSON);
        AcknowledgedResponse ackRsp = client().admin().cluster().putSearchPipeline(updateRequest).actionGet();
        assertTrue(ackRsp.isAcknowledged());

        // Verify pipeline description
        GetSearchPipelineResponse getPipelineResponse = client().admin()
            .cluster()
            .getSearchPipeline(new GetSearchPipelineRequest(PIPELINE_NAME))
            .actionGet();
        assertEquals(PIPELINE_NAME, getPipelineResponse.pipelines().get(0).getId());
        assertEquals(pipeline.getConfigAsMap(), getPipelineResponse.pipelines().get(0).getConfigAsMap());
        // Clean up.
        deletePipeline();
    }

    private void createPipeline() {
        PutSearchPipelineRequest putSearchPipelineRequest = new PutSearchPipelineRequest(
            PIPELINE_NAME,
            new BytesArray(
                "{"
                    + "\"request_processors\": ["
                    + "{"
                    + "\"filter_query\" : {"
                    + "\"query\": {"
                    + "\"term\" : {"
                    + "\"field\" : \"value\""
                    + "}"
                    + "}"
                    + "}"
                    + "}"
                    + "]"
                    + "}"
            ),
            MediaTypeRegistry.JSON
        );
        AcknowledgedResponse ackRsp = client().admin().cluster().putSearchPipeline(putSearchPipelineRequest).actionGet();
        assertTrue(ackRsp.isAcknowledged());
    }

    private void deletePipeline() {
        AcknowledgedResponse ackRsp = client().admin()
            .cluster()
            .deleteSearchPipeline(new DeleteSearchPipelineRequest(PIPELINE_NAME))
            .actionGet();
        assertTrue(ackRsp.isAcknowledged());
    }
}
