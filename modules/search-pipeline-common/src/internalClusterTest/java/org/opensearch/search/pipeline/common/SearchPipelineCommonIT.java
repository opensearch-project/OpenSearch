/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.DeleteSearchPipelineRequest;
import org.opensearch.action.search.PutSearchPipelineRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class SearchPipelineCommonIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(SearchPipelineCommonModulePlugin.class);
    }

    public void testFilterQuery() {
        // Create a pipeline with a filter_query processor.
        String pipelineName = "foo";
        PutSearchPipelineRequest putSearchPipelineRequest = new PutSearchPipelineRequest(
            pipelineName,
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
            XContentType.JSON
        );
        AcknowledgedResponse ackRsp = client().admin().cluster().putSearchPipeline(putSearchPipelineRequest).actionGet();
        assertTrue(ackRsp.isAcknowledged());

        // Index some documents.
        String indexName = "myindex";
        IndexRequest doc1 = new IndexRequest(indexName).id("doc1").source(Map.of("field", "value"));
        IndexRequest doc2 = new IndexRequest(indexName).id("doc2").source(Map.of("field", "something else"));

        IndexResponse ir = client().index(doc1).actionGet();
        assertSame(RestStatus.CREATED, ir.status());
        ir = client().index(doc2).actionGet();
        assertSame(RestStatus.CREATED, ir.status());

        // Refresh so the documents are visible to search.
        RefreshResponse refRsp = client().admin().indices().refresh(new RefreshRequest(indexName)).actionGet();
        assertSame(RestStatus.OK, refRsp.getStatus());

        // Search without the pipeline. Should see both documents.
        SearchRequest req = new SearchRequest(indexName).source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));
        SearchResponse rsp = client().search(req).actionGet();
        assertEquals(2, rsp.getHits().getTotalHits().value);

        // Search with the pipeline. Should only see document with "field":"value".
        req.pipeline(pipelineName);
        rsp = client().search(req).actionGet();
        assertEquals(1, rsp.getHits().getTotalHits().value);

        // Clean up.
        ackRsp = client().admin().cluster().deleteSearchPipeline(new DeleteSearchPipelineRequest(pipelineName)).actionGet();
        assertTrue(ackRsp.isAcknowledged());
        ackRsp = client().admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
        assertTrue(ackRsp.isAcknowledged());
    }
}
