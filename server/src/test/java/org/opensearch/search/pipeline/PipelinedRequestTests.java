/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.MockSearchPhaseContext;
import org.opensearch.action.search.QueryPhaseResultConsumer;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseController;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchProgressListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.InternalAggregationTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PipelinedRequestTests extends SearchPipelineTestCase {
    public void testTransformRequest() throws Exception {
        SearchPipelineService searchPipelineService = createWithProcessors();

        SearchPipelineMetadata metadata = new SearchPipelineMetadata(
            Map.of(
                "p1",
                new PipelineConfiguration(
                    "p1",
                    new BytesArray("{\"request_processors\" : [ { \"scale_request_size\": { \"scale\" : 2 } } ] }"),
                    MediaTypeRegistry.JSON
                )
            )
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousState = clusterState;

        Metadata.Builder mdBuilder = Metadata.builder()
            .putCustom(SearchPipelineMetadata.TYPE, metadata)
            .put(indexBuilder("my-index").putAlias(AliasMetadata.builder("barbaz")));

        clusterState = ClusterState.builder(clusterState).metadata(mdBuilder).build();

        searchPipelineService.applyClusterState(new ClusterChangedEvent("", clusterState, previousState));

        int size = 10;
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(new TermQueryBuilder("foo", "bar")).size(size);

        // This request doesn't specify a pipeline, it doesn't get transformed.
        SearchRequest request = new SearchRequest("my-index").source(sourceBuilder);
        PipelinedRequest pipelinedRequest = syncTransformRequest(
            searchPipelineService.resolvePipeline(request, indexNameExpressionResolver)
        );
        assertEquals(size, pipelinedRequest.source().size());

        // Specify the pipeline and use it to transform the request
        request = new SearchRequest("my-index").source(sourceBuilder).pipeline("p1");
        pipelinedRequest = syncTransformRequest(searchPipelineService.resolvePipeline(request, indexNameExpressionResolver));
        assertEquals(2 * size, pipelinedRequest.source().size());
    }

    public void testTransformRequestWithSystemGeneratedPipeline() throws Exception {
        SearchPipelineService service = createWithSystemGeneratedProcessors();
        setUpForResolvePipeline(service);
        enabledAllSystemGeneratedFactories(service);

        SearchRequest searchRequest = new SearchRequest("my_index").source(SearchSourceBuilder.searchSource().size(5));
        PipelinedRequest pipelinedRequest = service.resolvePipeline(searchRequest, indexNameExpressionResolver);
        pipelinedRequest.transformRequest(new ActionListener<SearchRequest>() {
            @Override
            public void onResponse(SearchRequest searchRequest) {
                // verify
                // initial query size = 5
                // system_scale_request_size_pre size = size*2 = 10
                // user defined scale_request_size size = size*2 = 20
                // system_scale_request_size_post size = size*3 = 60
                assertEquals(60, pipelinedRequest.source().size());
            }

            @Override
            public void onFailure(Exception e) {
                fail("Failed to transform the request due to " + e.getMessage());
            }
        });
    }

    public void testTransformResponse() throws Exception {
        SearchPipelineService searchPipelineService = createWithProcessors();

        SearchPipelineMetadata metadata = new SearchPipelineMetadata(
            Map.of(
                "p1",
                new PipelineConfiguration(
                    "p1",
                    new BytesArray("{\"response_processors\" : [ { \"fixed_score\": { \"score\" : 2 } } ] }"),
                    MediaTypeRegistry.JSON
                )
            )
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousState = clusterState;
        clusterState = ClusterState.builder(clusterState)
            .metadata(Metadata.builder().putCustom(SearchPipelineMetadata.TYPE, metadata))
            .build();
        searchPipelineService.applyClusterState(new ClusterChangedEvent("", clusterState, previousState));

        int size = 10;
        SearchResponse searchResponse = createSearchResponse(size);

        // First try without specifying a pipeline, which should be a no-op.
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(createDefaultSearchSourceBuilder());
        PipelinedRequest pipelinedRequest = searchPipelineService.resolvePipeline(searchRequest, indexNameExpressionResolver);
        SearchResponse notTransformedResponse = syncTransformResponse(pipelinedRequest, searchResponse);
        assertSame(searchResponse, notTransformedResponse);

        // Now apply a pipeline
        searchRequest = new SearchRequest().pipeline("p1");
        searchRequest.source(createDefaultSearchSourceBuilder());
        pipelinedRequest = searchPipelineService.resolvePipeline(searchRequest, indexNameExpressionResolver);
        SearchResponse transformedResponse = syncTransformResponse(pipelinedRequest, searchResponse);
        assertEquals(size, transformedResponse.getHits().getHits().length);
        for (int i = 0; i < size; i++) {
            assertEquals(2.0, transformedResponse.getHits().getHits()[i].getScore(), 0.0001f);
        }
    }

    public void testTransformResponseWithSystemGeneratedPipeline() throws Exception {
        SearchPipelineService service = createWithSystemGeneratedProcessors();
        setUpForResolvePipeline(service);
        enabledAllSystemGeneratedFactories(service);
        int size = 10;
        SearchRequest searchRequest = new SearchRequest("my_index").source(SearchSourceBuilder.searchSource().size(size));
        PipelinedRequest pipelinedRequest = service.resolvePipeline(searchRequest, indexNameExpressionResolver);

        ActionListener<SearchResponse> listener = pipelinedRequest.transformResponseListener(new ActionListener<SearchResponse>() {

            @Override
            public void onResponse(SearchResponse searchResponse) {
                // verify
                // system_scale_score_pre score*2
                // system_scale_score_post score*2
                for (int i = 0; i < size; i++) {
                    assertEquals(4.0 * i, searchResponse.getHits().getHits()[i].getScore(), 0.0001f);
                }
            }

            @Override
            public void onFailure(Exception e) {
                fail("Failed to transform the response due to " + e.getMessage());
            }
        });

        SearchResponse searchResponse = createSearchResponse(size);
        listener.onResponse(searchResponse);
    }

    public void testTransformSearchPhase() throws Exception {
        SearchPipelineService searchPipelineService = createWithProcessors();
        SearchPipelineMetadata metadata = new SearchPipelineMetadata(
            Map.of(
                "p1",
                new PipelineConfiguration(
                    "p1",
                    new BytesArray("{\"phase_results_processors\" : [ { \"max_score\" : { } } ]}"),
                    MediaTypeRegistry.JSON
                )
            )
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousState = clusterState;
        clusterState = ClusterState.builder(clusterState)
            .metadata(Metadata.builder().putCustom(SearchPipelineMetadata.TYPE, metadata))
            .build();
        searchPipelineService.applyClusterState(new ClusterChangedEvent("", clusterState, previousState));
        SearchPhaseController controller = new SearchPhaseController(
            writableRegistry(),
            s -> InternalAggregationTestCase.emptyReduceContextBuilder()
        );
        SearchPhaseContext searchPhaseContext = new MockSearchPhaseContext(10);
        QueryPhaseResultConsumer searchPhaseResults = new QueryPhaseResultConsumer(
            searchPhaseContext.getRequest(),
            OpenSearchExecutors.newDirectExecutorService(),
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            controller,
            SearchProgressListener.NOOP,
            writableRegistry(),
            2,
            exc -> {}
        );

        final QuerySearchResult querySearchResult = new QuerySearchResult();
        querySearchResult.setShardIndex(1);
        querySearchResult.topDocs(new TopDocsAndMaxScore(new TopDocs(null, new ScoreDoc[1]), 1f), null);
        searchPhaseResults.consumeResult(querySearchResult, () -> {});

        // First try without specifying a pipeline, which should be a no-op.
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(createDefaultSearchSourceBuilder());
        PipelinedRequest pipelinedRequest = searchPipelineService.resolvePipeline(searchRequest, indexNameExpressionResolver);
        AtomicArray<SearchPhaseResult> notTransformedSearchPhaseResults = searchPhaseResults.getAtomicArray();
        pipelinedRequest.transformSearchPhaseResults(
            searchPhaseResults,
            searchPhaseContext,
            SearchPhaseName.QUERY.getName(),
            SearchPhaseName.FETCH.getName()
        );
        assertSame(searchPhaseResults.getAtomicArray(), notTransformedSearchPhaseResults);

        // Now set the pipeline as p1
        searchRequest = new SearchRequest().pipeline("p1");
        searchRequest.source(createDefaultSearchSourceBuilder());
        pipelinedRequest = searchPipelineService.resolvePipeline(searchRequest, indexNameExpressionResolver);

        pipelinedRequest.transformSearchPhaseResults(
            searchPhaseResults,
            searchPhaseContext,
            SearchPhaseName.QUERY.getName(),
            SearchPhaseName.FETCH.getName()
        );

        List<SearchPhaseResult> resultAtomicArray = searchPhaseResults.getAtomicArray().asList();
        assertEquals(1, resultAtomicArray.size());
        // updating the maxScore
        for (SearchPhaseResult result : resultAtomicArray) {
            assertEquals(100f, result.queryResult().topDocs().maxScore, 0);
        }

        // Check Processor doesn't run for between other phases
        searchRequest = new SearchRequest().pipeline("p1");
        searchRequest.source(createDefaultSearchSourceBuilder());
        pipelinedRequest = searchPipelineService.resolvePipeline(searchRequest, indexNameExpressionResolver);
        AtomicArray<SearchPhaseResult> notTransformedSearchPhaseResult = searchPhaseResults.getAtomicArray();
        pipelinedRequest.transformSearchPhaseResults(
            searchPhaseResults,
            searchPhaseContext,
            SearchPhaseName.DFS_QUERY.getName(),
            SearchPhaseName.QUERY.getName()
        );

        assertSame(searchPhaseResults.getAtomicArray(), notTransformedSearchPhaseResult);
    }

    public void testTransformSearchPhaseWithSystemGeneratedPipeline() throws Exception {
        SearchPipelineService service = createWithSystemGeneratedProcessors();
        setUpForResolvePipeline(service);
        enabledAllSystemGeneratedFactories(service);

        SearchPhaseController controller = new SearchPhaseController(
            writableRegistry(),
            s -> InternalAggregationTestCase.emptyReduceContextBuilder()
        );
        SearchPhaseContext searchPhaseContext = new MockSearchPhaseContext(10);
        QueryPhaseResultConsumer searchPhaseResults = new QueryPhaseResultConsumer(
            searchPhaseContext.getRequest(),
            OpenSearchExecutors.newDirectExecutorService(),
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            controller,
            SearchProgressListener.NOOP,
            writableRegistry(),
            2,
            exc -> {}
        );

        final QuerySearchResult querySearchResult = new QuerySearchResult();
        querySearchResult.setShardIndex(1);
        querySearchResult.topDocs(new TopDocsAndMaxScore(new TopDocs(null, new ScoreDoc[1]), 1f), null);
        searchPhaseResults.consumeResult(querySearchResult, () -> {});

        SearchRequest searchRequest = new SearchRequest("my_index").source(SearchSourceBuilder.searchSource().size(10));
        PipelinedRequest pipelinedRequest = service.resolvePipeline(searchRequest, indexNameExpressionResolver);

        pipelinedRequest.transformSearchPhaseResults(
            searchPhaseResults,
            searchPhaseContext,
            SearchPhaseName.QUERY.getName(),
            SearchPhaseName.FETCH.getName()
        );

        // verify
        List<SearchPhaseResult> resultAtomicArray = searchPhaseResults.getAtomicArray().asList();
        assertEquals(1, resultAtomicArray.size());
        // system_max_score_pre maxScore = 100f
        // system_max_score_post maxScore = maxScore*2 = 200f
        for (SearchPhaseResult result : resultAtomicArray) {
            assertEquals(200f, result.queryResult().topDocs().maxScore, 0);
        }
    }

    private SearchResponse createSearchResponse(int size) {
        SearchHit[] hits = new SearchHit[size];
        for (int i = 0; i < size; i++) {
            hits[i] = new SearchHit(i, "doc" + i, Collections.emptyMap(), Collections.emptyMap());
            hits[i].score(i);
        }
        SearchHits searchHits = new SearchHits(hits, new TotalHits(size, TotalHits.Relation.EQUAL_TO), size);
        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        return new SearchResponse(searchResponseSections, null, 1, 1, 0, 10, null, null);
    }
}
