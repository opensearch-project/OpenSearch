/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.opensearch.OpenSearchParseException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.Version;
import org.opensearch.action.search.DeleteSearchPipelineRequest;
import org.opensearch.action.search.MockSearchPhaseContext;
import org.opensearch.action.search.PutSearchPipelineRequest;
import org.opensearch.action.search.QueryPhaseResultConsumer;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseController;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.action.search.SearchProgressListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.metrics.OperationStats;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.plugins.SearchPipelinePlugin;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchModule;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.InternalAggregationTestCase;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchPipelineServiceTests extends OpenSearchTestCase {

    private static final SearchPipelinePlugin DUMMY_PLUGIN = new SearchPipelinePlugin() {
        @Override
        public Map<String, Processor.Factory<SearchRequestProcessor>> getRequestProcessors(Parameters parameters) {
            return Map.of("foo", (factories, tag, description, ignoreFailure, config, ctx) -> null);
        }

        public Map<String, Processor.Factory<SearchResponseProcessor>> getResponseProcessors(Parameters parameters) {
            return Map.of("bar", (factories, tag, description, ignoreFailure, config, ctx) -> null);
        }

        @Override
        public Map<String, Processor.Factory<SearchPhaseResultsProcessor>> getSearchPhaseResultsProcessors(Parameters parameters) {
            return Map.of("zoe", (factories, tag, description, ignoreFailure, config, ctx) -> null);
        }
    };

    private ThreadPool threadPool;

    @Before
    public void setup() {
        threadPool = mock(ThreadPool.class);
        ExecutorService executorService = OpenSearchExecutors.newDirectExecutorService();
        when(threadPool.generic()).thenReturn(executorService);
        when(threadPool.executor(anyString())).thenReturn(executorService);
    }

    public void testSearchPipelinePlugin() {
        Client client = mock(Client.class);
        SearchPipelineService searchPipelineService = new SearchPipelineService(
            mock(ClusterService.class),
            threadPool,
            null,
            null,
            null,
            this.xContentRegistry(),
            this.writableRegistry(),
            List.of(DUMMY_PLUGIN),
            client
        );
        Map<String, Processor.Factory<SearchRequestProcessor>> requestProcessorFactories = searchPipelineService
            .getRequestProcessorFactories();
        assertEquals(1, requestProcessorFactories.size());
        assertTrue(requestProcessorFactories.containsKey("foo"));
        Map<String, Processor.Factory<SearchResponseProcessor>> responseProcessorFactories = searchPipelineService
            .getResponseProcessorFactories();
        assertEquals(1, responseProcessorFactories.size());
        assertTrue(responseProcessorFactories.containsKey("bar"));
    }

    public void testSearchPipelinePluginDuplicate() {
        Client client = mock(Client.class);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new SearchPipelineService(
                mock(ClusterService.class),
                threadPool,
                null,
                null,
                null,
                this.xContentRegistry(),
                this.writableRegistry(),
                List.of(DUMMY_PLUGIN, DUMMY_PLUGIN),
                client
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains(" already registered"));
    }

    public void testResolveSearchPipelineDoesNotExist() {
        Client client = mock(Client.class);
        SearchPipelineService searchPipelineService = new SearchPipelineService(
            mock(ClusterService.class),
            threadPool,
            null,
            null,
            null,
            this.xContentRegistry(),
            this.writableRegistry(),
            List.of(DUMMY_PLUGIN),
            client
        );
        final SearchRequest searchRequest = new SearchRequest("_index").pipeline("bar");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> searchPipelineService.resolvePipeline(searchRequest)
        );
        assertTrue(e.getMessage(), e.getMessage().contains(" not defined"));
    }

    public void testResolveIndexDefaultPipeline() throws Exception {
        SearchPipelineService service = createWithProcessors();

        SearchPipelineMetadata metadata = new SearchPipelineMetadata(
            Map.of(
                "p1",
                new PipelineConfiguration(
                    "p1",
                    new BytesArray("{\"request_processors\" : [ { \"scale_request_size\": { \"scale\" : 2 } } ] }"),
                    XContentType.JSON
                )
            )
        );
        Settings defaultPipelineSetting = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            .put(IndexSettings.DEFAULT_SEARCH_PIPELINE.getKey(), "p1")
            .build();
        IndexMetadata indexMetadata = new IndexMetadata.Builder("my_index").settings(defaultPipelineSetting).build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousState = clusterState;
        clusterState = ClusterState.builder(clusterState)
            .metadata(Metadata.builder().put(indexMetadata, false).putCustom(SearchPipelineMetadata.TYPE, metadata))
            .build();

        ClusterChangedEvent cce = new ClusterChangedEvent("", clusterState, previousState);
        service.applyClusterState(cce);

        SearchRequest searchRequest = new SearchRequest("my_index").source(SearchSourceBuilder.searchSource().size(5));
        PipelinedRequest pipelinedRequest = service.resolvePipeline(searchRequest);
        assertEquals("p1", pipelinedRequest.getPipeline().getId());
        assertEquals(10, pipelinedRequest.source().size());

        // Bypass the default pipeline
        searchRequest.pipeline("_none");
        pipelinedRequest = service.resolvePipeline(searchRequest);
        assertEquals("_none", pipelinedRequest.getPipeline().getId());
        assertEquals(5, pipelinedRequest.source().size());
    }

    private static class FakeRequestProcessor extends AbstractProcessor implements SearchRequestProcessor {
        private final Consumer<SearchRequest> executor;

        private final String type;

        public FakeRequestProcessor(String type, String tag, String description, boolean ignoreFailure, Consumer<SearchRequest> executor) {
            super(tag, description, ignoreFailure);
            this.executor = executor;
            this.type = type;
        }

        @Override
        public SearchRequest processRequest(SearchRequest request) throws Exception {
            executor.accept(request);
            return request;
        }

        /*
         * Gets the type of processor
         */
        @Override
        public String getType() {
            return this.type;
        }
    }

    private static class FakeResponseProcessor extends AbstractProcessor implements SearchResponseProcessor {
        private final Consumer<SearchResponse> executor;
        private String type;

        public FakeResponseProcessor(
            String type,
            String tag,
            String description,
            boolean ignoreFailure,
            Consumer<SearchResponse> executor
        ) {
            super(tag, description, ignoreFailure);
            this.executor = executor;
            this.type = type;
        }

        @Override
        public SearchResponse processResponse(SearchRequest request, SearchResponse response) throws Exception {
            executor.accept(response);
            return response;
        }

        /**
         * Gets the type of processor
         */
        @Override
        public String getType() {
            return this.type;
        }
    }

    private static class FakeSearchPhaseResultsProcessor extends AbstractProcessor implements SearchPhaseResultsProcessor {
        private Consumer<SearchPhaseResult> querySearchResultConsumer;

        private String type;

        public FakeSearchPhaseResultsProcessor(
            String type,
            String tag,
            String description,
            boolean ignoreFailure,
            Consumer<SearchPhaseResult> querySearchResultConsumer
        ) {
            super(tag, description, ignoreFailure);
            this.querySearchResultConsumer = querySearchResultConsumer;
            this.type = type;
        }

        @Override
        public <Result extends SearchPhaseResult> void process(
            SearchPhaseResults<Result> searchPhaseResult,
            SearchPhaseContext searchPhaseContext
        ) {
            List<Result> resultAtomicArray = searchPhaseResult.getAtomicArray().asList();
            // updating the maxScore
            resultAtomicArray.forEach(querySearchResultConsumer);
        }

        @Override
        public SearchPhaseName getBeforePhase() {
            return SearchPhaseName.QUERY;
        }

        @Override
        public SearchPhaseName getAfterPhase() {
            return SearchPhaseName.FETCH;
        }

        /**
         * Gets the type of processor
         */
        @Override
        public String getType() {
            return this.type;
        }
    }

    private SearchPipelineService createWithProcessors() {
        Map<String, Processor.Factory<SearchRequestProcessor>> requestProcessors = new HashMap<>();
        requestProcessors.put("scale_request_size", (processorFactories, tag, description, ignoreFailure, config, ctx) -> {
            float scale = ((Number) config.remove("scale")).floatValue();
            return new FakeRequestProcessor(
                "scale_request_size",
                tag,
                description,
                ignoreFailure,
                req -> req.source().size((int) (req.source().size() * scale))
            );
        });
        Map<String, Processor.Factory<SearchResponseProcessor>> responseProcessors = new HashMap<>();
        responseProcessors.put("fixed_score", (processorFactories, tag, description, ignoreFailure, config, ctx) -> {
            float score = ((Number) config.remove("score")).floatValue();
            return new FakeResponseProcessor(
                "fixed_score",
                tag,
                description,
                ignoreFailure,
                rsp -> rsp.getHits().forEach(h -> h.score(score))
            );
        });

        Map<String, Processor.Factory<SearchPhaseResultsProcessor>> searchPhaseProcessors = new HashMap<>();
        searchPhaseProcessors.put("max_score", (processorFactories, tag, description, ignoreFailure, config, context) -> {
            final float finalScore = config.containsKey("score") ? ((Number) config.remove("score")).floatValue() : 100f;
            final Consumer<SearchPhaseResult> querySearchResultConsumer = (result) -> result.queryResult().topDocs().maxScore = finalScore;
            return new FakeSearchPhaseResultsProcessor("max_score", tag, description, ignoreFailure, querySearchResultConsumer);
        });

        return createWithProcessors(requestProcessors, responseProcessors, searchPhaseProcessors);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    private SearchPipelineService createWithProcessors(
        Map<String, Processor.Factory<SearchRequestProcessor>> requestProcessors,
        Map<String, Processor.Factory<SearchResponseProcessor>> responseProcessors,
        Map<String, Processor.Factory<SearchPhaseResultsProcessor>> phaseProcessors
    ) {
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executorService = OpenSearchExecutors.newDirectExecutorService();
        when(threadPool.generic()).thenReturn(executorService);
        when(threadPool.executor(anyString())).thenReturn(executorService);
        return new SearchPipelineService(
            mock(ClusterService.class),
            threadPool,
            null,
            null,
            null,
            null,
            this.writableRegistry(),
            Collections.singletonList(new SearchPipelinePlugin() {
                @Override
                public Map<String, Processor.Factory<SearchRequestProcessor>> getRequestProcessors(Parameters parameters) {
                    return requestProcessors;
                }

                @Override
                public Map<String, Processor.Factory<SearchResponseProcessor>> getResponseProcessors(Parameters parameters) {
                    return responseProcessors;
                }

                @Override
                public Map<String, Processor.Factory<SearchPhaseResultsProcessor>> getSearchPhaseResultsProcessors(Parameters parameters) {
                    return phaseProcessors;
                }

            }),
            client
        );
    }

    public void testUpdatePipelines() {
        SearchPipelineService searchPipelineService = createWithProcessors();
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        searchPipelineService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertEquals(0, searchPipelineService.getPipelines().size());

        PipelineConfiguration pipeline = new PipelineConfiguration(
            "_id",
            new BytesArray(
                "{ "
                    + "\"request_processors\" : [ { \"scale_request_size\": { \"scale\" : 2 } } ], "
                    + "\"response_processors\" : [ { \"fixed_score\" : { \"score\" : 1.0 } } ],"
                    + "\"phase_results_processors\" : [ { \"max_score\" : { \"score\": 100 } } ]"
                    + "}"
            ),
            XContentType.JSON
        );
        SearchPipelineMetadata pipelineMetadata = new SearchPipelineMetadata(Map.of("_id", pipeline));
        clusterState = ClusterState.builder(clusterState)
            .metadata(Metadata.builder().putCustom(SearchPipelineMetadata.TYPE, pipelineMetadata).build())
            .build();
        searchPipelineService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertEquals(1, searchPipelineService.getPipelines().size());
        assertEquals("_id", searchPipelineService.getPipelines().get("_id").pipeline.getId());
        assertNull(searchPipelineService.getPipelines().get("_id").pipeline.getDescription());
        assertEquals(1, searchPipelineService.getPipelines().get("_id").pipeline.getSearchRequestProcessors().size());
        assertEquals(
            "scale_request_size",
            searchPipelineService.getPipelines().get("_id").pipeline.getSearchRequestProcessors().get(0).getType()
        );
        assertEquals(1, searchPipelineService.getPipelines().get("_id").pipeline.getSearchPhaseResultsProcessors().size());
        assertEquals(
            "max_score",
            searchPipelineService.getPipelines().get("_id").pipeline.getSearchPhaseResultsProcessors().get(0).getType()
        );
        assertEquals(1, searchPipelineService.getPipelines().get("_id").pipeline.getSearchResponseProcessors().size());
        assertEquals(
            "fixed_score",
            searchPipelineService.getPipelines().get("_id").pipeline.getSearchResponseProcessors().get(0).getType()
        );
    }

    public void testPutPipeline() {
        SearchPipelineService searchPipelineService = createWithProcessors();
        String id = "_id";
        SearchPipelineService.PipelineHolder pipeline = searchPipelineService.getPipelines().get(id);
        assertNull(pipeline);

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();

        PutSearchPipelineRequest putRequest = new PutSearchPipelineRequest(id, new BytesArray("{}"), XContentType.JSON);
        ClusterState previousClusterState = clusterState;
        clusterState = SearchPipelineService.innerPut(putRequest, clusterState);
        searchPipelineService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = searchPipelineService.getPipelines().get(id);
        assertNotNull(pipeline);
        assertEquals(id, pipeline.pipeline.getId());
        assertNull(pipeline.pipeline.getDescription());
        assertEquals(0, pipeline.pipeline.getSearchRequestProcessors().size());
        assertEquals(0, pipeline.pipeline.getSearchResponseProcessors().size());

        // Overwrite pipeline
        putRequest = new PutSearchPipelineRequest(id, new BytesArray("{ \"description\": \"empty pipeline\"}"), XContentType.JSON);
        previousClusterState = clusterState;
        clusterState = SearchPipelineService.innerPut(putRequest, clusterState);
        searchPipelineService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = searchPipelineService.getPipelines().get(id);
        assertNotNull(pipeline);
        assertEquals(id, pipeline.pipeline.getId());
        assertEquals("empty pipeline", pipeline.pipeline.getDescription());
        assertEquals(0, pipeline.pipeline.getSearchRequestProcessors().size());
        assertEquals(0, pipeline.pipeline.getSearchResponseProcessors().size());
        assertEquals(0, pipeline.pipeline.getSearchPhaseResultsProcessors().size());
    }

    public void testPutInvalidPipeline() throws IllegalAccessException {
        SearchPipelineService searchPipelineService = createWithProcessors();
        String id = "_id";

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousState = clusterState;

        PutSearchPipelineRequest putRequest = new PutSearchPipelineRequest(
            id,
            new BytesArray("{\"request_processors\" : [ { \"scale_request_size\": { \"scale\" : \"foo\" } } ] }"),
            XContentType.JSON
        );
        clusterState = SearchPipelineService.innerPut(putRequest, clusterState);
        try (MockLogAppender mockAppender = MockLogAppender.createForLoggers(LogManager.getLogger(SearchPipelineService.class))) {
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test1",
                    SearchPipelineService.class.getCanonicalName(),
                    Level.WARN,
                    "failed to update search pipelines"
                )
            );
            searchPipelineService.applyClusterState(new ClusterChangedEvent("", clusterState, previousState));
            mockAppender.assertAllExpectationsMatched();
        }
        assertEquals(0, searchPipelineService.getPipelines().size());
    }

    public void testDeletePipeline() {
        SearchPipelineService searchPipelineService = createWithProcessors();
        PipelineConfiguration config = new PipelineConfiguration(
            "_id",
            new BytesArray("{\"request_processors\" : [ { \"scale_request_size\": { \"scale\" : 2 } } ] }"),
            XContentType.JSON
        );
        SearchPipelineMetadata searchPipelineMetadata = new SearchPipelineMetadata(Map.of("_id", config));
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousState = clusterState;
        clusterState = ClusterState.builder(clusterState)
            .metadata(Metadata.builder().putCustom(SearchPipelineMetadata.TYPE, searchPipelineMetadata).build())
            .build();
        searchPipelineService.applyClusterState(new ClusterChangedEvent("", clusterState, previousState));
        assertEquals(1, searchPipelineService.getPipelines().size());

        // Delete pipeline:
        DeleteSearchPipelineRequest deleteRequest = new DeleteSearchPipelineRequest("_id");
        previousState = clusterState;
        clusterState = SearchPipelineService.innerDelete(deleteRequest, clusterState);
        searchPipelineService.applyClusterState(new ClusterChangedEvent("", clusterState, previousState));
        assertEquals(0, searchPipelineService.getPipelines().size());

        final ClusterState finalClusterState = clusterState;
        // Delete missing pipeline
        ResourceNotFoundException e = expectThrows(
            ResourceNotFoundException.class,
            () -> SearchPipelineService.innerDelete(deleteRequest, finalClusterState)
        );
        assertEquals("pipeline [_id] is missing", e.getMessage());
    }

    public void testDeletePipelinesWithWildcard() {
        SearchPipelineService searchPipelineService = createWithProcessors();
        BytesArray definition = new BytesArray("{\"request_processors\" : [ { \"scale_request_size\": { \"scale\" : 2 } } ] }");
        SearchPipelineMetadata metadata = new SearchPipelineMetadata(
            Map.of(
                "p1",
                new PipelineConfiguration("p1", definition, XContentType.JSON),
                "p2",
                new PipelineConfiguration("p2", definition, XContentType.JSON),
                "q1",
                new PipelineConfiguration("q1", definition, XContentType.JSON)
            )
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousState = clusterState;
        clusterState = ClusterState.builder(clusterState)
            .metadata(Metadata.builder().putCustom(SearchPipelineMetadata.TYPE, metadata))
            .build();
        searchPipelineService.applyClusterState(new ClusterChangedEvent("", clusterState, previousState));
        assertNotNull(searchPipelineService.getPipelines().get("p1"));
        assertNotNull(searchPipelineService.getPipelines().get("p2"));
        assertNotNull(searchPipelineService.getPipelines().get("q1"));

        // Delete all pipelines starting with "p"
        DeleteSearchPipelineRequest deleteRequest = new DeleteSearchPipelineRequest("p*");
        previousState = clusterState;
        clusterState = SearchPipelineService.innerDelete(deleteRequest, clusterState);
        searchPipelineService.applyClusterState(new ClusterChangedEvent("", clusterState, previousState));
        assertEquals(1, searchPipelineService.getPipelines().size());
        assertNotNull(searchPipelineService.getPipelines().get("q1"));

        // Prefix wildcard fails if no matches
        final ClusterState finalClusterState = clusterState;
        ResourceNotFoundException e = expectThrows(
            ResourceNotFoundException.class,
            () -> SearchPipelineService.innerDelete(deleteRequest, finalClusterState)
        );
        assertEquals("pipeline [p*] is missing", e.getMessage());

        // Delete all removes remaining pipeline
        DeleteSearchPipelineRequest deleteAllRequest = new DeleteSearchPipelineRequest("*");
        previousState = clusterState;
        clusterState = SearchPipelineService.innerDelete(deleteAllRequest, clusterState);
        searchPipelineService.applyClusterState(new ClusterChangedEvent("", clusterState, previousState));
        assertEquals(0, searchPipelineService.getPipelines().size());

        // Delete all does not throw exception if no pipelines
        SearchPipelineService.innerDelete(deleteAllRequest, clusterState);
    }

    public void testTransformRequest() throws Exception {
        SearchPipelineService searchPipelineService = createWithProcessors();

        SearchPipelineMetadata metadata = new SearchPipelineMetadata(
            Map.of(
                "p1",
                new PipelineConfiguration(
                    "p1",
                    new BytesArray("{\"request_processors\" : [ { \"scale_request_size\": { \"scale\" : 2 } } ] }"),
                    XContentType.JSON
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
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(new TermQueryBuilder("foo", "bar")).size(size);
        SearchRequest request = new SearchRequest("_index").source(sourceBuilder).pipeline("p1");

        PipelinedRequest pipelinedRequest = searchPipelineService.resolvePipeline(request);

        assertEquals(2 * size, pipelinedRequest.source().size());
        assertEquals(size, request.source().size());

        // This request doesn't specify a pipeline, it doesn't get transformed.
        request = new SearchRequest("_index").source(sourceBuilder);
        pipelinedRequest = searchPipelineService.resolvePipeline(request);
        assertEquals(size, pipelinedRequest.source().size());
    }

    public void testTransformResponse() throws Exception {
        SearchPipelineService searchPipelineService = createWithProcessors();

        SearchPipelineMetadata metadata = new SearchPipelineMetadata(
            Map.of(
                "p1",
                new PipelineConfiguration(
                    "p1",
                    new BytesArray("{\"response_processors\" : [ { \"fixed_score\": { \"score\" : 2 } } ] }"),
                    XContentType.JSON
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
        SearchHit[] hits = new SearchHit[size];
        for (int i = 0; i < size; i++) {
            hits[i] = new SearchHit(i, "doc" + i, Collections.emptyMap(), Collections.emptyMap());
            hits[i].score(i);
        }
        SearchHits searchHits = new SearchHits(hits, new TotalHits(size * 2, TotalHits.Relation.EQUAL_TO), size);
        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        SearchResponse searchResponse = new SearchResponse(searchResponseSections, null, 1, 1, 0, 10, null, null);

        // First try without specifying a pipeline, which should be a no-op.
        SearchRequest searchRequest = new SearchRequest();
        PipelinedRequest pipelinedRequest = searchPipelineService.resolvePipeline(searchRequest);
        SearchResponse notTransformedResponse = pipelinedRequest.transformResponse(searchResponse);
        assertSame(searchResponse, notTransformedResponse);

        // Now apply a pipeline
        searchRequest = new SearchRequest().pipeline("p1");
        pipelinedRequest = searchPipelineService.resolvePipeline(searchRequest);
        SearchResponse transformedResponse = pipelinedRequest.transformResponse(searchResponse);
        assertEquals(size, transformedResponse.getHits().getHits().length);
        for (int i = 0; i < size; i++) {
            assertEquals(2.0, transformedResponse.getHits().getHits()[i].getScore(), 0.0001f);
        }
    }

    public void testTransformSearchPhase() {
        SearchPipelineService searchPipelineService = createWithProcessors();
        SearchPipelineMetadata metadata = new SearchPipelineMetadata(
            Map.of(
                "p1",
                new PipelineConfiguration(
                    "p1",
                    new BytesArray("{\"phase_results_processors\" : [ { \"max_score\" : { } } ]}"),
                    XContentType.JSON
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
        PipelinedRequest pipelinedRequest = searchPipelineService.resolvePipeline(searchRequest);
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
        pipelinedRequest = searchPipelineService.resolvePipeline(searchRequest);

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
        pipelinedRequest = searchPipelineService.resolvePipeline(searchRequest);
        AtomicArray<SearchPhaseResult> notTransformedSearchPhaseResult = searchPhaseResults.getAtomicArray();
        pipelinedRequest.transformSearchPhaseResults(
            searchPhaseResults,
            searchPhaseContext,
            SearchPhaseName.DFS_QUERY.getName(),
            SearchPhaseName.QUERY.getName()
        );

        assertSame(searchPhaseResults.getAtomicArray(), notTransformedSearchPhaseResult);
    }

    public void testGetPipelines() {
        //
        assertEquals(0, SearchPipelineService.innerGetPipelines(null, "p1").size());

        SearchPipelineMetadata metadata = new SearchPipelineMetadata(
            Map.of(
                "p1",
                new PipelineConfiguration(
                    "p1",
                    new BytesArray("{\"request_processors\" : [ { \"scale_request_size\": { \"scale\" : 2 } } ] }"),
                    XContentType.JSON
                ),
                "p2",
                new PipelineConfiguration(
                    "p2",
                    new BytesArray("{\"response_processors\" : [ { \"fixed_score\": { \"score\" : 2 } } ] }"),
                    XContentType.JSON
                ),
                "p3",
                new PipelineConfiguration(
                    "p3",
                    new BytesArray("{\"phase_results_processors\" : [ { \"max_score\" : { } } ]}"),
                    XContentType.JSON
                )
            )
        );

        // Return all when no ids specified
        List<PipelineConfiguration> pipelines = SearchPipelineService.innerGetPipelines(metadata);
        assertEquals(3, pipelines.size());
        pipelines.sort(Comparator.comparing(PipelineConfiguration::getId));
        assertEquals("p1", pipelines.get(0).getId());
        assertEquals("p2", pipelines.get(1).getId());
        assertEquals("p3", pipelines.get(2).getId());

        // Get specific pipeline
        pipelines = SearchPipelineService.innerGetPipelines(metadata, "p1");
        assertEquals(1, pipelines.size());
        assertEquals("p1", pipelines.get(0).getId());

        // Get both pipelines explicitly
        pipelines = SearchPipelineService.innerGetPipelines(metadata, "p1", "p2");
        assertEquals(2, pipelines.size());
        pipelines.sort(Comparator.comparing(PipelineConfiguration::getId));
        assertEquals("p1", pipelines.get(0).getId());
        assertEquals("p2", pipelines.get(1).getId());

        // Match all
        pipelines = SearchPipelineService.innerGetPipelines(metadata, "*");
        assertEquals(3, pipelines.size());
        pipelines.sort(Comparator.comparing(PipelineConfiguration::getId));
        assertEquals("p1", pipelines.get(0).getId());
        assertEquals("p2", pipelines.get(1).getId());
        assertEquals("p3", pipelines.get(2).getId());

        // Match prefix
        pipelines = SearchPipelineService.innerGetPipelines(metadata, "p*");
        assertEquals(3, pipelines.size());
        pipelines.sort(Comparator.comparing(PipelineConfiguration::getId));
        assertEquals("p1", pipelines.get(0).getId());
        assertEquals("p2", pipelines.get(1).getId());
        assertEquals("p3", pipelines.get(2).getId());
    }

    public void testValidatePipeline() throws Exception {
        SearchPipelineService searchPipelineService = createWithProcessors();

        ProcessorInfo reqProcessor = new ProcessorInfo("scale_request_size");
        ProcessorInfo rspProcessor = new ProcessorInfo("fixed_score");
        ProcessorInfo injProcessor = new ProcessorInfo("max_score");
        DiscoveryNode n1 = new DiscoveryNode("n1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode n2 = new DiscoveryNode("n2", buildNewFakeTransportAddress(), Version.CURRENT);
        PutSearchPipelineRequest putRequest = new PutSearchPipelineRequest(
            "p1",
            new BytesArray(
                "{"
                    + "\"request_processors\": [{ \"scale_request_size\": { \"scale\" : 2 } }],"
                    + "\"response_processors\": [{ \"fixed_score\": { \"score\" : 2 } }],"
                    + "\"phase_results_processors\" : [ { \"max_score\" : { } } ]"
                    + "}"
            ),
            XContentType.JSON
        );

        SearchPipelineInfo completePipelineInfo = new SearchPipelineInfo(
            Map.of(Pipeline.REQUEST_PROCESSORS_KEY, List.of(reqProcessor), Pipeline.RESPONSE_PROCESSORS_KEY, List.of(rspProcessor))
        );
        SearchPipelineInfo incompletePipelineInfo = new SearchPipelineInfo(Map.of(Pipeline.REQUEST_PROCESSORS_KEY, List.of(reqProcessor)));
        // One node is missing a processor
        expectThrows(
            OpenSearchParseException.class,
            () -> searchPipelineService.validatePipeline(Map.of(n1, completePipelineInfo, n2, incompletePipelineInfo), putRequest)
        );

        // Discovery failed, no infos passed.
        expectThrows(IllegalStateException.class, () -> searchPipelineService.validatePipeline(Collections.emptyMap(), putRequest));

        // Invalid configuration in request
        PutSearchPipelineRequest badPutRequest = new PutSearchPipelineRequest(
            "p1",
            new BytesArray(
                "{"
                    + "\"request_processors\": [{ \"scale_request_size\": { \"scale\" : \"banana\" } }],"
                    + "\"response_processors\": [{ \"fixed_score\": { \"score\" : 2 } }]"
                    + "}"
            ),
            XContentType.JSON
        );
        expectThrows(
            ClassCastException.class,
            () -> searchPipelineService.validatePipeline(Map.of(n1, completePipelineInfo, n2, completePipelineInfo), badPutRequest)
        );

        // Success
        searchPipelineService.validatePipeline(Map.of(n1, completePipelineInfo, n2, completePipelineInfo), putRequest);
    }

    /**
     * Tests a pipeline defined in the search request source.
     */
    public void testInlinePipeline() throws Exception {
        SearchPipelineService searchPipelineService = createWithProcessors();
        Map<String, Object> pipelineSourceMap = new HashMap<>();
        Map<String, Object> requestProcessorConfig = new HashMap<>();
        requestProcessorConfig.put("scale", 2);
        Map<String, Object> requestProcessorObject = new HashMap<>();
        requestProcessorObject.put("scale_request_size", requestProcessorConfig);
        pipelineSourceMap.put(Pipeline.REQUEST_PROCESSORS_KEY, List.of(requestProcessorObject));
        Map<String, Object> responseProcessorConfig = new HashMap<>();
        responseProcessorConfig.put("score", 2);
        Map<String, Object> responseProcessorObject = new HashMap<>();
        responseProcessorObject.put("fixed_score", responseProcessorConfig);
        pipelineSourceMap.put(Pipeline.RESPONSE_PROCESSORS_KEY, List.of(responseProcessorObject));

        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource().size(100).searchPipelineSource(pipelineSourceMap);
        SearchRequest searchRequest = new SearchRequest().source(sourceBuilder);

        // Verify pipeline
        PipelinedRequest pipelinedRequest = searchPipelineService.resolvePipeline(searchRequest);
        Pipeline pipeline = pipelinedRequest.getPipeline();
        assertEquals(SearchPipelineService.AD_HOC_PIPELINE_ID, pipeline.getId());
        assertEquals(1, pipeline.getSearchRequestProcessors().size());
        assertEquals(1, pipeline.getSearchResponseProcessors().size());

        // Verify that pipeline transforms request
        assertEquals(200, pipelinedRequest.source().size());

        int size = 10;
        SearchHit[] hits = new SearchHit[size];
        for (int i = 0; i < size; i++) {
            hits[i] = new SearchHit(i, "doc" + i, Collections.emptyMap(), Collections.emptyMap());
            hits[i].score(i);
        }
        SearchHits searchHits = new SearchHits(hits, new TotalHits(size * 2, TotalHits.Relation.EQUAL_TO), size);
        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        SearchResponse searchResponse = new SearchResponse(searchResponseSections, null, 1, 1, 0, 10, null, null);

        SearchResponse transformedResponse = pipeline.transformResponse(searchRequest, searchResponse);
        for (int i = 0; i < size; i++) {
            assertEquals(2.0, transformedResponse.getHits().getHits()[i].getScore(), 0.0001);
        }
    }

    public void testInfo() {
        SearchPipelineService searchPipelineService = createWithProcessors();
        SearchPipelineInfo info = searchPipelineService.info();
        assertTrue(info.containsProcessor(Pipeline.REQUEST_PROCESSORS_KEY, "scale_request_size"));
        assertTrue(info.containsProcessor(Pipeline.RESPONSE_PROCESSORS_KEY, "fixed_score"));
    }

    public void testExceptionOnPipelineCreation() {
        Map<String, Processor.Factory<SearchRequestProcessor>> badFactory = Map.of("bad_factory", (pf, t, i, f, c, ctx) -> {
            throw new RuntimeException();
        });
        SearchPipelineService searchPipelineService = createWithProcessors(badFactory, Collections.emptyMap(), Collections.emptyMap());

        Map<String, Object> pipelineSourceMap = new HashMap<>();
        pipelineSourceMap.put(Pipeline.REQUEST_PROCESSORS_KEY, List.of(Map.of("bad_factory", Collections.emptyMap())));

        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource().searchPipelineSource(pipelineSourceMap);
        SearchRequest searchRequest = new SearchRequest().source(sourceBuilder);

        // Exception thrown when creating the pipeline
        expectThrows(SearchPipelineProcessingException.class, () -> searchPipelineService.resolvePipeline(searchRequest));

    }

    public void testExceptionOnRequestProcessing() {
        SearchRequestProcessor throwingRequestProcessor = new FakeRequestProcessor("throwing_request", null, null, false, r -> {
            throw new RuntimeException();
        });
        Map<String, Processor.Factory<SearchRequestProcessor>> throwingRequestProcessorFactory = Map.of(
            "throwing_request",
            (pf, t, i, f, c, ctx) -> throwingRequestProcessor
        );

        SearchPipelineService searchPipelineService = createWithProcessors(
            throwingRequestProcessorFactory,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        Map<String, Object> pipelineSourceMap = new HashMap<>();
        pipelineSourceMap.put(Pipeline.REQUEST_PROCESSORS_KEY, List.of(Map.of("throwing_request", Collections.emptyMap())));

        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource().searchPipelineSource(pipelineSourceMap);
        SearchRequest searchRequest = new SearchRequest().source(sourceBuilder);

        // Exception thrown when processing the request
        expectThrows(SearchPipelineProcessingException.class, () -> searchPipelineService.resolvePipeline(searchRequest));
    }

    public void testExceptionOnResponseProcessing() throws Exception {
        SearchResponseProcessor throwingResponseProcessor = new FakeResponseProcessor("throwing_response", null, null, false, r -> {
            throw new RuntimeException();
        });
        Map<String, Processor.Factory<SearchResponseProcessor>> throwingResponseProcessorFactory = Map.of(
            "throwing_response",
            (pf, t, i, f, c, ctx) -> throwingResponseProcessor
        );

        SearchPipelineService searchPipelineService = createWithProcessors(
            Collections.emptyMap(),
            throwingResponseProcessorFactory,
            Collections.emptyMap()
        );

        Map<String, Object> pipelineSourceMap = new HashMap<>();
        pipelineSourceMap.put(Pipeline.RESPONSE_PROCESSORS_KEY, List.of(Map.of("throwing_response", new HashMap<>())));

        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource().size(100).searchPipelineSource(pipelineSourceMap);
        SearchRequest searchRequest = new SearchRequest().source(sourceBuilder);

        PipelinedRequest pipelinedRequest = searchPipelineService.resolvePipeline(searchRequest);

        SearchResponse response = new SearchResponse(null, null, 0, 0, 0, 0, null, null);
        // Exception thrown when processing response
        expectThrows(SearchPipelineProcessingException.class, () -> pipelinedRequest.transformResponse(response));
    }

    public void testCatchExceptionOnRequestProcessing() throws IllegalAccessException {
        SearchRequestProcessor throwingRequestProcessor = new FakeRequestProcessor("throwing_request", null, null, true, r -> {
            throw new RuntimeException();
        });
        Map<String, Processor.Factory<SearchRequestProcessor>> throwingRequestProcessorFactory = Map.of(
            "throwing_request",
            (pf, t, i, f, c, ctx) -> throwingRequestProcessor
        );

        SearchPipelineService searchPipelineService = createWithProcessors(
            throwingRequestProcessorFactory,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        Map<String, Object> pipelineSourceMap = new HashMap<>();
        pipelineSourceMap.put(Pipeline.REQUEST_PROCESSORS_KEY, List.of(Map.of("throwing_request", Collections.emptyMap())));

        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource().searchPipelineSource(pipelineSourceMap);
        SearchRequest searchRequest = new SearchRequest().source(sourceBuilder);

        // Caught Exception thrown when processing the request and produced warn level logging message
        try (MockLogAppender mockAppender = MockLogAppender.createForLoggers(LogManager.getLogger(Pipeline.class))) {
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test1",
                    Pipeline.class.getCanonicalName(),
                    Level.WARN,
                    "The exception from request processor [throwing_request] in the search pipeline [_ad_hoc_pipeline] was ignored"
                )
            );
            PipelinedRequest pipelinedRequest = searchPipelineService.resolvePipeline(searchRequest);
            mockAppender.assertAllExpectationsMatched();
        }
    }

    public void testCatchExceptionOnResponseProcessing() throws Exception {
        SearchResponseProcessor throwingResponseProcessor = new FakeResponseProcessor("throwing_response", null, null, true, r -> {
            throw new RuntimeException();
        });
        Map<String, Processor.Factory<SearchResponseProcessor>> throwingResponseProcessorFactory = Map.of(
            "throwing_response",
            (pf, t, i, f, c, ctx) -> throwingResponseProcessor
        );

        SearchPipelineService searchPipelineService = createWithProcessors(
            Collections.emptyMap(),
            throwingResponseProcessorFactory,
            Collections.emptyMap()
        );

        Map<String, Object> pipelineSourceMap = new HashMap<>();
        pipelineSourceMap.put(Pipeline.RESPONSE_PROCESSORS_KEY, List.of(Map.of("throwing_response", Collections.emptyMap())));

        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource().size(100).searchPipelineSource(pipelineSourceMap);
        SearchRequest searchRequest = new SearchRequest().source(sourceBuilder);

        PipelinedRequest pipelinedRequest = searchPipelineService.resolvePipeline(searchRequest);

        SearchResponse response = new SearchResponse(null, null, 0, 0, 0, 0, null, null);

        // Caught Exception thrown when processing response and produced warn level logging message
        try (MockLogAppender mockAppender = MockLogAppender.createForLoggers(LogManager.getLogger(Pipeline.class))) {
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test1",
                    Pipeline.class.getCanonicalName(),
                    Level.WARN,
                    "The exception from response processor [throwing_response] in the search pipeline [_ad_hoc_pipeline] was ignored"
                )
            );
            pipelinedRequest.transformResponse(response);
            mockAppender.assertAllExpectationsMatched();
        }
    }

    public void testStats() throws Exception {
        SearchRequestProcessor throwingRequestProcessor = new FakeRequestProcessor("throwing_request", "1", null, false, r -> {
            throw new RuntimeException();
        });
        Map<String, Processor.Factory<SearchRequestProcessor>> requestProcessors = Map.of(
            "successful_request",
            (pf, t, i, f, c, ctx) -> new FakeRequestProcessor("successful_request", "2", null, false, r -> {}),
            "throwing_request",
            (pf, t, i, f, c, ctx) -> throwingRequestProcessor
        );
        SearchResponseProcessor throwingResponseProcessor = new FakeResponseProcessor("throwing_response", "3", null, false, r -> {
            throw new RuntimeException();
        });
        Map<String, Processor.Factory<SearchResponseProcessor>> responseProcessors = Map.of(
            "successful_response",
            (pf, t, i, f, c, ctx) -> new FakeResponseProcessor("successful_response", "4", null, false, r -> {}),
            "throwing_response",
            (pf, t, i, f, c, ctx) -> throwingResponseProcessor
        );

        SearchPipelineService searchPipelineService = getSearchPipelineService(requestProcessors, responseProcessors);

        SearchRequest request = new SearchRequest();
        SearchResponse response = new SearchResponse(null, null, 0, 0, 0, 0, null, null);

        searchPipelineService.resolvePipeline(request.pipeline("good_request_pipeline")).transformResponse(response);
        expectThrows(
            SearchPipelineProcessingException.class,
            () -> searchPipelineService.resolvePipeline(request.pipeline("bad_request_pipeline")).transformResponse(response)
        );
        searchPipelineService.resolvePipeline(request.pipeline("good_response_pipeline")).transformResponse(response);
        expectThrows(
            SearchPipelineProcessingException.class,
            () -> searchPipelineService.resolvePipeline(request.pipeline("bad_response_pipeline")).transformResponse(response)
        );

        SearchPipelineStats stats = searchPipelineService.stats();
        assertPipelineStats(stats.getTotalRequestStats(), 2, 1);
        assertPipelineStats(stats.getTotalResponseStats(), 2, 1);
        for (SearchPipelineStats.PerPipelineStats perPipelineStats : stats.getPipelineStats()) {
            SearchPipelineStats.PipelineDetailStats detailStats = stats.getPerPipelineProcessorStats()
                .get(perPipelineStats.getPipelineId());
            switch (perPipelineStats.getPipelineId()) {
                case "good_request_pipeline":
                    assertPipelineStats(perPipelineStats.getRequestStats(), 1, 0);
                    assertPipelineStats(perPipelineStats.getResponseStats(), 0, 0);
                    assertEquals(1, detailStats.requestProcessorStats().size());
                    assertEquals(0, detailStats.responseProcessorStats().size());
                    assertEquals("successful_request:2", detailStats.requestProcessorStats().get(0).getProcessorName());
                    assertEquals("successful_request", detailStats.requestProcessorStats().get(0).getProcessorType());
                    assertPipelineStats(detailStats.requestProcessorStats().get(0).getStats(), 1, 0);
                    break;
                case "bad_request_pipeline":
                    assertPipelineStats(perPipelineStats.getRequestStats(), 1, 1);
                    assertPipelineStats(perPipelineStats.getResponseStats(), 0, 0);
                    assertEquals(1, detailStats.requestProcessorStats().size());
                    assertEquals(0, detailStats.responseProcessorStats().size());
                    assertEquals("throwing_request:1", detailStats.requestProcessorStats().get(0).getProcessorName());
                    assertEquals("throwing_request", detailStats.requestProcessorStats().get(0).getProcessorType());
                    assertPipelineStats(detailStats.requestProcessorStats().get(0).getStats(), 1, 1);
                    break;
                case "good_response_pipeline":
                    assertPipelineStats(perPipelineStats.getRequestStats(), 0, 0);
                    assertPipelineStats(perPipelineStats.getResponseStats(), 1, 0);
                    assertEquals(0, detailStats.requestProcessorStats().size());
                    assertEquals(1, detailStats.responseProcessorStats().size());
                    assertEquals("successful_response:4", detailStats.responseProcessorStats().get(0).getProcessorName());
                    assertEquals("successful_response", detailStats.responseProcessorStats().get(0).getProcessorType());
                    assertPipelineStats(detailStats.responseProcessorStats().get(0).getStats(), 1, 0);
                    break;
                case "bad_response_pipeline":
                    assertPipelineStats(perPipelineStats.getRequestStats(), 0, 0);
                    assertPipelineStats(perPipelineStats.getResponseStats(), 1, 1);
                    assertEquals(0, detailStats.requestProcessorStats().size());
                    assertEquals(1, detailStats.responseProcessorStats().size());
                    assertEquals("throwing_response:3", detailStats.responseProcessorStats().get(0).getProcessorName());
                    assertEquals("throwing_response", detailStats.responseProcessorStats().get(0).getProcessorType());
                    assertPipelineStats(detailStats.responseProcessorStats().get(0).getStats(), 1, 1);
                    break;
            }
        }
    }

    public void testStatsEnabledIgnoreFailure() throws Exception {
        SearchRequestProcessor throwingRequestProcessor = new FakeRequestProcessor("throwing_request", "1", null, true, r -> {
            throw new RuntimeException();
        });
        Map<String, Processor.Factory<SearchRequestProcessor>> requestProcessorsEnableIgnoreFailure = Map.of(
            "successful_request",
            (pf, t, i, f, c, ctx) -> new FakeRequestProcessor("successful_request", "2", null, true, r -> {}),
            "throwing_request",
            (pf, t, i, f, c, ctx) -> throwingRequestProcessor
        );
        SearchResponseProcessor throwingResponseProcessor = new FakeResponseProcessor("throwing_response", "3", null, true, r -> {
            throw new RuntimeException();
        });
        Map<String, Processor.Factory<SearchResponseProcessor>> responseProcessorsEnableIgnoreFailure = Map.of(
            "successful_response",
            (pf, t, i, f, c, ctx) -> new FakeResponseProcessor("successful_response", "4", null, true, r -> {}),
            "throwing_response",
            (pf, t, i, f, c, ctx) -> throwingResponseProcessor
        );

        SearchPipelineService searchPipelineService = getSearchPipelineService(
            requestProcessorsEnableIgnoreFailure,
            responseProcessorsEnableIgnoreFailure
        );

        SearchRequest request = new SearchRequest();
        SearchResponse response = new SearchResponse(null, null, 0, 0, 0, 0, null, null);

        searchPipelineService.resolvePipeline(request.pipeline("good_request_pipeline")).transformResponse(response);
        // Caught Exception here
        searchPipelineService.resolvePipeline(request.pipeline("bad_request_pipeline")).transformResponse(response);
        searchPipelineService.resolvePipeline(request.pipeline("good_response_pipeline")).transformResponse(response);
        // Caught Exception here
        searchPipelineService.resolvePipeline(request.pipeline("bad_response_pipeline")).transformResponse(response);

        // when ignoreFailure enabled, the search pipelines will all succeed.
        SearchPipelineStats stats = searchPipelineService.stats();
        assertPipelineStats(stats.getTotalRequestStats(), 2, 0);
        assertPipelineStats(stats.getTotalResponseStats(), 2, 0);

        for (SearchPipelineStats.PerPipelineStats perPipelineStats : stats.getPipelineStats()) {
            SearchPipelineStats.PipelineDetailStats detailStats = stats.getPerPipelineProcessorStats()
                .get(perPipelineStats.getPipelineId());
            switch (perPipelineStats.getPipelineId()) {
                case "good_request_pipeline":
                    assertPipelineStats(perPipelineStats.getRequestStats(), 1, 0);
                    assertPipelineStats(perPipelineStats.getResponseStats(), 0, 0);
                    assertEquals(1, detailStats.requestProcessorStats().size());
                    assertEquals(0, detailStats.responseProcessorStats().size());
                    assertEquals("successful_request:2", detailStats.requestProcessorStats().get(0).getProcessorName());
                    assertEquals("successful_request", detailStats.requestProcessorStats().get(0).getProcessorType());
                    assertPipelineStats(detailStats.requestProcessorStats().get(0).getStats(), 1, 0);
                    break;
                case "bad_request_pipeline":
                    // pipeline succeed when ignore failure is true
                    assertPipelineStats(perPipelineStats.getRequestStats(), 1, 0);
                    assertPipelineStats(perPipelineStats.getResponseStats(), 0, 0);
                    assertEquals(1, detailStats.requestProcessorStats().size());
                    assertEquals(0, detailStats.responseProcessorStats().size());
                    assertEquals("throwing_request:1", detailStats.requestProcessorStats().get(0).getProcessorName());
                    assertEquals("throwing_request", detailStats.requestProcessorStats().get(0).getProcessorType());
                    // processor stats got 1 count and 1 failed
                    assertPipelineStats(detailStats.requestProcessorStats().get(0).getStats(), 1, 1);
                    break;
                case "good_response_pipeline":
                    assertPipelineStats(perPipelineStats.getRequestStats(), 0, 0);
                    assertPipelineStats(perPipelineStats.getResponseStats(), 1, 0);
                    assertEquals(0, detailStats.requestProcessorStats().size());
                    assertEquals(1, detailStats.responseProcessorStats().size());
                    assertEquals("successful_response:4", detailStats.responseProcessorStats().get(0).getProcessorName());
                    assertEquals("successful_response", detailStats.responseProcessorStats().get(0).getProcessorType());
                    assertPipelineStats(detailStats.responseProcessorStats().get(0).getStats(), 1, 0);
                    break;
                case "bad_response_pipeline":
                    // pipeline succeed when ignore failure is true
                    assertPipelineStats(perPipelineStats.getRequestStats(), 0, 0);
                    assertPipelineStats(perPipelineStats.getResponseStats(), 1, 0);
                    assertEquals(0, detailStats.requestProcessorStats().size());
                    assertEquals(1, detailStats.responseProcessorStats().size());
                    assertEquals("throwing_response:3", detailStats.responseProcessorStats().get(0).getProcessorName());
                    assertEquals("throwing_response", detailStats.responseProcessorStats().get(0).getProcessorType());
                    // processor stats got 1 count and 1 failed
                    assertPipelineStats(detailStats.responseProcessorStats().get(0).getStats(), 1, 1);
                    break;
            }
        }

    }

    private SearchPipelineService getSearchPipelineService(
        Map<String, Processor.Factory<SearchRequestProcessor>> requestProcessorsEnableIgnoreFailure,
        Map<String, Processor.Factory<SearchResponseProcessor>> responseProcessorsEnableIgnoreFailure
    ) {
        SearchPipelineService searchPipelineService = createWithProcessors(
            requestProcessorsEnableIgnoreFailure,
            responseProcessorsEnableIgnoreFailure,
            Collections.emptyMap()
        );

        SearchPipelineMetadata metadata = new SearchPipelineMetadata(
            Map.of(
                "good_response_pipeline",
                new PipelineConfiguration(
                    "good_response_pipeline",
                    new BytesArray("{\"response_processors\" : [ { \"successful_response\": {} } ] }"),
                    XContentType.JSON
                ),
                "bad_response_pipeline",
                new PipelineConfiguration(
                    "bad_response_pipeline",
                    new BytesArray("{\"response_processors\" : [ { \"throwing_response\": {} } ] }"),
                    XContentType.JSON
                ),
                "good_request_pipeline",
                new PipelineConfiguration(
                    "good_request_pipeline",
                    new BytesArray("{\"request_processors\" : [ { \"successful_request\": {} } ] }"),
                    XContentType.JSON
                ),
                "bad_request_pipeline",
                new PipelineConfiguration(
                    "bad_request_pipeline",
                    new BytesArray("{\"request_processors\" : [ { \"throwing_request\": {} } ] }"),
                    XContentType.JSON
                )
            )
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousState = clusterState;
        clusterState = ClusterState.builder(clusterState)
            .metadata(Metadata.builder().putCustom(SearchPipelineMetadata.TYPE, metadata))
            .build();
        searchPipelineService.applyClusterState(new ClusterChangedEvent("", clusterState, previousState));
        return searchPipelineService;
    }

    private static void assertPipelineStats(OperationStats stats, long count, long failed) {
        assertEquals(stats.getCount(), count);
        assertEquals(stats.getFailedCount(), failed);
    }

    public void testAdHocRejectingProcessor() {
        String processorType = "ad_hoc_rejecting";
        Map<String, Processor.Factory<SearchRequestProcessor>> requestProcessorFactories = Map.of(processorType, (pf, t, d, i, c, ctx) -> {
            if (ctx.getPipelineSource() == Processor.PipelineSource.SEARCH_REQUEST) {
                throw new IllegalArgumentException(processorType + " cannot be created as part of a pipeline defined in a search request");
            }
            return new FakeRequestProcessor(processorType, t, d, i, r -> {});
        });

        SearchPipelineService searchPipelineService = createWithProcessors(
            requestProcessorFactories,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        String id = "_id";
        SearchPipelineService.PipelineHolder pipeline = searchPipelineService.getPipelines().get(id);
        assertNull(pipeline);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        PutSearchPipelineRequest putRequest = new PutSearchPipelineRequest(
            id,
            new BytesArray("{\"request_processors\":[" + " { \"" + processorType + "\": {}}" + "]}"),
            XContentType.JSON
        );
        ClusterState previousClusterState = clusterState;
        clusterState = SearchPipelineService.innerPut(putRequest, clusterState);
        // The following line successfully creates the pipeline:
        searchPipelineService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        Map<String, Object> pipelineSourceMap = new HashMap<>();
        pipelineSourceMap.put(Pipeline.REQUEST_PROCESSORS_KEY, List.of(Map.of(processorType, Collections.emptyMap())));

        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource().searchPipelineSource(pipelineSourceMap);
        SearchRequest searchRequest = new SearchRequest().source(sourceBuilder);
        expectThrows(SearchPipelineProcessingException.class, () -> searchPipelineService.resolvePipeline(searchRequest));
    }

    public void testExtraParameterInProcessorConfig() {
        SearchPipelineService searchPipelineService = createWithProcessors();

        Map<String, Object> pipelineSourceMap = new HashMap<>();
        Map<String, Object> processorConfig = new HashMap<>(
            Map.of("score", 1.0f, "tag", "my_tag", "comment", "I just like to add extra parameters so that I feel like I'm being heard.")
        );
        pipelineSourceMap.put(Pipeline.RESPONSE_PROCESSORS_KEY, List.of(Map.of("fixed_score", processorConfig)));
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource().searchPipelineSource(pipelineSourceMap);
        SearchRequest searchRequest = new SearchRequest().source(sourceBuilder);
        try {
            searchPipelineService.resolvePipeline(searchRequest);
            fail("Exception should have been thrown");
        } catch (SearchPipelineProcessingException e) {
            assertTrue(
                e.getMessage()
                    .contains("processor [fixed_score:my_tag] doesn't support one or more provided configuration parameters: [comment]")
            );
        } catch (Exception e) {
            fail("Wrong exception type: " + e.getClass());
        }
    }
}
