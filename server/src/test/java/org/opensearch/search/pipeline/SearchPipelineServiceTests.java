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
import org.apache.lucene.search.TotalHits;
import org.opensearch.OpenSearchParseException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.Version;
import org.opensearch.action.search.DeleteSearchPipelineRequest;
import org.opensearch.action.search.PutSearchPipelineRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.metrics.OperationStats;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.plugins.SearchPipelinePlugin;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.MockLogAppender;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.junit.Before;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.opensearch.search.pipeline.SearchPipelineService.ENABLED_SYSTEM_GENERATED_FACTORIES_SETTING;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchPipelineServiceTests extends SearchPipelineTestCase {

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
    public void setup() throws Exception {
        super.setUp();
        threadPool = mock(ThreadPool.class);
        ExecutorService executorService = OpenSearchExecutors.newDirectExecutorService();
        when(threadPool.generic()).thenReturn(executorService);
        when(threadPool.executor(anyString())).thenReturn(executorService);
    }

    public void testSearchPipelinePlugin() {
        Client client = mock(Client.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        SearchPipelineService searchPipelineService = new SearchPipelineService(
            clusterService,
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
        Map<String, Processor.Factory<SearchPhaseResultsProcessor>> phaseInjectorProcessorFactories = searchPipelineService
            .getSearchPhaseResultsProcessorFactories();
        assertEquals(1, phaseInjectorProcessorFactories.size());
        assertTrue(phaseInjectorProcessorFactories.containsKey("zoe"));
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
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        SearchPipelineService searchPipelineService = new SearchPipelineService(
            clusterService,
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
            () -> searchPipelineService.resolvePipeline(searchRequest, indexNameExpressionResolver)
        );
        assertTrue(e.getMessage(), e.getMessage().contains(" not defined"));
    }

    public void testResolveIndexDefaultPipeline() throws Exception {
        SearchPipelineService service = createWithProcessors();
        setUpForResolvePipeline(service);

        SearchRequest searchRequest = new SearchRequest("my_index").source(SearchSourceBuilder.searchSource().size(5));
        PipelinedRequest pipelinedRequest = syncTransformRequest(service.resolvePipeline(searchRequest, indexNameExpressionResolver));
        assertEquals("p1", pipelinedRequest.getPipeline().getId());
        assertEquals(10, pipelinedRequest.source().size());

        // Bypass the default pipeline
        searchRequest = new SearchRequest("my_index").source(SearchSourceBuilder.searchSource().size(5)).pipeline("_none");
        pipelinedRequest = service.resolvePipeline(searchRequest, indexNameExpressionResolver);
        assertEquals("_none", pipelinedRequest.getPipeline().getId());
        assertEquals(5, pipelinedRequest.source().size());
        assertTrue(pipelinedRequest.getSystemGeneratedPipelineHolder().isNoOp());
    }

    public void testResolveSystemGeneratedSearchPipeline_whenHappyCase_thenSuccess() throws Exception {
        SearchPipelineService service = createWithSystemGeneratedProcessors();
        setUpForResolvePipeline(service);
        enabledAllSystemGeneratedFactories(service);

        SearchRequest searchRequest = new SearchRequest("my_index").source(SearchSourceBuilder.searchSource().size(5));
        PipelinedRequest pipelinedRequest = service.resolvePipeline(searchRequest, indexNameExpressionResolver);
        // user defined pipeline is resolved
        assertEquals("p1", pipelinedRequest.getPipeline().getId());
        // system generated pipelines are resolved
        SearchRequestProcessor systemGeneratedRequestProcessorPre = pipelinedRequest.getSystemGeneratedPipelineHolder()
            .prePipeline()
            .getSearchRequestProcessors()
            .getFirst();
        SearchRequestProcessor systemGeneratedRequestProcessorPost = pipelinedRequest.getSystemGeneratedPipelineHolder()
            .postPipeline()
            .getSearchRequestProcessors()
            .getFirst();
        SearchResponseProcessor systemGeneratedResponseProcessorPre = pipelinedRequest.getSystemGeneratedPipelineHolder()
            .prePipeline()
            .getSearchResponseProcessors()
            .getFirst();
        SearchResponseProcessor systemGeneratedResponseProcessorPost = pipelinedRequest.getSystemGeneratedPipelineHolder()
            .postPipeline()
            .getSearchResponseProcessors()
            .getFirst();
        SearchPhaseResultsProcessor systemGeneratedPhaseProcessorPre = pipelinedRequest.getSystemGeneratedPipelineHolder()
            .prePipeline()
            .getSearchPhaseResultsProcessors()
            .getFirst();
        SearchPhaseResultsProcessor systemGeneratedPhaseProcessorPost = pipelinedRequest.getSystemGeneratedPipelineHolder()
            .postPipeline()
            .getSearchPhaseResultsProcessors()
            .getFirst();
        assertTrue(systemGeneratedRequestProcessorPre instanceof FakeSystemGeneratedRequestPreProcessor);
        assertTrue(systemGeneratedRequestProcessorPost instanceof FakeSystemGeneratedRequestPostProcessor);
        assertTrue(systemGeneratedResponseProcessorPre instanceof FakeSystemGeneratedResponsePreProcessor);
        assertTrue(systemGeneratedResponseProcessorPost instanceof FakeSystemGeneratedResponsePostProcessor);
        assertTrue(systemGeneratedPhaseProcessorPre instanceof FakeSystemGeneratedSearchPhaseResultsPreProcessor);
        assertTrue(systemGeneratedPhaseProcessorPost instanceof FakeSystemGeneratedSearchPhaseResultsPostProcessor);
    }

    public void testResolveSystemGeneratedSearchPipeline_whenMoreThanOneProcessorPerStage_thenException() throws Exception {
        Map<String, Processor.Factory<SearchRequestProcessor>> requestProcessors = new HashMap<>();
        requestProcessors.put("scale_request_size", new ScaleRequestSizeFactory());

        Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchRequestProcessor>> systemGeneratedRequestProcessors =
            new HashMap<>();
        systemGeneratedRequestProcessors.put("system_scale_request_size", new SystemGeneratedScaleRequestSizePreFactory());
        // Add the factory again to generate two processors in the same execution stage
        systemGeneratedRequestProcessors.put("system_scale_request_size_1", new SystemGeneratedScaleRequestSizePreFactory());

        SearchPipelineService service = createWithProcessors(
            requestProcessors,
            Collections.emptyMap(),
            Collections.emptyMap(),
            systemGeneratedRequestProcessors,
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        setUpForResolvePipeline(service);
        enabledAllSystemGeneratedFactories(service);

        SearchRequest searchRequest = new SearchRequest("my_index").source(SearchSourceBuilder.searchSource().size(5));
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> service.resolvePipeline(searchRequest, indexNameExpressionResolver)
        );

        String expectedError =
            "Cannot support more than one system generated SEARCH_REQUEST processor to be executed PRE_USER_DEFINED. Now we have [system_scale_request_size_pre,system_scale_request_size_pre].";
        assertEquals(expectedError, exception.getMessage());
    }

    public void testResolveSystemGeneratedSearchPipeline_whenNotMeetCondition_thenNoGeneration() throws Exception {
        SearchPipelineService service = createWithSystemGeneratedProcessors();
        setUpForResolvePipeline(service);
        enabledAllSystemGeneratedFactories(service);

        // set a large size to not meet the condition to generate the system generated request processor
        SearchRequest searchRequest = new SearchRequest("my_index").source(SearchSourceBuilder.searchSource().size(20));
        PipelinedRequest pipelinedRequest = service.resolvePipeline(searchRequest, indexNameExpressionResolver);

        // verify the system generated request processor is not generated
        assertTrue(
            "Should not systematically generate the search request processor",
            pipelinedRequest.getSystemGeneratedPipelineHolder().prePipeline().getSearchRequestProcessors().isEmpty()
        );
    }

    public void testResolveSystemGeneratedSearchPipeline_whenDisableSystemGeneratedFactories_thenNoGeneration() throws Exception {
        SearchPipelineService service = createWithSystemGeneratedProcessors();
        setUpForResolvePipeline(service);

        // set a large size to not meet the condition to generate the system generated request processor
        SearchRequest searchRequest = new SearchRequest("my_index").source(SearchSourceBuilder.searchSource().size(5));
        PipelinedRequest pipelinedRequest = service.resolvePipeline(searchRequest, indexNameExpressionResolver);

        // verify no system generated processors
        assertTrue(
            "Should not systematically generate the search processors",
            pipelinedRequest.getSystemGeneratedPipelineHolder().isNoOp()
        );
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
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
            MediaTypeRegistry.JSON
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

        PutSearchPipelineRequest putRequest = new PutSearchPipelineRequest(id, new BytesArray("{}"), MediaTypeRegistry.JSON);
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
        putRequest = new PutSearchPipelineRequest(id, new BytesArray("{ \"description\": \"empty pipeline\"}"), MediaTypeRegistry.JSON);
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
            MediaTypeRegistry.JSON
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
            MediaTypeRegistry.JSON
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
                new PipelineConfiguration("p1", definition, MediaTypeRegistry.JSON),
                "p2",
                new PipelineConfiguration("p2", definition, MediaTypeRegistry.JSON),
                "q1",
                new PipelineConfiguration("q1", definition, MediaTypeRegistry.JSON)
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

    public void testGetPipelines() {
        //
        assertEquals(0, SearchPipelineService.innerGetPipelines(null, "p1").size());

        SearchPipelineMetadata metadata = new SearchPipelineMetadata(
            Map.of(
                "p1",
                new PipelineConfiguration(
                    "p1",
                    new BytesArray("{\"request_processors\" : [ { \"scale_request_size\": { \"scale\" : 2 } } ] }"),
                    MediaTypeRegistry.JSON
                ),
                "p2",
                new PipelineConfiguration(
                    "p2",
                    new BytesArray("{\"response_processors\" : [ { \"fixed_score\": { \"score\" : 2 } } ] }"),
                    MediaTypeRegistry.JSON
                ),
                "p3",
                new PipelineConfiguration(
                    "p3",
                    new BytesArray("{\"phase_results_processors\" : [ { \"max_score\" : { } } ]}"),
                    MediaTypeRegistry.JSON
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
        ProcessorInfo searchPhaseProcessor = new ProcessorInfo("max_score");
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
            MediaTypeRegistry.JSON
        );

        String longId = "a".repeat(512) + "a";
        PutSearchPipelineRequest maxLengthIdPutRequest = new PutSearchPipelineRequest(
            longId,
            new BytesArray("{\"request_processors\" : [ { \"scale_request_size\": { \"scale\" : \"foo\" } } ] }"),
            MediaTypeRegistry.JSON
        );

        SearchPipelineInfo completePipelineInfo = new SearchPipelineInfo(
            Map.of(
                Pipeline.REQUEST_PROCESSORS_KEY,
                List.of(reqProcessor),
                Pipeline.RESPONSE_PROCESSORS_KEY,
                List.of(rspProcessor),
                Pipeline.PHASE_PROCESSORS_KEY,
                List.of(searchPhaseProcessor)
            )
        );
        SearchPipelineInfo incompletePipelineInfo = new SearchPipelineInfo(Map.of(Pipeline.REQUEST_PROCESSORS_KEY, List.of(reqProcessor)));
        // One node is missing a processor
        expectThrows(
            OpenSearchParseException.class,
            () -> searchPipelineService.validatePipeline(Map.of(n1, completePipelineInfo, n2, incompletePipelineInfo), putRequest)
        );

        // Discovery failed, no infos passed.
        expectThrows(IllegalStateException.class, () -> searchPipelineService.validatePipeline(Collections.emptyMap(), putRequest));

        // Max length of pipeline length
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> searchPipelineService.validatePipeline(Map.of(n1, completePipelineInfo), maxLengthIdPutRequest)
        );
        String errorMessage = String.format(
            Locale.ROOT,
            "Search Pipeline id [%s] exceeds maximum length of 512 UTF-8 bytes (actual: 513 bytes)",
            longId
        );
        assertEquals(errorMessage, e.getMessage());

        // Invalid configuration in request
        PutSearchPipelineRequest badPutRequest = new PutSearchPipelineRequest(
            "p1",
            new BytesArray(
                "{"
                    + "\"request_processors\": [{ \"scale_request_size\": { \"scale\" : \"banana\" } }],"
                    + "\"response_processors\": [{ \"fixed_score\": { \"score\" : 2 } }]"
                    + "}"
            ),
            MediaTypeRegistry.JSON
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
        PipelinedRequest pipelinedRequest = syncTransformRequest(
            searchPipelineService.resolvePipeline(searchRequest, indexNameExpressionResolver)
        );
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

        SearchResponse transformedResponse = syncTransformResponse(pipelinedRequest, searchResponse);
        for (int i = 0; i < size; i++) {
            assertEquals(2.0, transformedResponse.getHits().getHits()[i].getScore(), 0.0001);
        }
    }

    public void testInlineDefinedPipeline() throws Exception {
        SearchPipelineService searchPipelineService = createWithProcessors();

        SearchPipelineMetadata metadata = new SearchPipelineMetadata(
            Map.of(
                "p1",
                new PipelineConfiguration(
                    "p1",
                    new BytesArray(
                        "{"
                            + "\"request_processors\": [{ \"scale_request_size\": { \"scale\" : 2 } }],"
                            + "\"response_processors\": [{ \"fixed_score\": { \"score\" : 2 } }]"
                            + "}"
                    ),
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

        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource().size(100).pipeline("p1");
        SearchRequest searchRequest = new SearchRequest().source(sourceBuilder);
        searchRequest.pipeline(searchRequest.source().pipeline());

        // Verify pipeline
        PipelinedRequest pipelinedRequest = syncTransformRequest(
            searchPipelineService.resolvePipeline(searchRequest, indexNameExpressionResolver)
        );
        Pipeline pipeline = pipelinedRequest.getPipeline();
        assertEquals("p1", pipeline.getId());
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

        SearchResponse transformedResponse = syncTransformResponse(pipelinedRequest, searchResponse);
        for (int i = 0; i < size; i++) {
            assertEquals(2.0, transformedResponse.getHits().getHits()[i].getScore(), 0.0001);
        }
    }

    public void testInfo() {
        SearchPipelineService searchPipelineService = createWithProcessors();
        SearchPipelineInfo info = searchPipelineService.info();
        assertTrue(info.containsProcessor(Pipeline.REQUEST_PROCESSORS_KEY, "scale_request_size"));
        assertTrue(info.containsProcessor(Pipeline.RESPONSE_PROCESSORS_KEY, "fixed_score"));
        assertTrue(info.containsProcessor(Pipeline.PHASE_PROCESSORS_KEY, "max_score"));
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
        expectThrows(
            SearchPipelineProcessingException.class,
            () -> searchPipelineService.resolvePipeline(searchRequest, indexNameExpressionResolver)
        );

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
        expectThrows(
            SearchPipelineProcessingException.class,
            () -> syncTransformRequest(searchPipelineService.resolvePipeline(searchRequest, indexNameExpressionResolver))
        );
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

        PipelinedRequest pipelinedRequest = searchPipelineService.resolvePipeline(searchRequest, indexNameExpressionResolver);

        SearchResponse response = createDefaultSearchResponse();
        // Exception thrown when processing response
        expectThrows(SearchPipelineProcessingException.class, () -> syncTransformResponse(pipelinedRequest, response));
    }

    public void testCatchExceptionOnRequestProcessing() throws Exception {
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
            syncTransformRequest(searchPipelineService.resolvePipeline(searchRequest, indexNameExpressionResolver));
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

        PipelinedRequest pipelinedRequest = searchPipelineService.resolvePipeline(searchRequest, indexNameExpressionResolver);

        SearchResponse response = createDefaultSearchResponse();

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
            syncTransformResponse(pipelinedRequest, response);
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
        request.source(createDefaultSearchSourceBuilder());
        SearchResponse response = createDefaultSearchResponse();

        syncExecutePipeline(
            searchPipelineService.resolvePipeline(request.pipeline("good_request_pipeline"), indexNameExpressionResolver),
            response
        );
        expectThrows(
            SearchPipelineProcessingException.class,
            () -> syncExecutePipeline(
                searchPipelineService.resolvePipeline(request.pipeline("bad_request_pipeline"), indexNameExpressionResolver),
                response
            )
        );
        syncExecutePipeline(
            searchPipelineService.resolvePipeline(request.pipeline("good_response_pipeline"), indexNameExpressionResolver),
            response
        );
        expectThrows(
            SearchPipelineProcessingException.class,
            () -> syncExecutePipeline(
                searchPipelineService.resolvePipeline(request.pipeline("bad_response_pipeline"), indexNameExpressionResolver),
                response
            )
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
        request.source(createDefaultSearchSourceBuilder());
        SearchResponse response = createDefaultSearchResponse();

        syncExecutePipeline(
            searchPipelineService.resolvePipeline(request.pipeline("good_request_pipeline"), indexNameExpressionResolver),
            response
        );
        // Caught Exception here
        syncExecutePipeline(
            searchPipelineService.resolvePipeline(request.pipeline("bad_request_pipeline"), indexNameExpressionResolver),
            response
        );
        syncExecutePipeline(
            searchPipelineService.resolvePipeline(request.pipeline("good_response_pipeline"), indexNameExpressionResolver),
            response
        );
        // Caught Exception here
        syncExecutePipeline(
            searchPipelineService.resolvePipeline(request.pipeline("bad_response_pipeline"), indexNameExpressionResolver),
            response
        );

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
                    MediaTypeRegistry.JSON
                ),
                "bad_response_pipeline",
                new PipelineConfiguration(
                    "bad_response_pipeline",
                    new BytesArray("{\"response_processors\" : [ { \"throwing_response\": {} } ] }"),
                    MediaTypeRegistry.JSON
                ),
                "good_request_pipeline",
                new PipelineConfiguration(
                    "good_request_pipeline",
                    new BytesArray("{\"request_processors\" : [ { \"successful_request\": {} } ] }"),
                    MediaTypeRegistry.JSON
                ),
                "bad_request_pipeline",
                new PipelineConfiguration(
                    "bad_request_pipeline",
                    new BytesArray("{\"request_processors\" : [ { \"throwing_request\": {} } ] }"),
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
        return searchPipelineService;
    }

    private static void assertPipelineStats(OperationStats stats, long count, long failed) {
        assertEquals(count, stats.getCount());
        assertEquals(failed, stats.getFailedCount());
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
            MediaTypeRegistry.JSON
        );
        ClusterState previousClusterState = clusterState;
        clusterState = SearchPipelineService.innerPut(putRequest, clusterState);
        // The following line successfully creates the pipeline:
        searchPipelineService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        Map<String, Object> pipelineSourceMap = new HashMap<>();
        pipelineSourceMap.put(Pipeline.REQUEST_PROCESSORS_KEY, List.of(Map.of(processorType, Collections.emptyMap())));

        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource().searchPipelineSource(pipelineSourceMap);
        SearchRequest searchRequest = new SearchRequest().source(sourceBuilder);
        expectThrows(
            SearchPipelineProcessingException.class,
            () -> searchPipelineService.resolvePipeline(searchRequest, indexNameExpressionResolver)
        );
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
            searchPipelineService.resolvePipeline(searchRequest, indexNameExpressionResolver);
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

    private static class FakeStatefulRequestProcessor extends AbstractProcessor implements StatefulSearchRequestProcessor {
        private final String type;
        private final Consumer<PipelineProcessingContext> stateConsumer;

        public FakeStatefulRequestProcessor(String type, Consumer<PipelineProcessingContext> stateConsumer) {
            super(null, null, false);
            this.type = type;
            this.stateConsumer = stateConsumer;
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public SearchRequest processRequest(SearchRequest request, PipelineProcessingContext requestContext) throws Exception {
            stateConsumer.accept(requestContext);
            return request;
        }
    }

    private static class FakeStatefulResponseProcessor extends AbstractProcessor implements StatefulSearchResponseProcessor {
        private final String type;
        private final Consumer<PipelineProcessingContext> stateConsumer;

        public FakeStatefulResponseProcessor(String type, Consumer<PipelineProcessingContext> stateConsumer) {
            super(null, null, false);
            this.type = type;
            this.stateConsumer = stateConsumer;
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public SearchResponse processResponse(SearchRequest request, SearchResponse response, PipelineProcessingContext requestContext)
            throws Exception {
            stateConsumer.accept(requestContext);
            return response;
        }
    }

    public void testStatefulProcessors() throws Exception {
        AtomicReference<String> contextHolder = new AtomicReference<>();
        SearchPipelineService searchPipelineService = createWithProcessors(
            Map.of(
                "write_context",
                (pf, t, d, igf, cfg, ctx) -> new FakeStatefulRequestProcessor("write_context", (c) -> c.setAttribute("a", "b"))
            ),
            Map.of(
                "read_context",
                (pf, t, d, igf, cfg, ctx) -> new FakeStatefulResponseProcessor(
                    "read_context",
                    (c) -> contextHolder.set((String) c.getAttribute("a"))
                )
            ),
            Collections.emptyMap()
        );

        SearchPipelineMetadata metadata = new SearchPipelineMetadata(
            Map.of(
                "p1",
                new PipelineConfiguration(
                    "p1",
                    new BytesArray(
                        "{\"request_processors\" : [ { \"write_context\": {} } ], \"response_processors\": [ { \"read_context\": {} }] }"
                    ),
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

        PipelinedRequest request = searchPipelineService.resolvePipeline(
            new SearchRequest().source(createDefaultSearchSourceBuilder()).pipeline("p1"),
            indexNameExpressionResolver
        );
        assertNull(contextHolder.get());
        syncExecutePipeline(request, createDefaultSearchResponse());
        assertNotNull(contextHolder.get());
        assertEquals("b", contextHolder.get());
    }

    public void testDefaultPipelineForMultipleIndices() throws Exception {
        SearchPipelineService service = createWithProcessors();

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

        Settings defaultPipelineSetting = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            .put(IndexSettings.DEFAULT_SEARCH_PIPELINE.getKey(), "p1")
            .build();

        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("foo", defaultPipelineSetting).putAlias(AliasMetadata.builder("bar")))
            .put(indexBuilder("foobar", defaultPipelineSetting).putAlias(AliasMetadata.builder("bar")))
            .put(indexBuilder("foofoo-closed", defaultPipelineSetting).putAlias(AliasMetadata.builder("bar")))
            .put(indexBuilder("foofoo", defaultPipelineSetting).putAlias(AliasMetadata.builder("bar")));

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousState = clusterState;

        clusterState = ClusterState.builder(clusterState).metadata(mdBuilder.putCustom(SearchPipelineMetadata.TYPE, metadata)).build();

        ClusterChangedEvent cce = new ClusterChangedEvent("", clusterState, previousState);
        service.applyClusterState(cce);

        SearchRequest searchRequest = new SearchRequest("bar").source(SearchSourceBuilder.searchSource().size(5));
        PipelinedRequest pipelinedRequest = syncTransformRequest(service.resolvePipeline(searchRequest, indexNameExpressionResolver));
        assertEquals("p1", pipelinedRequest.getPipeline().getId());
        assertEquals(10, pipelinedRequest.source().size());

        // Bypass the default pipeline
        searchRequest = new SearchRequest("bar").source(SearchSourceBuilder.searchSource().size(5)).pipeline("_none");
        pipelinedRequest = service.resolvePipeline(searchRequest, indexNameExpressionResolver);
        assertEquals("_none", pipelinedRequest.getPipeline().getId());
        assertEquals(5, pipelinedRequest.source().size());
    }

    public void testDifferentDefaultPipelineForMultipleIndices() throws Exception {
        SearchPipelineService service = createWithProcessors();

        SearchPipelineMetadata metadata = new SearchPipelineMetadata(
            Map.of(
                "p1",
                new PipelineConfiguration(
                    "p1",
                    new BytesArray("{\"request_processors\" : [ { \"scale_request_size\": { \"scale\" : 2 } } ] }"),
                    MediaTypeRegistry.JSON
                ),

                "p2",
                new PipelineConfiguration(
                    "p2",
                    new BytesArray("{\"request_processors\" : [ { \"scale_request_size\": { \"scale\" : 1 } } ] }"),
                    MediaTypeRegistry.JSON
                )
            )
        );

        Settings defaultPipelineSetting1 = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            .put(IndexSettings.DEFAULT_SEARCH_PIPELINE.getKey(), "p1")
            .build();

        Settings defaultPipelineSetting2 = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            .put(IndexSettings.DEFAULT_SEARCH_PIPELINE.getKey(), "p2")
            .build();

        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("foo", defaultPipelineSetting1).putAlias(AliasMetadata.builder("bar")))
            .put(indexBuilder("foobar", defaultPipelineSetting1).putAlias(AliasMetadata.builder("bar")))
            .put(indexBuilder("foofoo-closed", defaultPipelineSetting1).putAlias(AliasMetadata.builder("bar")))
            .put(indexBuilder("foofoo", defaultPipelineSetting2).putAlias(AliasMetadata.builder("bar")));

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousState = clusterState;

        clusterState = ClusterState.builder(clusterState).metadata(mdBuilder.putCustom(SearchPipelineMetadata.TYPE, metadata)).build();

        ClusterChangedEvent cce = new ClusterChangedEvent("", clusterState, previousState);
        service.applyClusterState(cce);

        SearchRequest searchRequest = new SearchRequest("bar").source(SearchSourceBuilder.searchSource().size(5));
        PipelinedRequest pipelinedRequest = syncTransformRequest(service.resolvePipeline(searchRequest, indexNameExpressionResolver));
        assertEquals("_none", pipelinedRequest.getPipeline().getId());
        assertEquals(5, pipelinedRequest.source().size());
    }

    public void testNoIndexResolveIndexDefaultPipeline() throws Exception {
        SearchPipelineService service = createWithProcessors();

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

        SearchRequest searchRequest = new SearchRequest().source(SearchSourceBuilder.searchSource().size(5));
        PipelinedRequest pipelinedRequest = syncTransformRequest(service.resolvePipeline(searchRequest, indexNameExpressionResolver));
        assertEquals("_none", pipelinedRequest.getPipeline().getId());
        assertEquals(5, pipelinedRequest.source().size());
    }

    public void testInvalidIndexResolveIndexDefaultPipeline() throws Exception {
        SearchPipelineService service = createWithProcessors();

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

        SearchRequest searchRequest = new SearchRequest("xyz").source(SearchSourceBuilder.searchSource().size(5));
        PipelinedRequest pipelinedRequest = syncTransformRequest(service.resolvePipeline(searchRequest, indexNameExpressionResolver));
        assertEquals("_none", pipelinedRequest.getPipeline().getId());
        assertEquals(5, pipelinedRequest.source().size());
    }

    public void testVerbosePipelineExecution() throws Exception {
        SearchPipelineService searchPipelineService = createWithProcessors();

        SearchPipelineMetadata metadata = new SearchPipelineMetadata(
            Map.of(
                "verbose_pipeline",
                new PipelineConfiguration(
                    "verbose_pipeline",
                    new BytesArray(
                        "{"
                            + "\"request_processors\" : [ { \"scale_request_size\": { \"scale\" : 2 } } ],"
                            + "\"response_processors\": [ { \"fixed_score\": { \"score\": 5.0 } } ]"
                            + "}"
                    ),
                    MediaTypeRegistry.JSON
                )
            )
        );

        ClusterState initialState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState updatedState = ClusterState.builder(initialState)
            .metadata(Metadata.builder().putCustom(SearchPipelineMetadata.TYPE, metadata))
            .build();

        searchPipelineService.applyClusterState(new ClusterChangedEvent("clusterStateUpdated", updatedState, initialState));

        SearchRequest searchRequest = new SearchRequest().source(SearchSourceBuilder.searchSource().size(10)).pipeline("verbose_pipeline");
        searchRequest.source().verbosePipeline(true);

        PipelinedRequest pipelinedRequest = syncTransformRequest(
            searchPipelineService.resolvePipeline(searchRequest, indexNameExpressionResolver)
        );

        SearchResponseSections sections = new SearchResponseSections(
            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f),
            null,
            null,
            false,
            null,
            null,
            1,
            List.of(),
            List.of()
        );

        SearchResponse searchResponse = new SearchResponse(sections, null, 0, 0, 0, 0, null, null);
        SearchResponse transformedResponse = syncTransformResponse(pipelinedRequest, searchResponse);
        List<ProcessorExecutionDetail> executionDetails = transformedResponse.getInternalResponse().getProcessorResult();

        assertNotNull(executionDetails);
        assertEquals(2, executionDetails.size());
        assertEquals("scale_request_size", executionDetails.get(0).getProcessorName());
        assertEquals("fixed_score", executionDetails.get(1).getProcessorName());
    }

    public void testVerbosePipelineWithoutDefinedPipelineThrowsException() {
        SearchPipelineService searchPipelineService = createWithProcessors();

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(SearchSourceBuilder.searchSource().verbosePipeline(true));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> searchPipelineService.resolvePipeline(searchRequest, indexNameExpressionResolver)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("The 'verbose pipeline' option requires a search pipeline to be defined."));
    }

    public void testVerbosePipelineWithRequestProcessorOnly() throws Exception {
        SearchPipelineService searchPipelineService = createWithProcessors();

        SearchPipelineMetadata metadata = new SearchPipelineMetadata(
            Map.of(
                "request_only_pipeline",
                new PipelineConfiguration(
                    "request_only_pipeline",
                    new BytesArray("{" + "\"request_processors\" : [ { \"scale_request_size\": { \"scale\" : 2 } } ]" + "}"),
                    MediaTypeRegistry.JSON
                )
            )
        );

        ClusterState initialState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState updatedState = ClusterState.builder(initialState)
            .metadata(Metadata.builder().putCustom(SearchPipelineMetadata.TYPE, metadata))
            .build();

        searchPipelineService.applyClusterState(new ClusterChangedEvent("clusterStateUpdated", updatedState, initialState));

        SearchRequest searchRequest = new SearchRequest().source(SearchSourceBuilder.searchSource().size(10))
            .pipeline("request_only_pipeline");
        searchRequest.source().verbosePipeline(true);

        PipelinedRequest pipelinedRequest = syncTransformRequest(
            searchPipelineService.resolvePipeline(searchRequest, indexNameExpressionResolver)
        );

        SearchResponseSections sections = new SearchResponseSections(
            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f),
            null,
            null,
            false,
            null,
            null,
            1,
            List.of(),
            List.of()
        );

        SearchResponse searchResponse = new SearchResponse(sections, null, 0, 0, 0, 0, null, null);
        SearchResponse transformedResponse = syncTransformResponse(pipelinedRequest, searchResponse);
        List<ProcessorExecutionDetail> executionDetails = transformedResponse.getInternalResponse().getProcessorResult();

        assertNotNull(executionDetails);
        assertEquals(1, executionDetails.size());
        assertEquals("scale_request_size", executionDetails.get(0).getProcessorName());
    }

    public void testVerbosePipelineWithResponseProcessorOnly() throws Exception {
        SearchPipelineService searchPipelineService = createWithProcessors();

        SearchPipelineMetadata metadata = new SearchPipelineMetadata(
            Map.of(
                "response_only_pipeline",
                new PipelineConfiguration(
                    "response_only_pipeline",
                    new BytesArray("{" + "\"response_processors\": [ { \"fixed_score\": { \"score\": 5.0 } } ]" + "}"),
                    MediaTypeRegistry.JSON
                )
            )
        );

        ClusterState initialState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState updatedState = ClusterState.builder(initialState)
            .metadata(Metadata.builder().putCustom(SearchPipelineMetadata.TYPE, metadata))
            .build();

        searchPipelineService.applyClusterState(new ClusterChangedEvent("clusterStateUpdated", updatedState, initialState));

        SearchRequest searchRequest = new SearchRequest().source(SearchSourceBuilder.searchSource().size(10))
            .pipeline("response_only_pipeline");
        searchRequest.source().verbosePipeline(true);

        PipelinedRequest pipelinedRequest = syncTransformRequest(
            searchPipelineService.resolvePipeline(searchRequest, indexNameExpressionResolver)
        );

        SearchResponseSections sections = new SearchResponseSections(
            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f),
            null,
            null,
            false,
            null,
            null,
            1,
            List.of(),
            List.of()
        );

        SearchResponse searchResponse = new SearchResponse(sections, null, 0, 0, 0, 0, null, null);
        SearchResponse transformedResponse = syncTransformResponse(pipelinedRequest, searchResponse);
        List<ProcessorExecutionDetail> executionDetails = transformedResponse.getInternalResponse().getProcessorResult();

        assertNotNull(executionDetails);
        assertEquals(1, executionDetails.size());
        assertEquals("fixed_score", executionDetails.get(0).getProcessorName());
    }

    private SearchResponse createDefaultSearchResponse() {
        SearchHits searchHits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f);

        SearchResponseSections sections = new SearchResponseSections(searchHits, null, null, false, null, null, 1, List.of(), List.of());

        return new SearchResponse(sections, null, 0, 0, 0, 0, null, null);
    }

    public void testIsSystemGeneratedFactoryEnabled_whenAllEnabled_thenTrue() throws Exception {
        SearchPipelineService service = createWithSystemGeneratedProcessors();
        enabledAllSystemGeneratedFactories(service);

        assertTrue(service.isSystemGeneratedFactoryEnabled("dummy_factory"));
    }

    public void testIsSystemGeneratedFactoryEnabled_whenEnabled_thenTrue() {
        SearchPipelineService service = createWithSystemGeneratedProcessors();
        service.getClusterService()
            .getClusterSettings()
            .applySettings(Settings.builder().putList(ENABLED_SYSTEM_GENERATED_FACTORIES_SETTING.getKey(), "dummy_factory").build());

        assertTrue(service.isSystemGeneratedFactoryEnabled("dummy_factory"));
    }

    public void testIsSystemGeneratedFactoryEnabled_whenNotEnabled_thenFalse() {
        SearchPipelineService service = createWithSystemGeneratedProcessors();

        assertFalse(service.isSystemGeneratedFactoryEnabled("dummy_factory"));
    }
}
