/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.Version;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.plugins.SearchPipelinePlugin;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static org.opensearch.search.pipeline.SearchPipelineService.ALL;
import static org.opensearch.search.pipeline.SearchPipelineService.ENABLED_SYSTEM_GENERATED_FACTORIES_SETTING;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class SearchPipelineTestCase extends OpenSearchTestCase {
    IndexNameExpressionResolver indexNameExpressionResolver;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        indexNameExpressionResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
    }

    SearchPipelineService createWithProcessors() {
        ProcessorSets base = buildBaseProcessors();
        return createWithProcessors(base.request, base.response, base.phase);
    }

    SearchPipelineService createWithSystemGeneratedProcessors() {
        ProcessorSets base = buildBaseProcessors();
        SystemProcessorSets system = buildSystemProcessors();

        return createWithProcessors(base.request, base.response, base.phase, system.request, system.response, system.phase);
    }

    SearchPipelineService createWithProcessors(
        Map<String, Processor.Factory<SearchRequestProcessor>> requestProcessors,
        Map<String, Processor.Factory<SearchResponseProcessor>> responseProcessors,
        Map<String, Processor.Factory<SearchPhaseResultsProcessor>> phaseProcessors
    ) {
        return createWithProcessors(
            requestProcessors,
            responseProcessors,
            phaseProcessors,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
    }

    SearchPipelineService createWithProcessors(
        Map<String, Processor.Factory<SearchRequestProcessor>> requestProcessors,
        Map<String, Processor.Factory<SearchResponseProcessor>> responseProcessors,
        Map<String, Processor.Factory<SearchPhaseResultsProcessor>> phaseProcessors,
        Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchRequestProcessor>> systemGeneratedRequestProcessors,
        Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchResponseProcessor>> systemGeneratedResponseProcessors,
        Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchPhaseResultsProcessor>> systemGeneratedPhaseResultsProcessors
    ) {
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executorService = OpenSearchExecutors.newDirectExecutorService();
        when(threadPool.generic()).thenReturn(executorService);
        when(threadPool.executor(anyString())).thenReturn(executorService);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        return new SearchPipelineService(
            clusterService,
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

                @Override
                public
                    Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchResponseProcessor>>
                    getSystemGeneratedResponseProcessors(Parameters parameters) {
                    return systemGeneratedResponseProcessors;
                }

                @Override
                public
                    Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchRequestProcessor>>
                    getSystemGeneratedRequestProcessors(Parameters parameters) {
                    return systemGeneratedRequestProcessors;
                }

                @Override
                public
                    Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchPhaseResultsProcessor>>
                    getSystemGeneratedSearchPhaseResultsProcessors(Parameters parameters) {
                    return systemGeneratedPhaseResultsProcessors;
                }
            }),
            client
        );
    }

    static class FakeRequestProcessor extends AbstractProcessor implements SearchRequestProcessor {
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

    static class FakeSystemGeneratedRequestPreProcessor extends FakeRequestProcessor implements SystemGeneratedProcessor {
        public FakeSystemGeneratedRequestPreProcessor(
            String type,
            String tag,
            String description,
            boolean ignoreFailure,
            Consumer<SearchRequest> executor
        ) {
            super(type, tag, description, ignoreFailure, executor);
        }

        @Override
        public ExecutionStage getExecutionStage() {
            return ExecutionStage.PRE_USER_DEFINED;
        }

    }

    static class FakeSystemGeneratedRequestPostProcessor extends FakeRequestProcessor implements SystemGeneratedProcessor {
        public FakeSystemGeneratedRequestPostProcessor(
            String type,
            String tag,
            String description,
            boolean ignoreFailure,
            Consumer<SearchRequest> executor
        ) {
            super(type, tag, description, ignoreFailure, executor);
        }

        @Override
        public ExecutionStage getExecutionStage() {
            return ExecutionStage.POST_USER_DEFINED;
        }

    }

    static class FakeResponseProcessor extends AbstractProcessor implements SearchResponseProcessor {
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

    static class FakeSystemGeneratedResponsePreProcessor extends FakeResponseProcessor implements SystemGeneratedProcessor {

        public FakeSystemGeneratedResponsePreProcessor(
            String type,
            String tag,
            String description,
            boolean ignoreFailure,
            Consumer<SearchResponse> executor
        ) {
            super(type, tag, description, ignoreFailure, executor);
        }

        @Override
        public ExecutionStage getExecutionStage() {
            return ExecutionStage.PRE_USER_DEFINED;
        }

    }

    static class FakeSystemGeneratedResponsePostProcessor extends FakeResponseProcessor implements SystemGeneratedProcessor {

        public FakeSystemGeneratedResponsePostProcessor(
            String type,
            String tag,
            String description,
            boolean ignoreFailure,
            Consumer<SearchResponse> executor
        ) {
            super(type, tag, description, ignoreFailure, executor);
        }

        @Override
        public ExecutionStage getExecutionStage() {
            return ExecutionStage.POST_USER_DEFINED;
        }

    }

    static class FakeSearchPhaseResultsProcessor extends AbstractProcessor implements SearchPhaseResultsProcessor {
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

    static class FakeSystemGeneratedSearchPhaseResultsPreProcessor extends FakeSearchPhaseResultsProcessor
        implements
            SystemGeneratedProcessor {

        public FakeSystemGeneratedSearchPhaseResultsPreProcessor(
            String type,
            String tag,
            String description,
            boolean ignoreFailure,
            Consumer<SearchPhaseResult> querySearchResultConsumer
        ) {
            super(type, tag, description, ignoreFailure, querySearchResultConsumer);
        }

        @Override
        public ExecutionStage getExecutionStage() {
            return ExecutionStage.PRE_USER_DEFINED;
        }

    }

    static class FakeSystemGeneratedSearchPhaseResultsPostProcessor extends FakeSearchPhaseResultsProcessor
        implements
            SystemGeneratedProcessor {

        public FakeSystemGeneratedSearchPhaseResultsPostProcessor(
            String type,
            String tag,
            String description,
            boolean ignoreFailure,
            Consumer<SearchPhaseResult> querySearchResultConsumer
        ) {
            super(type, tag, description, ignoreFailure, querySearchResultConsumer);
        }

        @Override
        public ExecutionStage getExecutionStage() {
            return null;
        }

    }

    static class ProcessorSets {
        final Map<String, Processor.Factory<SearchRequestProcessor>> request;
        final Map<String, Processor.Factory<SearchResponseProcessor>> response;
        final Map<String, Processor.Factory<SearchPhaseResultsProcessor>> phase;

        ProcessorSets(
            Map<String, Processor.Factory<SearchRequestProcessor>> request,
            Map<String, Processor.Factory<SearchResponseProcessor>> response,
            Map<String, Processor.Factory<SearchPhaseResultsProcessor>> phase
        ) {
            this.request = request;
            this.response = response;
            this.phase = phase;
        }
    }

    static class SystemProcessorSets {
        final Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchRequestProcessor>> request;
        final Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchResponseProcessor>> response;
        final Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchPhaseResultsProcessor>> phase;

        SystemProcessorSets(
            Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchRequestProcessor>> request,
            Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchResponseProcessor>> response,
            Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchPhaseResultsProcessor>> phase
        ) {
            this.request = request;
            this.response = response;
            this.phase = phase;
        }
    }

    static class ScaleRequestSizeFactory implements Processor.Factory<SearchRequestProcessor> {

        @Override
        public SearchRequestProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            Processor.PipelineContext pipelineContext
        ) throws Exception {
            float scale = ((Number) config.remove("scale")).floatValue();
            return new FakeRequestProcessor(
                "scale_request_size",
                tag,
                description,
                ignoreFailure,
                req -> req.source().size((int) (req.source().size() * scale))
            );
        }
    }

    ProcessorSets buildBaseProcessors() {
        Map<String, Processor.Factory<SearchRequestProcessor>> requestProcessors = new HashMap<>();
        requestProcessors.put("scale_request_size", new ScaleRequestSizeFactory());

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

        return new SearchPipelineServiceTests.ProcessorSets(requestProcessors, responseProcessors, searchPhaseProcessors);
    }

    static class SystemGeneratedScaleRequestSizePreFactory
        implements
            SystemGeneratedProcessor.SystemGeneratedFactory<SearchRequestProcessor> {
        @Override
        public SearchRequestProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            Processor.PipelineContext pipelineContext
        ) throws Exception {
            return new FakeSystemGeneratedRequestPreProcessor(
                "system_scale_request_size_pre",
                tag,
                description,
                ignoreFailure,
                req -> req.source().size((int) (req.source().size() * 2))
            );
        }

        @Override
        public boolean shouldGenerate(ProcessorGenerationContext context) {
            return context.searchRequest().source().size() < 10;
        }
    }

    static class SystemGeneratedScaleRequestSizePostFactory
        implements
            SystemGeneratedProcessor.SystemGeneratedFactory<SearchRequestProcessor> {
        @Override
        public SearchRequestProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            Processor.PipelineContext pipelineContext
        ) throws Exception {
            return new FakeSystemGeneratedRequestPostProcessor(
                "system_scale_request_size_post",
                tag,
                description,
                ignoreFailure,
                req -> req.source().size((int) (req.source().size() * 3))
            );
        }

        @Override
        public boolean shouldGenerate(ProcessorGenerationContext context) {
            return context.searchRequest().source().size() < 10;
        }
    }

    static class SystemGeneratedScaleScorePreFactory implements SystemGeneratedProcessor.SystemGeneratedFactory<SearchResponseProcessor> {

        @Override
        public SearchResponseProcessor create(
            Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            Processor.PipelineContext pipelineContext
        ) throws Exception {
            return new FakeSystemGeneratedResponsePreProcessor(
                "system_scale_score_pre",
                tag,
                description,
                ignoreFailure,
                rsp -> rsp.getHits().forEach(h -> h.score(h.getScore() * 2))
            );
        }

        @Override
        public boolean shouldGenerate(ProcessorGenerationContext context) {
            return true;
        }
    }

    static class SystemGeneratedScaleScorePostFactory implements SystemGeneratedProcessor.SystemGeneratedFactory<SearchResponseProcessor> {

        @Override
        public SearchResponseProcessor create(
            Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            Processor.PipelineContext pipelineContext
        ) throws Exception {
            return new FakeSystemGeneratedResponsePostProcessor(
                "system_scale_score_post",
                tag,
                description,
                ignoreFailure,
                rsp -> rsp.getHits().forEach(h -> h.score(h.getScore() * 2))
            );
        }

        @Override
        public boolean shouldGenerate(ProcessorGenerationContext context) {
            return true;
        }
    }

    static class SystemGeneratedMaxScorePreFactory implements SystemGeneratedProcessor.SystemGeneratedFactory<SearchPhaseResultsProcessor> {
        @Override
        public SearchPhaseResultsProcessor create(
            Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            Processor.PipelineContext pipelineContext
        ) throws Exception {
            final Consumer<SearchPhaseResult> querySearchResultConsumer = (result) -> result.queryResult().topDocs().maxScore = 100f;
            return new FakeSystemGeneratedSearchPhaseResultsPreProcessor(
                "system_max_score_pre",
                tag,
                description,
                ignoreFailure,
                querySearchResultConsumer
            );
        }

        @Override
        public boolean shouldGenerate(ProcessorGenerationContext context) {
            return true;
        }
    }

    static class SystemGeneratedMaxScorePostFactory
        implements
            SystemGeneratedProcessor.SystemGeneratedFactory<SearchPhaseResultsProcessor> {
        @Override
        public SearchPhaseResultsProcessor create(
            Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            Processor.PipelineContext pipelineContext
        ) throws Exception {
            final Consumer<SearchPhaseResult> querySearchResultConsumer = (result) -> result.queryResult().topDocs().maxScore *= 2;
            return new FakeSystemGeneratedSearchPhaseResultsPostProcessor(
                "system_max_score_post",
                tag,
                description,
                ignoreFailure,
                querySearchResultConsumer
            );
        }

        @Override
        public boolean shouldGenerate(ProcessorGenerationContext context) {
            return true;
        }
    }

    SystemProcessorSets buildSystemProcessors() {
        Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchRequestProcessor>> systemGeneratedRequestProcessors =
            new HashMap<>();
        systemGeneratedRequestProcessors.put("system_scale_request_size_pre", new SystemGeneratedScaleRequestSizePreFactory());
        systemGeneratedRequestProcessors.put("system_scale_request_size_post", new SystemGeneratedScaleRequestSizePostFactory());

        Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchResponseProcessor>> systemGeneratedResponseProcessors =
            new HashMap<>();
        systemGeneratedResponseProcessors.put("system_scale_score_pre", new SystemGeneratedScaleScorePreFactory());
        systemGeneratedResponseProcessors.put("system_scale_score_post", new SystemGeneratedScaleScorePostFactory());

        Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchPhaseResultsProcessor>> systemGeneratedPhaseProcessors =
            new HashMap<>();
        systemGeneratedPhaseProcessors.put("system_max_score_pre", new SystemGeneratedMaxScorePreFactory());
        systemGeneratedPhaseProcessors.put("system_max_score_post", new SystemGeneratedMaxScorePostFactory());

        return new SystemProcessorSets(systemGeneratedRequestProcessors, systemGeneratedResponseProcessors, systemGeneratedPhaseProcessors);
    }

    /**
     * Helper to synchronously apply a response pipeline, returning the transformed response.
     */
    static SearchResponse syncTransformResponse(PipelinedRequest pipelinedRequest, SearchResponse searchResponse) throws Exception {
        SearchResponse[] responseBox = new SearchResponse[1];
        Exception[] exceptionBox = new Exception[1];
        ActionListener<SearchResponse> responseListener = pipelinedRequest.transformResponseListener(ActionListener.wrap(r -> {
            responseBox[0] = r;
        }, e -> { exceptionBox[0] = e; }));
        responseListener.onResponse(searchResponse);

        if (exceptionBox[0] != null) {
            throw exceptionBox[0];
        }
        return responseBox[0];
    }

    /**
     * Helper to synchronously apply a request pipeline, returning the transformed request.
     */
    static PipelinedRequest syncTransformRequest(PipelinedRequest request) throws Exception {
        PipelinedRequest[] requestBox = new PipelinedRequest[1];
        Exception[] exceptionBox = new Exception[1];

        request.transformRequest(ActionListener.wrap(r -> requestBox[0] = (PipelinedRequest) r, e -> exceptionBox[0] = e));
        if (exceptionBox[0] != null) {
            throw exceptionBox[0];
        }
        return requestBox[0];
    }

    /**
     * Helper to synchronously apply a request pipeline and response pipeline, returning the transformed response.
     */
    static SearchResponse syncExecutePipeline(PipelinedRequest request, SearchResponse response) throws Exception {
        return syncTransformResponse(syncTransformRequest(request), response);
    }

    static IndexMetadata.Builder indexBuilder(String index) {
        return indexBuilder(index, Settings.EMPTY);
    }

    static IndexMetadata.Builder indexBuilder(String index, Settings additionalSettings) {
        return IndexMetadata.builder(index).settings(addAdditionalSettings(additionalSettings));
    }

    static Settings.Builder addAdditionalSettings(Settings additionalSettings) {
        return settings(Version.CURRENT).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(additionalSettings);
    }

    void setUpForResolvePipeline(SearchPipelineService service) throws Exception {
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
    }

    SearchSourceBuilder createDefaultSearchSourceBuilder() {
        return SearchSourceBuilder.searchSource().size(10);
    }

    void enabledAllSystemGeneratedFactories(SearchPipelineService service) {
        service.getClusterService()
            .getClusterSettings()
            .applySettings(Settings.builder().putList(ENABLED_SYSTEM_GENERATED_FACTORIES_SETTING.getKey(), ALL).build());
    }
}
