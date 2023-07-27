/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchParseException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.DeleteSearchPipelineRequest;
import org.opensearch.action.search.PutSearchPipelineRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateApplier;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterManagerTaskKeys;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.metrics.OperationMetrics;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.gateway.GatewayService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.node.ReportingService;
import org.opensearch.plugins.SearchPipelinePlugin;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The main entry point for search pipelines. Handles CRUD operations and exposes the API to execute search pipelines
 * against requests and responses.
 */
public class SearchPipelineService implements ClusterStateApplier, ReportingService<SearchPipelineInfo> {

    public static final String SEARCH_PIPELINE_ORIGIN = "search_pipeline";
    public static final String AD_HOC_PIPELINE_ID = "_ad_hoc_pipeline";
    public static final String NOOP_PIPELINE_ID = "_none";
    private static final Logger logger = LogManager.getLogger(SearchPipelineService.class);
    private final ClusterService clusterService;
    private final ScriptService scriptService;
    private final Map<String, Processor.Factory<SearchRequestProcessor>> requestProcessorFactories;
    private final Map<String, Processor.Factory<SearchResponseProcessor>> responseProcessorFactories;
    private final Map<String, Processor.Factory<SearchPhaseResultsProcessor>> phaseInjectorProcessorFactories;
    private volatile Map<String, PipelineHolder> pipelines = Collections.emptyMap();
    private final ThreadPool threadPool;
    private final List<Consumer<ClusterState>> searchPipelineClusterStateListeners = new CopyOnWriteArrayList<>();
    private final ClusterManagerTaskThrottler.ThrottlingKey putPipelineTaskKey;
    private final ClusterManagerTaskThrottler.ThrottlingKey deletePipelineTaskKey;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private volatile ClusterState state;

    private final OperationMetrics totalRequestProcessingMetrics = new OperationMetrics();
    private final OperationMetrics totalResponseProcessingMetrics = new OperationMetrics();

    public SearchPipelineService(
        ClusterService clusterService,
        ThreadPool threadPool,
        Environment env,
        ScriptService scriptService,
        AnalysisRegistry analysisRegistry,
        NamedXContentRegistry namedXContentRegistry,
        NamedWriteableRegistry namedWriteableRegistry,
        List<SearchPipelinePlugin> searchPipelinePlugins,
        Client client
    ) {
        this.clusterService = clusterService;
        this.scriptService = scriptService;
        this.threadPool = threadPool;
        this.namedWriteableRegistry = namedWriteableRegistry;
        SearchPipelinePlugin.Parameters parameters = new SearchPipelinePlugin.Parameters(
            env,
            scriptService,
            analysisRegistry,
            threadPool.getThreadContext(),
            threadPool::relativeTimeInMillis,
            (delay, command) -> threadPool.schedule(command, TimeValue.timeValueMillis(delay), ThreadPool.Names.GENERIC),
            this,
            client,
            threadPool.generic()::execute,
            namedXContentRegistry
        );
        this.requestProcessorFactories = processorFactories(searchPipelinePlugins, p -> p.getRequestProcessors(parameters));
        this.responseProcessorFactories = processorFactories(searchPipelinePlugins, p -> p.getResponseProcessors(parameters));
        this.phaseInjectorProcessorFactories = processorFactories(
            searchPipelinePlugins,
            p -> p.getSearchPhaseResultsProcessors(parameters)
        );
        putPipelineTaskKey = clusterService.registerClusterManagerTask(ClusterManagerTaskKeys.PUT_SEARCH_PIPELINE_KEY, true);
        deletePipelineTaskKey = clusterService.registerClusterManagerTask(ClusterManagerTaskKeys.DELETE_SEARCH_PIPELINE_KEY, true);
    }

    private static <T extends Processor> Map<String, Processor.Factory<T>> processorFactories(
        List<SearchPipelinePlugin> searchPipelinePlugins,
        Function<SearchPipelinePlugin, Map<String, Processor.Factory<T>>> processorLoader
    ) {
        Map<String, Processor.Factory<T>> processorFactories = new HashMap<>();
        for (SearchPipelinePlugin searchPipelinePlugin : searchPipelinePlugins) {
            Map<String, Processor.Factory<T>> newProcessors = processorLoader.apply(searchPipelinePlugin);
            for (Map.Entry<String, Processor.Factory<T>> entry : newProcessors.entrySet()) {
                if (processorFactories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Search processor [" + entry.getKey() + "] is already registered");
                }
            }
        }
        return Collections.unmodifiableMap(processorFactories);
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        state = event.state();

        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }
        searchPipelineClusterStateListeners.forEach(consumer -> consumer.accept(state));

        SearchPipelineMetadata newSearchPipelineMetadata = state.getMetadata().custom(SearchPipelineMetadata.TYPE);
        if (newSearchPipelineMetadata == null) {
            return;
        }

        try {
            innerUpdatePipelines(newSearchPipelineMetadata);
        } catch (OpenSearchParseException e) {
            logger.warn("failed to update search pipelines", e);
        }
    }

    void innerUpdatePipelines(SearchPipelineMetadata newSearchPipelineMetadata) {
        Map<String, PipelineHolder> existingPipelines = this.pipelines;

        // Lazily initialize these variables in order to favour the most likely scenario that there are no pipeline changes:
        Map<String, PipelineHolder> newPipelines = null;
        List<OpenSearchParseException> exceptions = null;
        // Iterate over pipeline configurations in metadata and constructs a new pipeline if there is no pipeline
        // or the pipeline configuration has been modified

        for (PipelineConfiguration newConfiguration : newSearchPipelineMetadata.getPipelines().values()) {
            PipelineHolder previous = existingPipelines.get(newConfiguration.getId());
            if (previous != null && previous.configuration.equals(newConfiguration)) {
                continue;
            }
            if (newPipelines == null) {
                newPipelines = new HashMap<>(existingPipelines);
            }
            try {
                PipelineWithMetrics newPipeline = PipelineWithMetrics.create(
                    newConfiguration.getId(),
                    newConfiguration.getConfigAsMap(),
                    requestProcessorFactories,
                    responseProcessorFactories,
                    phaseInjectorProcessorFactories,
                    namedWriteableRegistry,
                    totalRequestProcessingMetrics,
                    totalResponseProcessingMetrics,
                    new Processor.PipelineContext(Processor.PipelineSource.UPDATE_PIPELINE)
                );
                newPipelines.put(newConfiguration.getId(), new PipelineHolder(newConfiguration, newPipeline));

                if (previous != null) {
                    newPipeline.copyMetrics(previous.pipeline);
                }
            } catch (Exception e) {
                OpenSearchParseException parseException = new OpenSearchParseException(
                    "Error updating pipeline with id [" + newConfiguration.getId() + "]",
                    e
                );
                // TODO -- replace pipeline with one that throws this exception when we try to use it
                if (exceptions == null) {
                    exceptions = new ArrayList<>();
                }
                exceptions.add(parseException);
            }
        }
        // Iterate over the current active pipelines and check whether they are missing in the pipeline configuration and
        // if so delete the pipeline from new Pipelines map:
        for (Map.Entry<String, PipelineHolder> entry : existingPipelines.entrySet()) {
            if (newSearchPipelineMetadata.getPipelines().get(entry.getKey()) == null) {
                if (newPipelines == null) {
                    newPipelines = new HashMap<>(existingPipelines);
                }
                newPipelines.remove(entry.getKey());
            }
        }

        if (newPipelines != null) {
            this.pipelines = Collections.unmodifiableMap(newPipelines);
            if (exceptions != null) {
                ExceptionsHelper.rethrowAndSuppress(exceptions);
            }
        }
    }

    public void putPipeline(
        Map<DiscoveryNode, SearchPipelineInfo> searchPipelineInfos,
        PutSearchPipelineRequest request,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        validatePipeline(searchPipelineInfos, request);
        clusterService.submitStateUpdateTask(
            "put-search-pipeline-" + request.getId(),
            new AckedClusterStateUpdateTask<>(request, listener) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return innerPut(request, currentState);
                }

                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return putPipelineTaskKey;
                }

                @Override
                protected AcknowledgedResponse newResponse(boolean acknowledged) {
                    return new AcknowledgedResponse(acknowledged);
                }
            }
        );
    }

    static ClusterState innerPut(PutSearchPipelineRequest request, ClusterState currentState) {
        SearchPipelineMetadata currentSearchPipelineMetadata = currentState.metadata().custom(SearchPipelineMetadata.TYPE);
        Map<String, PipelineConfiguration> pipelines;
        if (currentSearchPipelineMetadata != null) {
            pipelines = new HashMap<>(currentSearchPipelineMetadata.getPipelines());
        } else {
            pipelines = new HashMap<>();
        }
        pipelines.put(request.getId(), new PipelineConfiguration(request.getId(), request.getSource(), request.getMediaType()));
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metadata(
            Metadata.builder(currentState.getMetadata())
                .putCustom(SearchPipelineMetadata.TYPE, new SearchPipelineMetadata(pipelines))
                .build()
        );
        return newState.build();
    }

    void validatePipeline(Map<DiscoveryNode, SearchPipelineInfo> searchPipelineInfos, PutSearchPipelineRequest request) throws Exception {
        if (searchPipelineInfos.isEmpty()) {
            throw new IllegalStateException("Search pipeline info is empty");
        }
        Map<String, Object> pipelineConfig = XContentHelper.convertToMap(request.getSource(), false, request.getMediaType()).v2();
        Pipeline pipeline = PipelineWithMetrics.create(
            request.getId(),
            pipelineConfig,
            requestProcessorFactories,
            responseProcessorFactories,
            phaseInjectorProcessorFactories,
            namedWriteableRegistry,
            new OperationMetrics(), // Use ephemeral metrics for validation
            new OperationMetrics(),
            new Processor.PipelineContext(Processor.PipelineSource.VALIDATE_PIPELINE)
        );
        List<Exception> exceptions = new ArrayList<>();
        for (SearchRequestProcessor processor : pipeline.getSearchRequestProcessors()) {
            for (Map.Entry<DiscoveryNode, SearchPipelineInfo> entry : searchPipelineInfos.entrySet()) {
                String type = processor.getType();
                if (entry.getValue().containsProcessor(Pipeline.REQUEST_PROCESSORS_KEY, type) == false) {
                    String message = "Processor type [" + processor.getType() + "] is not installed on node [" + entry.getKey() + "]";
                    exceptions.add(ConfigurationUtils.newConfigurationException(processor.getType(), processor.getTag(), null, message));
                }
            }
        }
        for (SearchResponseProcessor processor : pipeline.getSearchResponseProcessors()) {
            for (Map.Entry<DiscoveryNode, SearchPipelineInfo> entry : searchPipelineInfos.entrySet()) {
                String type = processor.getType();
                if (entry.getValue().containsProcessor(Pipeline.RESPONSE_PROCESSORS_KEY, type) == false) {
                    String message = "Processor type [" + processor.getType() + "] is not installed on node [" + entry.getKey() + "]";
                    exceptions.add(ConfigurationUtils.newConfigurationException(processor.getType(), processor.getTag(), null, message));
                }
            }
        }
        ExceptionsHelper.rethrowAndSuppress(exceptions);
    }

    public void deletePipeline(DeleteSearchPipelineRequest request, ActionListener<AcknowledgedResponse> listener) throws Exception {
        clusterService.submitStateUpdateTask(
            "delete-search-pipeline-" + request.getId(),
            new AckedClusterStateUpdateTask<>(request, listener) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return innerDelete(request, currentState);
                }

                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return deletePipelineTaskKey;
                }

                @Override
                protected AcknowledgedResponse newResponse(boolean acknowledged) {
                    return new AcknowledgedResponse(acknowledged);
                }
            }
        );

    }

    static ClusterState innerDelete(DeleteSearchPipelineRequest request, ClusterState currentState) {
        SearchPipelineMetadata currentMetadata = currentState.metadata().custom(SearchPipelineMetadata.TYPE);
        if (currentMetadata == null) {
            return currentState;
        }
        Map<String, PipelineConfiguration> pipelines = currentMetadata.getPipelines();
        Set<String> toRemove = new HashSet<>();
        for (String pipelineKey : pipelines.keySet()) {
            if (Regex.simpleMatch(request.getId(), pipelineKey)) {
                toRemove.add(pipelineKey);
            }
        }
        if (toRemove.isEmpty()) {
            if (Regex.isMatchAllPattern(request.getId())) {
                // Deleting all the empty state is a no-op.
                return currentState;
            }
            throw new ResourceNotFoundException("pipeline [{}] is missing", request.getId());
        }
        final Map<String, PipelineConfiguration> newPipelines = new HashMap<>(pipelines);
        for (String key : toRemove) {
            newPipelines.remove(key);
        }
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metadata(
            Metadata.builder(currentState.getMetadata()).putCustom(SearchPipelineMetadata.TYPE, new SearchPipelineMetadata(newPipelines))
        );
        return newState.build();
    }

    public PipelinedRequest resolvePipeline(SearchRequest searchRequest) {
        Pipeline pipeline = Pipeline.NO_OP_PIPELINE;

        if (searchRequest.source() != null && searchRequest.source().searchPipelineSource() != null) {
            // Pipeline defined in search request (ad hoc pipeline).
            if (searchRequest.pipeline() != null) {
                throw new IllegalArgumentException(
                    "Both named and inline search pipeline were specified. Please only specify one or the other."
                );
            }
            try {
                pipeline = PipelineWithMetrics.create(
                    AD_HOC_PIPELINE_ID,
                    searchRequest.source().searchPipelineSource(),
                    requestProcessorFactories,
                    responseProcessorFactories,
                    phaseInjectorProcessorFactories,
                    namedWriteableRegistry,
                    totalRequestProcessingMetrics,
                    totalResponseProcessingMetrics,
                    new Processor.PipelineContext(Processor.PipelineSource.SEARCH_REQUEST)
                );
            } catch (Exception e) {
                throw new SearchPipelineProcessingException(e);
            }
        } else {
            String pipelineId = NOOP_PIPELINE_ID;
            if (searchRequest.pipeline() != null) {
                // Named pipeline specified for the request
                pipelineId = searchRequest.pipeline();
            } else if (state != null && searchRequest.indices() != null && searchRequest.indices().length == 1) {
                // Check for index default pipeline
                IndexMetadata indexMetadata = state.metadata().index(searchRequest.indices()[0]);
                if (indexMetadata != null) {
                    Settings indexSettings = indexMetadata.getSettings();
                    if (IndexSettings.DEFAULT_SEARCH_PIPELINE.exists(indexSettings)) {
                        pipelineId = IndexSettings.DEFAULT_SEARCH_PIPELINE.get(indexSettings);
                    }
                }
            }
            if (NOOP_PIPELINE_ID.equals(pipelineId) == false) {
                PipelineHolder pipelineHolder = pipelines.get(pipelineId);
                if (pipelineHolder == null) {
                    throw new IllegalArgumentException("Pipeline " + pipelineId + " is not defined");
                }
                pipeline = pipelineHolder.pipeline;
            }
        }
        SearchRequest transformedRequest = pipeline.transformRequest(searchRequest);
        return new PipelinedRequest(pipeline, transformedRequest);
    }

    Map<String, Processor.Factory<SearchRequestProcessor>> getRequestProcessorFactories() {
        return requestProcessorFactories;
    }

    Map<String, Processor.Factory<SearchResponseProcessor>> getResponseProcessorFactories() {
        return responseProcessorFactories;
    }

    @Override
    public SearchPipelineInfo info() {
        List<ProcessorInfo> requestProcessorInfoList = requestProcessorFactories.keySet()
            .stream()
            .map(ProcessorInfo::new)
            .collect(Collectors.toList());
        List<ProcessorInfo> responseProcessorInfoList = responseProcessorFactories.keySet()
            .stream()
            .map(ProcessorInfo::new)
            .collect(Collectors.toList());
        return new SearchPipelineInfo(
            Map.of(Pipeline.REQUEST_PROCESSORS_KEY, requestProcessorInfoList, Pipeline.RESPONSE_PROCESSORS_KEY, responseProcessorInfoList)
        );
    }

    public SearchPipelineStats stats() {
        SearchPipelineStats.Builder builder = new SearchPipelineStats.Builder();
        builder.withTotalStats(totalRequestProcessingMetrics, totalResponseProcessingMetrics);
        for (PipelineHolder pipelineHolder : pipelines.values()) {
            PipelineWithMetrics pipeline = pipelineHolder.pipeline;
            pipeline.populateStats(builder);
        }
        return builder.build();
    }

    public static List<PipelineConfiguration> getPipelines(ClusterState clusterState, String... ids) {
        SearchPipelineMetadata metadata = clusterState.getMetadata().custom(SearchPipelineMetadata.TYPE);
        return innerGetPipelines(metadata, ids);
    }

    static List<PipelineConfiguration> innerGetPipelines(SearchPipelineMetadata metadata, String... ids) {
        if (metadata == null) {
            return Collections.emptyList();
        }

        // if we didn't ask for _any_ ID, then we get them all (this is the same as if they ask for '*')
        if (ids.length == 0) {
            return new ArrayList<>(metadata.getPipelines().values());
        }
        List<PipelineConfiguration> result = new ArrayList<>(ids.length);
        for (String id : ids) {
            if (Regex.isSimpleMatchPattern(id)) {
                for (Map.Entry<String, PipelineConfiguration> entry : metadata.getPipelines().entrySet()) {
                    if (Regex.simpleMatch(id, entry.getKey())) {
                        result.add(entry.getValue());
                    }
                }
            } else {
                PipelineConfiguration pipeline = metadata.getPipelines().get(id);
                if (pipeline != null) {
                    result.add(pipeline);
                }
            }
        }
        return result;
    }

    public ClusterService getClusterService() {
        return clusterService;
    }

    Map<String, PipelineHolder> getPipelines() {
        return pipelines;
    }

    static class PipelineHolder {

        final PipelineConfiguration configuration;
        final PipelineWithMetrics pipeline;

        PipelineHolder(PipelineConfiguration configuration, PipelineWithMetrics pipeline) {
            this.configuration = Objects.requireNonNull(configuration);
            this.pipeline = Objects.requireNonNull(pipeline);
        }
    }
}
