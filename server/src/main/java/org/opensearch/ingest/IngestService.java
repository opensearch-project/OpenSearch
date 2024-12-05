/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.ingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchParseException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.TransportBulkAction;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.ingest.DeletePipelineRequest;
import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateApplier;
import org.opensearch.cluster.metadata.IndexAbstraction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataIndexTemplateService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterManagerTaskKeys;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.metrics.OperationMetrics;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.service.ReportingService;
import org.opensearch.env.Environment;
import org.opensearch.gateway.GatewayService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.VersionType;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.IngestPlugin;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;

/**
 * Holder class for several ingest related services.
 *
 * @opensearch.internal
 */
public class IngestService implements ClusterStateApplier, ReportingService<IngestInfo> {

    public static final String NOOP_PIPELINE_NAME = "_none";

    public static final String INGEST_ORIGIN = "ingest";

    /**
     * Defines the limit for the number of processors which can run on a given document during ingestion.
     */
    public static final Setting<Integer> MAX_NUMBER_OF_INGEST_PROCESSORS = Setting.intSetting(
        "cluster.ingest.max_number_processors",
        Integer.MAX_VALUE,
        1,
        Integer.MAX_VALUE,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Logger logger = LogManager.getLogger(IngestService.class);

    private final ClusterService clusterService;
    private final ScriptService scriptService;
    private final Map<String, Processor.Factory> processorFactories;
    // Ideally this should be in IngestMetadata class, but we don't have the processor factories around there.
    // We know of all the processor factories when a node with all its plugin have been initialized. Also some
    // processor factories rely on other node services. Custom metadata is statically registered when classes
    // are loaded, so in the cluster state we just save the pipeline config and here we keep the actual pipelines around.
    private volatile Map<String, PipelineHolder> pipelines = Collections.emptyMap();
    private final ThreadPool threadPool;
    private final OperationMetrics totalMetrics = new OperationMetrics();
    private final List<Consumer<ClusterState>> ingestClusterStateListeners = new CopyOnWriteArrayList<>();
    private final ClusterManagerTaskThrottler.ThrottlingKey putPipelineTaskKey;
    private final ClusterManagerTaskThrottler.ThrottlingKey deletePipelineTaskKey;
    private volatile ClusterState state;
    private volatile int maxIngestProcessorCount;

    public IngestService(
        ClusterService clusterService,
        ThreadPool threadPool,
        Environment env,
        ScriptService scriptService,
        AnalysisRegistry analysisRegistry,
        List<IngestPlugin> ingestPlugins,
        Client client,
        IndicesService indicesService
    ) {
        this.clusterService = clusterService;
        this.scriptService = scriptService;
        this.processorFactories = processorFactories(
            ingestPlugins,
            new Processor.Parameters(
                env,
                scriptService,
                analysisRegistry,
                threadPool.getThreadContext(),
                threadPool::relativeTimeInMillis,
                (delay, command) -> threadPool.schedule(command, TimeValue.timeValueMillis(delay), ThreadPool.Names.GENERIC),
                this,
                client,
                threadPool.generic()::execute,
                indicesService
            )
        );
        this.threadPool = threadPool;

        // Task is onboarded for throttling, it will get retried from associated TransportClusterManagerNodeAction.
        putPipelineTaskKey = clusterService.registerClusterManagerTask(ClusterManagerTaskKeys.PUT_PIPELINE_KEY, true);
        deletePipelineTaskKey = clusterService.registerClusterManagerTask(ClusterManagerTaskKeys.DELETE_PIPELINE_KEY, true);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_NUMBER_OF_INGEST_PROCESSORS, this::setMaxIngestProcessorCount);
        setMaxIngestProcessorCount(clusterService.getClusterSettings().get(MAX_NUMBER_OF_INGEST_PROCESSORS));
    }

    private void setMaxIngestProcessorCount(Integer maxIngestProcessorCount) {
        this.maxIngestProcessorCount = maxIngestProcessorCount;
    }

    private static Map<String, Processor.Factory> processorFactories(List<IngestPlugin> ingestPlugins, Processor.Parameters parameters) {
        Map<String, Processor.Factory> processorFactories = new HashMap<>();
        for (IngestPlugin ingestPlugin : ingestPlugins) {
            Map<String, Processor.Factory> newProcessors = ingestPlugin.getProcessors(parameters);
            for (Map.Entry<String, Processor.Factory> entry : newProcessors.entrySet()) {
                if (processorFactories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Ingest processor [" + entry.getKey() + "] is already registered");
                }
            }
        }
        return Collections.unmodifiableMap(processorFactories);
    }

    public static boolean resolvePipelines(
        final DocWriteRequest<?> originalRequest,
        final IndexRequest indexRequest,
        final Metadata metadata
    ) {
        if (indexRequest.isPipelineResolved() == false) {
            final String requestPipeline = indexRequest.getPipeline();
            indexRequest.setPipeline(NOOP_PIPELINE_NAME);
            indexRequest.setFinalPipeline(NOOP_PIPELINE_NAME);
            String defaultPipeline = null;
            String finalPipeline = null;
            IndexMetadata indexMetadata = null;
            // start to look for default or final pipelines via settings found in the index meta data
            if (originalRequest != null) {
                indexMetadata = metadata.indices().get(originalRequest.index());
            }
            // check the alias for the index request (this is how normal index requests are modeled)
            if (indexMetadata == null && indexRequest.index() != null) {
                IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(indexRequest.index());
                if (indexAbstraction != null) {
                    indexMetadata = indexAbstraction.getWriteIndex();
                }
            }
            // check the alias for the action request (this is how upserts are modeled)
            if (indexMetadata == null && originalRequest != null && originalRequest.index() != null) {
                IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(originalRequest.index());
                if (indexAbstraction != null) {
                    indexMetadata = indexAbstraction.getWriteIndex();
                }
            }
            if (indexMetadata != null) {
                final Settings indexSettings = indexMetadata.getSettings();
                if (IndexSettings.DEFAULT_PIPELINE.exists(indexSettings)) {
                    // find the default pipeline if one is defined from an existing index setting
                    defaultPipeline = IndexSettings.DEFAULT_PIPELINE.get(indexSettings);
                    indexRequest.setPipeline(defaultPipeline);
                }
                if (IndexSettings.FINAL_PIPELINE.exists(indexSettings)) {
                    // find the final pipeline if one is defined from an existing index setting
                    finalPipeline = IndexSettings.FINAL_PIPELINE.get(indexSettings);
                    indexRequest.setFinalPipeline(finalPipeline);
                }
            } else if (indexRequest.index() != null) {
                // the index does not exist yet (and this is a valid request), so match index
                // templates to look for pipelines in either a matching V2 template (which takes
                // precedence), or if a V2 template does not match, any V1 templates
                String v2Template = MetadataIndexTemplateService.findV2Template(metadata, indexRequest.index(), false);
                if (v2Template != null) {
                    Settings settings = MetadataIndexTemplateService.resolveSettings(metadata, v2Template);
                    if (IndexSettings.DEFAULT_PIPELINE.exists(settings)) {
                        defaultPipeline = IndexSettings.DEFAULT_PIPELINE.get(settings);
                        // we can not break in case a lower-order template has a final pipeline that we need to collect
                    }
                    if (IndexSettings.FINAL_PIPELINE.exists(settings)) {
                        finalPipeline = IndexSettings.FINAL_PIPELINE.get(settings);
                        // we can not break in case a lower-order template has a default pipeline that we need to collect
                    }
                    indexRequest.setPipeline(defaultPipeline != null ? defaultPipeline : NOOP_PIPELINE_NAME);
                    indexRequest.setFinalPipeline(finalPipeline != null ? finalPipeline : NOOP_PIPELINE_NAME);
                } else {
                    List<IndexTemplateMetadata> templates = MetadataIndexTemplateService.findV1Templates(
                        metadata,
                        indexRequest.index(),
                        null
                    );
                    // order of templates are highest order first
                    for (final IndexTemplateMetadata template : templates) {
                        final Settings settings = template.settings();
                        if (defaultPipeline == null && IndexSettings.DEFAULT_PIPELINE.exists(settings)) {
                            defaultPipeline = IndexSettings.DEFAULT_PIPELINE.get(settings);
                            // we can not break in case a lower-order template has a final pipeline that we need to collect
                        }
                        if (finalPipeline == null && IndexSettings.FINAL_PIPELINE.exists(settings)) {
                            finalPipeline = IndexSettings.FINAL_PIPELINE.get(settings);
                            // we can not break in case a lower-order template has a default pipeline that we need to collect
                        }
                        if (defaultPipeline != null && finalPipeline != null) {
                            // we can break if we have already collected a default and final pipeline
                            break;
                        }
                    }
                    indexRequest.setPipeline(defaultPipeline != null ? defaultPipeline : NOOP_PIPELINE_NAME);
                    indexRequest.setFinalPipeline(finalPipeline != null ? finalPipeline : NOOP_PIPELINE_NAME);
                }
            }

            if (requestPipeline != null) {
                indexRequest.setPipeline(requestPipeline);
            }

            /*
             * We have to track whether or not the pipeline for this request has already been resolved. It can happen that the
             * pipeline for this request has already been derived yet we execute this loop again. That occurs if the bulk request
             * has been forwarded by a non-ingest coordinating node to an ingest node. In this case, the coordinating node will have
             * already resolved the pipeline for this request. It is important that we are able to distinguish this situation as we
             * can not double-resolve the pipeline because we will not be able to distinguish the case of the pipeline having been
             * set from a request pipeline parameter versus having been set by the resolution. We need to be able to distinguish
             * these cases as we need to reject the request if the pipeline was set by a required pipeline and there is a request
             * pipeline parameter too.
             */
            indexRequest.isPipelineResolved(true);
        }

        // return whether this index request has a pipeline
        return NOOP_PIPELINE_NAME.equals(indexRequest.getPipeline()) == false
            || NOOP_PIPELINE_NAME.equals(indexRequest.getFinalPipeline()) == false;
    }

    public ClusterService getClusterService() {
        return clusterService;
    }

    public ScriptService getScriptService() {
        return scriptService;
    }

    /**
     * Deletes the pipeline specified by id in the request.
     */
    public void delete(DeletePipelineRequest request, ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask(
            "delete-pipeline-" + request.getId(),
            new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

                @Override
                protected AcknowledgedResponse newResponse(boolean acknowledged) {
                    return new AcknowledgedResponse(acknowledged);
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    return innerDelete(request, currentState);
                }

                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return deletePipelineTaskKey;
                }
            }
        );
    }

    static ClusterState innerDelete(DeletePipelineRequest request, ClusterState currentState) {
        IngestMetadata currentIngestMetadata = currentState.metadata().custom(IngestMetadata.TYPE);
        if (currentIngestMetadata == null) {
            return currentState;
        }
        Map<String, PipelineConfiguration> pipelines = currentIngestMetadata.getPipelines();
        Set<String> toRemove = new HashSet<>();
        for (String pipelineKey : pipelines.keySet()) {
            if (Regex.simpleMatch(request.getId(), pipelineKey)) {
                toRemove.add(pipelineKey);
            }
        }
        if (toRemove.isEmpty() && Regex.isMatchAllPattern(request.getId()) == false) {
            throw new ResourceNotFoundException("pipeline [{}] is missing", request.getId());
        } else if (toRemove.isEmpty()) {
            return currentState;
        }
        final Map<String, PipelineConfiguration> pipelinesCopy = new HashMap<>(pipelines);
        for (String key : toRemove) {
            pipelinesCopy.remove(key);
        }
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metadata(
            Metadata.builder(currentState.getMetadata()).putCustom(IngestMetadata.TYPE, new IngestMetadata(pipelinesCopy)).build()
        );
        return newState.build();
    }

    /**
     * @return pipeline configuration specified by id. If multiple ids or wildcards are specified multiple pipelines
     * may be returned
     */
    // Returning PipelineConfiguration instead of Pipeline, because Pipeline and Processor interface don't
    // know how to serialize themselves.
    public static List<PipelineConfiguration> getPipelines(ClusterState clusterState, String... ids) {
        IngestMetadata ingestMetadata = clusterState.getMetadata().custom(IngestMetadata.TYPE);
        return innerGetPipelines(ingestMetadata, ids);
    }

    static List<PipelineConfiguration> innerGetPipelines(IngestMetadata ingestMetadata, String... ids) {
        if (ingestMetadata == null) {
            return Collections.emptyList();
        }

        // if we didn't ask for _any_ ID, then we get them all (this is the same as if they ask for '*')
        if (ids.length == 0) {
            return new ArrayList<>(ingestMetadata.getPipelines().values());
        }

        List<PipelineConfiguration> result = new ArrayList<>(ids.length);
        for (String id : ids) {
            if (Regex.isSimpleMatchPattern(id)) {
                for (Map.Entry<String, PipelineConfiguration> entry : ingestMetadata.getPipelines().entrySet()) {
                    if (Regex.simpleMatch(id, entry.getKey())) {
                        result.add(entry.getValue());
                    }
                }
            } else {
                PipelineConfiguration pipeline = ingestMetadata.getPipelines().get(id);
                if (pipeline != null) {
                    result.add(pipeline);
                }
            }
        }
        return result;
    }

    /**
     * Stores the specified pipeline definition in the request.
     */
    public void putPipeline(
        Map<DiscoveryNode, IngestInfo> ingestInfos,
        PutPipelineRequest request,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        // validates the pipeline and processor configuration before submitting a cluster update task:
        validatePipeline(ingestInfos, request);
        clusterService.submitStateUpdateTask(
            "put-pipeline-" + request.getId(),
            new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

                @Override
                protected AcknowledgedResponse newResponse(boolean acknowledged) {
                    return new AcknowledgedResponse(acknowledged);
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    return innerPut(request, currentState);
                }

                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return putPipelineTaskKey;
                }
            }
        );
    }

    /**
     * Returns the pipeline by the specified id
     */
    public Pipeline getPipeline(String id) {
        PipelineHolder holder = pipelines.get(id);
        if (holder != null) {
            return holder.pipeline;
        } else {
            return null;
        }
    }

    public Map<String, Processor.Factory> getProcessorFactories() {
        return processorFactories;
    }

    @Override
    public IngestInfo info() {
        Map<String, Processor.Factory> processorFactories = getProcessorFactories();
        List<ProcessorInfo> processorInfoList = new ArrayList<>(processorFactories.size());
        for (Map.Entry<String, Processor.Factory> entry : processorFactories.entrySet()) {
            processorInfoList.add(new ProcessorInfo(entry.getKey()));
        }
        return new IngestInfo(processorInfoList);
    }

    Map<String, PipelineHolder> pipelines() {
        return pipelines;
    }

    /**
     * Recursive method to obtain all of the non-failure processors for given compoundProcessor. Since conditionals are implemented as
     * wrappers to the actual processor, always prefer the actual processor's metric over the conditional processor's metric.
     * @param compoundProcessor The compound processor to start walking the non-failure processors
     * @param processorMetrics The list of {@link Processor} {@link OperationMetrics} tuples.
     * @return the processorMetrics for all non-failure processor that belong to the original compoundProcessor
     */
    private static List<Tuple<Processor, OperationMetrics>> getProcessorMetrics(
        CompoundProcessor compoundProcessor,
        List<Tuple<Processor, OperationMetrics>> processorMetrics
    ) {
        // only surface the top level non-failure processors, on-failure processor times will be included in the top level non-failure
        for (Tuple<Processor, OperationMetrics> processorWithMetric : compoundProcessor.getProcessorsWithMetrics()) {
            Processor processor = processorWithMetric.v1();
            OperationMetrics metric = processorWithMetric.v2();
            if (processor instanceof CompoundProcessor) {
                getProcessorMetrics((CompoundProcessor) processor, processorMetrics);
            } else {
                // Prefer the conditional's metric since it only includes metrics when the conditional evaluated to true.
                if (processor instanceof ConditionalProcessor) {
                    metric = ((ConditionalProcessor) processor).getMetric();
                }
                processorMetrics.add(new Tuple<>(processor, metric));
            }
        }
        return processorMetrics;
    }

    public static ClusterState innerPut(PutPipelineRequest request, ClusterState currentState) {
        IngestMetadata currentIngestMetadata = currentState.metadata().custom(IngestMetadata.TYPE);
        Map<String, PipelineConfiguration> pipelines;
        if (currentIngestMetadata != null) {
            pipelines = new HashMap<>(currentIngestMetadata.getPipelines());
        } else {
            pipelines = new HashMap<>();
        }

        pipelines.put(request.getId(), new PipelineConfiguration(request.getId(), request.getSource(), request.getMediaType()));
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metadata(
            Metadata.builder(currentState.getMetadata()).putCustom(IngestMetadata.TYPE, new IngestMetadata(pipelines)).build()
        );
        return newState.build();
    }

    void validatePipeline(Map<DiscoveryNode, IngestInfo> ingestInfos, PutPipelineRequest request) throws Exception {
        if (ingestInfos.isEmpty()) {
            throw new IllegalStateException("Ingest info is empty");
        }

        Map<String, Object> pipelineConfig = XContentHelper.convertToMap(request.getSource(), false, request.getMediaType()).v2();
        Pipeline pipeline = Pipeline.create(request.getId(), pipelineConfig, processorFactories, scriptService);

        validateProcessorCountForIngestPipeline(pipeline);

        List<Exception> exceptions = new ArrayList<>();
        for (Processor processor : pipeline.flattenAllProcessors()) {
            for (Map.Entry<DiscoveryNode, IngestInfo> entry : ingestInfos.entrySet()) {
                String type = processor.getType();
                if (entry.getValue().containsProcessor(type) == false && ConditionalProcessor.TYPE.equals(type) == false) {
                    String message = "Processor type [" + processor.getType() + "] is not installed on node [" + entry.getKey() + "]";
                    exceptions.add(ConfigurationUtils.newConfigurationException(processor.getType(), processor.getTag(), null, message));
                }
            }
        }
        ExceptionsHelper.rethrowAndSuppress(exceptions);
    }

    public void validateProcessorCountForIngestPipeline(Pipeline pipeline) {
        List<Processor> processors = pipeline.flattenAllProcessors();

        if (processors.size() > maxIngestProcessorCount) {
            throw new IllegalStateException(
                "Cannot use more than the maximum processors allowed. Number of processors being configured is ["
                    + processors.size()
                    + "] which exceeds the maximum allowed configuration of ["
                    + maxIngestProcessorCount
                    + "] processors."
            );
        }
    }

    public void executeBulkRequest(
        int numberOfActionRequests,
        Iterable<DocWriteRequest<?>> actionRequests,
        BiConsumer<Integer, Exception> onFailure,
        BiConsumer<Thread, Exception> onCompletion,
        IntConsumer onDropped,
        String executorName,
        BulkRequest originalBulkRequest
    ) {
        threadPool.executor(executorName).execute(new AbstractRunnable() {

            @Override
            public void onFailure(Exception e) {
                onCompletion.accept(null, e);
            }

            @Override
            protected void doRun() {
                runBulkRequestInBatch(numberOfActionRequests, actionRequests, onFailure, onCompletion, onDropped, originalBulkRequest);
            }
        });
    }

    private void runBulkRequestInBatch(
        int numberOfActionRequests,
        Iterable<DocWriteRequest<?>> actionRequests,
        BiConsumer<Integer, Exception> onFailure,
        BiConsumer<Thread, Exception> onCompletion,
        IntConsumer onDropped,
        BulkRequest originalBulkRequest
    ) {

        final Thread originalThread = Thread.currentThread();
        final AtomicInteger counter = new AtomicInteger(numberOfActionRequests);
        int i = 0;
        List<IndexRequestWrapper> indexRequestWrappers = new ArrayList<>();
        for (DocWriteRequest<?> actionRequest : actionRequests) {
            IndexRequest indexRequest = TransportBulkAction.getIndexWriteRequest(actionRequest);
            if (indexRequest == null) {
                if (counter.decrementAndGet() == 0) {
                    onCompletion.accept(originalThread, null);
                }
                assert counter.get() >= 0;
                i++;
                continue;
            }

            final String pipelineId = indexRequest.getPipeline();
            indexRequest.setPipeline(NOOP_PIPELINE_NAME);
            final String finalPipelineId = indexRequest.getFinalPipeline();
            indexRequest.setFinalPipeline(NOOP_PIPELINE_NAME);
            boolean hasFinalPipeline = true;
            final List<String> pipelines;
            if (IngestService.NOOP_PIPELINE_NAME.equals(pipelineId) == false
                && IngestService.NOOP_PIPELINE_NAME.equals(finalPipelineId) == false) {
                pipelines = Arrays.asList(pipelineId, finalPipelineId);
            } else if (IngestService.NOOP_PIPELINE_NAME.equals(pipelineId) == false) {
                pipelines = Collections.singletonList(pipelineId);
                hasFinalPipeline = false;
            } else if (IngestService.NOOP_PIPELINE_NAME.equals(finalPipelineId) == false) {
                pipelines = Collections.singletonList(finalPipelineId);
            } else {
                if (counter.decrementAndGet() == 0) {
                    onCompletion.accept(originalThread, null);
                }
                assert counter.get() >= 0;
                i++;
                continue;
            }

            indexRequestWrappers.add(new IndexRequestWrapper(i, indexRequest, pipelines, hasFinalPipeline));
            i++;
        }

        int batchSize = Math.min(numberOfActionRequests, originalBulkRequest.batchSize());
        List<List<IndexRequestWrapper>> batches = prepareBatches(batchSize, indexRequestWrappers);
        logger.debug("batchSize: {}, batches: {}", batchSize, batches.size());

        for (List<IndexRequestWrapper> batch : batches) {
            executePipelinesInBatchRequests(
                batch.stream().map(IndexRequestWrapper::getSlot).collect(Collectors.toList()),
                batch.get(0).getPipelines().iterator(),
                batch.get(0).isHasFinalPipeline(),
                batch.stream().map(IndexRequestWrapper::getIndexRequest).collect(Collectors.toList()),
                onDropped,
                onFailure,
                counter,
                onCompletion,
                originalThread
            );
        }
    }

    /**
     * IndexRequests are grouped by unique (index + pipeline_ids) before batching.
     * Only IndexRequests in the same group could be batched. It's to ensure batched documents always
     * flow through the same pipeline together.
     *
     * An IndexRequest could be preprocessed by at most two pipelines: default_pipeline and final_pipeline.
     * A final_pipeline is configured on index level. The default_pipeline for a IndexRequest in a _bulk API
     * could come from three places:
     * 1. bound with index
     * 2. a request parameter of _bulk API
     * 3. a parameter of an IndexRequest.
     */
    static List<List<IndexRequestWrapper>> prepareBatches(int batchSize, List<IndexRequestWrapper> indexRequestWrappers) {
        final Map<Integer, List<IndexRequestWrapper>> indexRequestsPerIndexAndPipelines = new HashMap<>();
        for (IndexRequestWrapper indexRequestWrapper : indexRequestWrappers) {
            // IndexRequests are grouped by their index + pipeline ids
            List<String> indexAndPipelineIds = new ArrayList<>();
            String index = indexRequestWrapper.getIndexRequest().index();
            List<String> pipelines = indexRequestWrapper.getPipelines();
            indexAndPipelineIds.add(index);
            indexAndPipelineIds.addAll(pipelines);
            int hashCode = indexAndPipelineIds.hashCode();
            indexRequestsPerIndexAndPipelines.putIfAbsent(hashCode, new ArrayList<>());
            indexRequestsPerIndexAndPipelines.get(hashCode).add(indexRequestWrapper);
        }
        List<List<IndexRequestWrapper>> batchedIndexRequests = new ArrayList<>();
        for (Map.Entry<Integer, List<IndexRequestWrapper>> indexRequestsPerKey : indexRequestsPerIndexAndPipelines.entrySet()) {
            for (int i = 0; i < indexRequestsPerKey.getValue().size(); i += Math.min(indexRequestsPerKey.getValue().size(), batchSize)) {
                batchedIndexRequests.add(
                    new ArrayList<>(
                        indexRequestsPerKey.getValue().subList(i, i + Math.min(batchSize, indexRequestsPerKey.getValue().size() - i))
                    )
                );
            }
        }
        return batchedIndexRequests;
    }

    /* visible for testing */
    static final class IndexRequestWrapper {
        private final int slot;
        private final IndexRequest indexRequest;
        private final List<String> pipelines;
        private final boolean hasFinalPipeline;

        IndexRequestWrapper(int slot, IndexRequest indexRequest, List<String> pipelines, boolean hasFinalPipeline) {
            this.slot = slot;
            this.indexRequest = indexRequest;
            this.pipelines = pipelines;
            this.hasFinalPipeline = hasFinalPipeline;
        }

        public int getSlot() {
            return slot;
        }

        public IndexRequest getIndexRequest() {
            return indexRequest;
        }

        public List<String> getPipelines() {
            return pipelines;
        }

        public boolean isHasFinalPipeline() {
            return hasFinalPipeline;
        }
    }

    private void executePipelinesInBatchRequests(
        final List<Integer> slots,
        final Iterator<String> pipelineIterator,
        final boolean hasFinalPipeline,
        final List<IndexRequest> indexRequests,
        final IntConsumer onDropped,
        final BiConsumer<Integer, Exception> onFailure,
        final AtomicInteger counter,
        final BiConsumer<Thread, Exception> onCompletion,
        final Thread originalThread
    ) {
        if (indexRequests.size() == 1) {
            executePipelines(
                slots.get(0),
                pipelineIterator,
                hasFinalPipeline,
                indexRequests.get(0),
                onDropped,
                onFailure,
                counter,
                onCompletion,
                originalThread
            );
            return;
        }
        while (pipelineIterator.hasNext()) {
            final String pipelineId = pipelineIterator.next();
            try {
                PipelineHolder holder = pipelines.get(pipelineId);
                if (holder == null) {
                    throw new IllegalArgumentException("pipeline with id [" + pipelineId + "] does not exist");
                }
                Pipeline pipeline = holder.pipeline;
                String originalIndex = indexRequests.get(0).indices()[0];
                Map<Integer, IndexRequest> slotIndexRequestMap = createSlotIndexRequestMap(slots, indexRequests);
                innerBatchExecute(slots, indexRequests, pipeline, onDropped, results -> {
                    for (int i = 0; i < results.size(); ++i) {
                        if (results.get(i).getException() != null) {
                            IndexRequest indexRequest = slotIndexRequestMap.get(results.get(i).getSlot());
                            logger.debug(
                                () -> new ParameterizedMessage(
                                    "failed to execute pipeline [{}] for document [{}/{}]",
                                    pipelineId,
                                    indexRequest.index(),
                                    indexRequest.id()
                                ),
                                results.get(i).getException()
                            );
                            onFailure.accept(results.get(i).getSlot(), results.get(i).getException());
                        }
                    }

                    Iterator<String> newPipelineIterator = pipelineIterator;
                    boolean newHasFinalPipeline = hasFinalPipeline;
                    // indexRequests are grouped for the same index and same pipelines
                    String newIndex = indexRequests.get(0).indices()[0];

                    // handle index change case
                    if (Objects.equals(originalIndex, newIndex) == false) {
                        if (hasFinalPipeline && pipelineIterator.hasNext() == false) {
                            totalMetrics.failed();
                            for (int slot : slots) {
                                onFailure.accept(
                                    slot,
                                    new IllegalStateException("final pipeline [" + pipelineId + "] can't change the target index")
                                );
                            }
                        } else {
                            // Drain old it so it's not looped over
                            pipelineIterator.forEachRemaining($ -> {});
                            for (IndexRequest indexRequest : indexRequests) {
                                indexRequest.isPipelineResolved(false);
                                resolvePipelines(null, indexRequest, state.metadata());
                                if (IngestService.NOOP_PIPELINE_NAME.equals(indexRequest.getFinalPipeline()) == false) {
                                    newPipelineIterator = Collections.singleton(indexRequest.getFinalPipeline()).iterator();
                                    newHasFinalPipeline = true;
                                } else {
                                    newPipelineIterator = Collections.emptyIterator();
                                }
                            }
                        }
                    }

                    if (newPipelineIterator.hasNext()) {
                        executePipelinesInBatchRequests(
                            slots,
                            newPipelineIterator,
                            newHasFinalPipeline,
                            indexRequests,
                            onDropped,
                            onFailure,
                            counter,
                            onCompletion,
                            originalThread
                        );
                    } else {
                        if (counter.addAndGet(-results.size()) == 0) {
                            onCompletion.accept(originalThread, null);
                        }
                        assert counter.get() >= 0;
                    }
                });
            } catch (Exception e) {
                StringBuilder documentLogBuilder = new StringBuilder();
                for (int i = 0; i < indexRequests.size(); ++i) {
                    IndexRequest indexRequest = indexRequests.get(i);
                    documentLogBuilder.append(indexRequest.index());
                    documentLogBuilder.append("/");
                    documentLogBuilder.append(indexRequest.id());
                    if (i < indexRequests.size() - 1) {
                        documentLogBuilder.append(", ");
                    }
                    onFailure.accept(slots.get(i), e);
                }
                logger.debug(
                    () -> new ParameterizedMessage(
                        "failed to execute pipeline [{}] for documents [{}]",
                        pipelineId,
                        documentLogBuilder.toString()
                    ),
                    e
                );
                if (counter.addAndGet(-indexRequests.size()) == 0) {
                    onCompletion.accept(originalThread, null);
                }
                assert counter.get() >= 0;
                break;
            }
        }
    }

    private void executePipelines(
        final int slot,
        final Iterator<String> it,
        final boolean hasFinalPipeline,
        final IndexRequest indexRequest,
        final IntConsumer onDropped,
        final BiConsumer<Integer, Exception> onFailure,
        final AtomicInteger counter,
        final BiConsumer<Thread, Exception> onCompletion,
        final Thread originalThread
    ) {
        while (it.hasNext()) {
            final String pipelineId = it.next();
            try {
                PipelineHolder holder = pipelines.get(pipelineId);
                if (holder == null) {
                    throw new IllegalArgumentException("pipeline with id [" + pipelineId + "] does not exist");
                }
                Pipeline pipeline = holder.pipeline;
                String originalIndex = indexRequest.indices()[0];
                innerExecute(slot, indexRequest, pipeline, onDropped, e -> {
                    if (e != null) {
                        logger.debug(
                            () -> new ParameterizedMessage(
                                "failed to execute pipeline [{}] for document [{}/{}]",
                                pipelineId,
                                indexRequest.index(),
                                indexRequest.id()
                            ),
                            e
                        );
                        onFailure.accept(slot, e);
                    }

                    Iterator<String> newIt = it;
                    boolean newHasFinalPipeline = hasFinalPipeline;
                    String newIndex = indexRequest.indices()[0];

                    if (Objects.equals(originalIndex, newIndex) == false) {
                        if (hasFinalPipeline && it.hasNext() == false) {
                            totalMetrics.failed();
                            onFailure.accept(
                                slot,
                                new IllegalStateException("final pipeline [" + pipelineId + "] can't change the target index")
                            );
                        } else {

                            // Drain old it so it's not looped over
                            it.forEachRemaining($ -> {});
                            indexRequest.isPipelineResolved(false);
                            resolvePipelines(null, indexRequest, state.metadata());
                            if (IngestService.NOOP_PIPELINE_NAME.equals(indexRequest.getFinalPipeline()) == false) {
                                newIt = Collections.singleton(indexRequest.getFinalPipeline()).iterator();
                                newHasFinalPipeline = true;
                            } else {
                                newIt = Collections.emptyIterator();
                            }
                        }
                    }

                    if (newIt.hasNext()) {
                        executePipelines(
                            slot,
                            newIt,
                            newHasFinalPipeline,
                            indexRequest,
                            onDropped,
                            onFailure,
                            counter,
                            onCompletion,
                            originalThread
                        );
                    } else {
                        if (counter.decrementAndGet() == 0) {
                            onCompletion.accept(originalThread, null);
                        }
                        assert counter.get() >= 0;
                    }
                });
            } catch (Exception e) {
                logger.debug(
                    () -> new ParameterizedMessage(
                        "failed to execute pipeline [{}] for document [{}/{}]",
                        pipelineId,
                        indexRequest.index(),
                        indexRequest.id()
                    ),
                    e
                );
                onFailure.accept(slot, e);
                if (counter.decrementAndGet() == 0) {
                    onCompletion.accept(originalThread, null);
                }
                assert counter.get() >= 0;
                break;
            }
        }
    }

    public IngestStats stats() {
        IngestStats.Builder statsBuilder = new IngestStats.Builder();
        statsBuilder.addTotalMetrics(totalMetrics);
        pipelines.forEach((id, holder) -> {
            Pipeline pipeline = holder.pipeline;
            CompoundProcessor rootProcessor = pipeline.getCompoundProcessor();
            statsBuilder.addPipelineMetrics(id, pipeline.getMetrics());
            List<Tuple<Processor, OperationMetrics>> processorMetrics = new ArrayList<>();
            getProcessorMetrics(rootProcessor, processorMetrics);
            processorMetrics.forEach(t -> {
                Processor processor = t.v1();
                OperationMetrics processorMetric = t.v2();
                statsBuilder.addProcessorMetrics(id, getProcessorName(processor), processor.getType(), processorMetric);
            });
        });
        return statsBuilder.build();
    }

    /**
     * Adds a listener that gets invoked with the current cluster state before processor factories
     * get invoked.
     * <p>
     * This is useful for components that are used by ingest processors, so that they have the opportunity to update
     * before these components get used by the ingest processor factory.
     */
    public void addIngestClusterStateListener(Consumer<ClusterState> listener) {
        ingestClusterStateListeners.add(listener);
    }

    // package private for testing
    static String getProcessorName(Processor processor) {
        // conditionals are implemented as wrappers around the real processor, so get the real processor for the correct type for the name
        if (processor instanceof ConditionalProcessor) {
            processor = ((ConditionalProcessor) processor).getInnerProcessor();
        }
        StringBuilder sb = new StringBuilder(5);
        sb.append(processor.getType());

        if (processor instanceof PipelineProcessor) {
            String pipelineName = ((PipelineProcessor) processor).getPipelineTemplate().newInstance(Collections.emptyMap()).execute();
            sb.append(":");
            sb.append(pipelineName);
        }
        String tag = processor.getTag();
        if (tag != null && !tag.isEmpty()) {
            sb.append(":");
            sb.append(tag);
        }
        return sb.toString();
    }

    private void innerExecute(
        int slot,
        IndexRequest indexRequest,
        Pipeline pipeline,
        IntConsumer itemDroppedHandler,
        Consumer<Exception> handler
    ) {
        if (pipeline.getProcessors().isEmpty()) {
            handler.accept(null);
            return;
        }

        long startTimeInNanos = System.nanoTime();
        // the pipeline specific stat holder may not exist and that is fine:
        // (e.g. the pipeline may have been removed while we're ingesting a document
        totalMetrics.before();
        String index = indexRequest.index();
        String id = indexRequest.id();
        String routing = indexRequest.routing();
        Long version = indexRequest.version();
        VersionType versionType = indexRequest.versionType();
        Map<String, Object> sourceAsMap = indexRequest.sourceAsMap();
        IngestDocument ingestDocument = new IngestDocument(index, id, routing, version, versionType, sourceAsMap);
        ingestDocument.executePipeline(pipeline, (result, e) -> {
            long ingestTimeInMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeInNanos);
            totalMetrics.after(ingestTimeInMillis);
            if (e != null) {
                totalMetrics.failed();
                handler.accept(e);
            } else if (result == null) {
                itemDroppedHandler.accept(slot);
                handler.accept(null);
            } else {
                updateIndexRequestWithIngestDocument(indexRequest, ingestDocument);
                handler.accept(null);
            }
        });
    }

    private void innerBatchExecute(
        List<Integer> slots,
        List<IndexRequest> indexRequests,
        Pipeline pipeline,
        IntConsumer itemDroppedHandler,
        Consumer<List<IngestDocumentWrapper>> handler
    ) {
        if (pipeline.getProcessors().isEmpty()) {
            handler.accept(toIngestDocumentWrappers(slots, indexRequests));
            return;
        }

        int size = indexRequests.size();
        long startTimeInNanos = System.nanoTime();
        // the pipeline specific stat holder may not exist and that is fine:
        // (e.g. the pipeline may have been removed while we're ingesting a document
        totalMetrics.beforeN(size);
        List<IngestDocumentWrapper> ingestDocumentWrappers = new ArrayList<>();
        Map<Integer, IndexRequest> slotToindexRequestMap = new HashMap<>();
        for (int i = 0; i < slots.size(); ++i) {
            slotToindexRequestMap.put(slots.get(i), indexRequests.get(i));
            ingestDocumentWrappers.add(toIngestDocumentWrapper(slots.get(i), indexRequests.get(i)));
        }
        AtomicInteger counter = new AtomicInteger(size);
        List<IngestDocumentWrapper> allResults = Collections.synchronizedList(new ArrayList<>());
        pipeline.batchExecute(ingestDocumentWrappers, results -> {
            if (results.isEmpty()) return;
            allResults.addAll(results);
            if (counter.addAndGet(-results.size()) == 0) {
                long ingestTimeInMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeInNanos);
                totalMetrics.afterN(size, ingestTimeInMillis);
                List<IngestDocumentWrapper> succeeded = new ArrayList<>();
                List<IngestDocumentWrapper> dropped = new ArrayList<>();
                List<IngestDocumentWrapper> exceptions = new ArrayList<>();
                for (IngestDocumentWrapper result : allResults) {
                    if (result.getException() != null) {
                        exceptions.add(result);
                    } else if (result.getIngestDocument() == null) {
                        dropped.add(result);
                    } else {
                        succeeded.add(result);
                    }
                }
                if (!exceptions.isEmpty()) {
                    totalMetrics.failedN(exceptions.size());
                }
                if (!dropped.isEmpty()) {
                    dropped.forEach(t -> itemDroppedHandler.accept(t.getSlot()));
                }
                for (IngestDocumentWrapper ingestDocumentWrapper : succeeded) {
                    updateIndexRequestWithIngestDocument(
                        slotToindexRequestMap.get(ingestDocumentWrapper.getSlot()),
                        ingestDocumentWrapper.getIngestDocument()
                    );
                }
                handler.accept(allResults);
            }
            assert counter.get() >= 0;
        });
    }

    @Override
    public void applyClusterState(final ClusterChangedEvent event) {
        state = event.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        // Publish cluster state to components that are used by processor factories before letting
        // processor factories create new processor instances.
        // (Note that this needs to be done also in the case when there is no change to ingest metadata, because in the case
        // when only the part of the cluster state that a component is interested in, is updated.)
        ingestClusterStateListeners.forEach(consumer -> consumer.accept(state));

        IngestMetadata newIngestMetadata = state.getMetadata().custom(IngestMetadata.TYPE);
        if (newIngestMetadata == null) {
            return;
        }

        try {
            innerUpdatePipelines(newIngestMetadata);
        } catch (OpenSearchParseException e) {
            logger.warn("failed to update ingest pipelines", e);
        }
    }

    void innerUpdatePipelines(IngestMetadata newIngestMetadata) {
        Map<String, PipelineHolder> existingPipelines = this.pipelines;

        // Lazy initialize these variables in order to favour the most like scenario that there are no pipeline changes:
        Map<String, PipelineHolder> newPipelines = null;
        List<OpenSearchParseException> exceptions = null;
        // Iterate over pipeline configurations in ingest metadata and constructs a new pipeline if there is no pipeline
        // or the pipeline configuration has been modified
        for (PipelineConfiguration newConfiguration : newIngestMetadata.getPipelines().values()) {
            PipelineHolder previous = existingPipelines.get(newConfiguration.getId());
            if (previous != null && previous.configuration.equals(newConfiguration)) {
                continue;
            }

            if (newPipelines == null) {
                newPipelines = new HashMap<>(existingPipelines);
            }
            try {
                Pipeline newPipeline = Pipeline.create(
                    newConfiguration.getId(),
                    newConfiguration.getConfigAsMap(),
                    processorFactories,
                    scriptService
                );
                newPipelines.put(newConfiguration.getId(), new PipelineHolder(newConfiguration, newPipeline));

                if (previous == null) {
                    continue;
                }
                Pipeline oldPipeline = previous.pipeline;
                newPipeline.getMetrics().add(oldPipeline.getMetrics());
                List<Tuple<Processor, OperationMetrics>> oldPerProcessMetrics = new ArrayList<>();
                List<Tuple<Processor, OperationMetrics>> newPerProcessMetrics = new ArrayList<>();
                getProcessorMetrics(oldPipeline.getCompoundProcessor(), oldPerProcessMetrics);
                getProcessorMetrics(newPipeline.getCompoundProcessor(), newPerProcessMetrics);
                // Best attempt to populate new processor metrics using a parallel array of the old metrics. This is not ideal since
                // the per processor metrics may get reset when the arrays don't match. However, to get to an ideal model, unique and
                // consistent id's per processor and/or semantic equals for each processor will be needed.
                if (newPerProcessMetrics.size() == oldPerProcessMetrics.size()) {
                    Iterator<Tuple<Processor, OperationMetrics>> oldMetricsIterator = oldPerProcessMetrics.iterator();
                    for (Tuple<Processor, OperationMetrics> compositeMetric : newPerProcessMetrics) {
                        String type = compositeMetric.v1().getType();
                        OperationMetrics metric = compositeMetric.v2();
                        if (oldMetricsIterator.hasNext()) {
                            Tuple<Processor, OperationMetrics> oldCompositeMetric = oldMetricsIterator.next();
                            String oldType = oldCompositeMetric.v1().getType();
                            OperationMetrics oldMetric = oldCompositeMetric.v2();
                            if (type.equals(oldType)) {
                                metric.add(oldMetric);
                            }
                        }
                    }
                }
            } catch (OpenSearchParseException e) {
                Pipeline pipeline = substitutePipeline(newConfiguration.getId(), e);
                newPipelines.put(newConfiguration.getId(), new PipelineHolder(newConfiguration, pipeline));
                if (exceptions == null) {
                    exceptions = new ArrayList<>();
                }
                exceptions.add(e);
            } catch (Exception e) {
                OpenSearchParseException parseException = new OpenSearchParseException(
                    "Error updating pipeline with id [" + newConfiguration.getId() + "]",
                    e
                );
                Pipeline pipeline = substitutePipeline(newConfiguration.getId(), parseException);
                newPipelines.put(newConfiguration.getId(), new PipelineHolder(newConfiguration, pipeline));
                if (exceptions == null) {
                    exceptions = new ArrayList<>();
                }
                exceptions.add(parseException);
            }
        }

        // Iterate over the current active pipelines and check whether they are missing in the pipeline configuration and
        // if so delete the pipeline from new Pipelines map:
        for (Map.Entry<String, PipelineHolder> entry : existingPipelines.entrySet()) {
            if (newIngestMetadata.getPipelines().get(entry.getKey()) == null) {
                if (newPipelines == null) {
                    newPipelines = new HashMap<>(existingPipelines);
                }
                newPipelines.remove(entry.getKey());
            }
        }

        if (newPipelines != null) {
            // Update the pipelines:
            this.pipelines = Collections.unmodifiableMap(newPipelines);

            // Rethrow errors that may have occurred during creating new pipeline instances:
            if (exceptions != null) {
                ExceptionsHelper.rethrowAndSuppress(exceptions);
            }
        }
    }

    /**
     * Gets all the Processors of the given type from within a Pipeline.
     * @param pipelineId the pipeline to inspect
     * @param clazz the Processor class to look for
     * @return True if the pipeline contains an instance of the Processor class passed in
     */
    public <P extends Processor> List<P> getProcessorsInPipeline(String pipelineId, Class<P> clazz) {
        Pipeline pipeline = getPipeline(pipelineId);
        if (pipeline == null) {
            throw new IllegalArgumentException("pipeline with id [" + pipelineId + "] does not exist");
        }

        List<P> processors = new ArrayList<>();
        for (Processor processor : pipeline.flattenAllProcessors()) {
            if (clazz.isAssignableFrom(processor.getClass())) {
                processors.add(clazz.cast(processor));
            }

            while (processor instanceof WrappingProcessor) {
                WrappingProcessor wrappingProcessor = (WrappingProcessor) processor;
                if (clazz.isAssignableFrom(wrappingProcessor.getInnerProcessor().getClass())) {
                    processors.add(clazz.cast(wrappingProcessor.getInnerProcessor()));
                }
                processor = wrappingProcessor.getInnerProcessor();
                // break in the case of self referencing processors in the event a processor author creates a
                // wrapping processor that has its inner processor refer to itself.
                if (wrappingProcessor == processor) {
                    break;
                }
            }
        }

        return processors;
    }

    private static Pipeline substitutePipeline(String id, OpenSearchParseException e) {
        String tag = e.getHeaderKeys().contains("processor_tag") ? e.getHeader("processor_tag").get(0) : null;
        String type = e.getHeaderKeys().contains("processor_type") ? e.getHeader("processor_type").get(0) : "unknown";
        String errorMessage = "pipeline with id [" + id + "] could not be loaded, caused by [" + e.getDetailedMessage() + "]";
        Processor failureProcessor = new AbstractProcessor(tag, "this is a placeholder processor") {
            @Override
            public IngestDocument execute(IngestDocument ingestDocument) {
                throw new IllegalStateException(errorMessage);
            }

            @Override
            public String getType() {
                return type;
            }
        };
        String description = "this is a place holder pipeline, because pipeline with id [" + id + "] could not be loaded";
        return new Pipeline(id, description, null, new CompoundProcessor(failureProcessor));
    }

    static class PipelineHolder {

        final PipelineConfiguration configuration;
        final Pipeline pipeline;

        PipelineHolder(PipelineConfiguration configuration, Pipeline pipeline) {
            this.configuration = Objects.requireNonNull(configuration);
            this.pipeline = Objects.requireNonNull(pipeline);
        }
    }

    public static void updateIndexRequestWithIngestDocument(IndexRequest indexRequest, IngestDocument ingestDocument) {
        Map<IngestDocument.Metadata, Object> metadataMap = ingestDocument.extractMetadata();
        // it's fine to set all metadata fields all the time, as ingest document holds their starting values
        // before ingestion, which might also get modified during ingestion.
        indexRequest.index((String) metadataMap.get(IngestDocument.Metadata.INDEX));
        indexRequest.id((String) metadataMap.get(IngestDocument.Metadata.ID));
        indexRequest.routing((String) metadataMap.get(IngestDocument.Metadata.ROUTING));
        indexRequest.version(((Number) metadataMap.get(IngestDocument.Metadata.VERSION)).longValue());
        if (metadataMap.get(IngestDocument.Metadata.VERSION_TYPE) != null) {
            indexRequest.versionType(VersionType.fromString((String) metadataMap.get(IngestDocument.Metadata.VERSION_TYPE)));
        }
        if (metadataMap.get(IngestDocument.Metadata.IF_SEQ_NO) != null) {
            indexRequest.setIfSeqNo(((Number) metadataMap.get(IngestDocument.Metadata.IF_SEQ_NO)).longValue());
        }
        if (metadataMap.get(IngestDocument.Metadata.IF_PRIMARY_TERM) != null) {
            indexRequest.setIfPrimaryTerm(((Number) metadataMap.get(IngestDocument.Metadata.IF_PRIMARY_TERM)).longValue());
        }
        indexRequest.source(ingestDocument.getSourceAndMetadata(), indexRequest.getContentType());
    }

    static IngestDocument toIngestDocument(IndexRequest indexRequest) {
        return new IngestDocument(
            indexRequest.index(),
            indexRequest.id(),
            indexRequest.routing(),
            indexRequest.version(),
            indexRequest.versionType(),
            indexRequest.sourceAsMap()
        );
    }

    private static IngestDocumentWrapper toIngestDocumentWrapper(int slot, IndexRequest indexRequest) {
        return new IngestDocumentWrapper(slot, toIngestDocument(indexRequest), null);
    }

    private static List<IngestDocumentWrapper> toIngestDocumentWrappers(List<Integer> slots, List<IndexRequest> indexRequests) {
        List<IngestDocumentWrapper> ingestDocumentWrappers = new ArrayList<>();
        for (int i = 0; i < slots.size(); ++i) {
            ingestDocumentWrappers.add(toIngestDocumentWrapper(slots.get(i), indexRequests.get(i)));
        }
        return ingestDocumentWrappers;
    }

    private static Map<Integer, IndexRequest> createSlotIndexRequestMap(List<Integer> slots, List<IndexRequest> indexRequests) {
        Map<Integer, IndexRequest> slotIndexRequestMap = new HashMap<>();
        for (int i = 0; i < slots.size(); ++i) {
            slotIndexRequestMap.put(slots.get(i), indexRequests.get(i));
        }
        return slotIndexRequestMap;
    }
}
