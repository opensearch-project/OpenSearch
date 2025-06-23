/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.metrics.OperationMetrics;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.ingest.ConfigurationUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

import static org.opensearch.ingest.ConfigurationUtils.IGNORE_FAILURE_KEY;
import static org.opensearch.ingest.ConfigurationUtils.TAG_KEY;
import static org.opensearch.ingest.Pipeline.DESCRIPTION_KEY;
import static org.opensearch.ingest.Pipeline.VERSION_KEY;

/**
 * Specialization of {@link Pipeline} that adds metrics to track executions of the pipeline and individual processors.
 */
class PipelineWithMetrics extends Pipeline {

    private final OperationMetrics totalRequestMetrics;
    private final OperationMetrics totalResponseMetrics;
    private final OperationMetrics totalPhaseResultsMetrics;
    private final OperationMetrics pipelineRequestMetrics = new OperationMetrics();
    private final OperationMetrics pipelineResponseMetrics = new OperationMetrics();
    private final Map<String, OperationMetrics> requestProcessorMetrics = new HashMap<>();
    private final Map<String, OperationMetrics> responseProcessorMetrics = new HashMap<>();
    private final Map<String, OperationMetrics> phaseResultsProcessorMetrics = new HashMap<>();

    PipelineWithMetrics(
        String id,
        String description,
        Integer version,
        List<SearchRequestProcessor> requestProcessors,
        List<SearchResponseProcessor> responseProcessors,
        List<SearchPhaseResultsProcessor> phaseResultsProcessors,
        NamedWriteableRegistry namedWriteableRegistry,
        OperationMetrics totalRequestMetrics,
        OperationMetrics totalResponseMetrics,
        OperationMetrics totalPhaseResultMetrics,
        LongSupplier relativeTimeSupplier
    ) {
        super(
            id,
            description,
            version,
            requestProcessors,
            responseProcessors,
            phaseResultsProcessors,
            namedWriteableRegistry,
            relativeTimeSupplier
        );
        this.totalRequestMetrics = totalRequestMetrics;
        this.totalResponseMetrics = totalResponseMetrics;
        this.totalPhaseResultsMetrics = totalPhaseResultMetrics;
        for (Processor requestProcessor : getSearchRequestProcessors()) {
            requestProcessorMetrics.putIfAbsent(getProcessorKey(requestProcessor), new OperationMetrics());
        }
        for (Processor responseProcessor : getSearchResponseProcessors()) {
            responseProcessorMetrics.putIfAbsent(getProcessorKey(responseProcessor), new OperationMetrics());
        }
        for (Processor phaseResultsProcessor : getSearchPhaseResultsProcessors()) {
            phaseResultsProcessorMetrics.putIfAbsent(getProcessorKey(phaseResultsProcessor), new OperationMetrics());
        }
    }

    static PipelineWithMetrics create(
        String id,
        Map<String, Object> config,
        Map<String, Processor.Factory<SearchRequestProcessor>> requestProcessorFactories,
        Map<String, Processor.Factory<SearchResponseProcessor>> responseProcessorFactories,
        Map<String, Processor.Factory<SearchPhaseResultsProcessor>> phaseResultsProcessorFactories,
        NamedWriteableRegistry namedWriteableRegistry,
        OperationMetrics totalRequestProcessingMetrics,
        OperationMetrics totalResponseProcessingMetrics,
        OperationMetrics totalPhaseResultProcessingMetrics,
        Processor.PipelineContext pipelineContext
    ) throws Exception {
        String description = ConfigurationUtils.readOptionalStringProperty(null, null, config, DESCRIPTION_KEY);
        Integer version = ConfigurationUtils.readIntProperty(null, null, config, VERSION_KEY, null);
        List<Map<String, Object>> requestProcessorConfigs = ConfigurationUtils.readOptionalList(null, null, config, REQUEST_PROCESSORS_KEY);
        List<SearchRequestProcessor> requestProcessors = readProcessors(
            requestProcessorFactories,
            requestProcessorConfigs,
            pipelineContext
        );
        List<Map<String, Object>> responseProcessorConfigs = ConfigurationUtils.readOptionalList(
            null,
            null,
            config,
            RESPONSE_PROCESSORS_KEY
        );
        List<SearchResponseProcessor> responseProcessors = readProcessors(
            responseProcessorFactories,
            responseProcessorConfigs,
            pipelineContext
        );
        List<Map<String, Object>> phaseResultsProcessorConfigs = ConfigurationUtils.readOptionalList(
            null,
            null,
            config,
            PHASE_PROCESSORS_KEY
        );
        List<SearchPhaseResultsProcessor> phaseResultsProcessors = readProcessors(
            phaseResultsProcessorFactories,
            phaseResultsProcessorConfigs,
            pipelineContext
        );
        if (config.isEmpty() == false) {
            throw new OpenSearchParseException(
                "pipeline ["
                    + id
                    + "] doesn't support one or more provided configuration parameters "
                    + Arrays.toString(config.keySet().toArray())
            );
        }
        return new PipelineWithMetrics(
            id,
            description,
            version,
            requestProcessors,
            responseProcessors,
            phaseResultsProcessors,
            namedWriteableRegistry,
            totalRequestProcessingMetrics,
            totalResponseProcessingMetrics,
            totalPhaseResultProcessingMetrics,
            System::nanoTime
        );

    }

    private static <T extends Processor> List<T> readProcessors(
        Map<String, Processor.Factory<T>> processorFactories,
        List<Map<String, Object>> requestProcessorConfigs,
        Processor.PipelineContext pipelineContext
    ) throws Exception {
        List<T> processors = new ArrayList<>();
        if (requestProcessorConfigs == null) {
            return processors;
        }
        for (Map<String, Object> processorConfigWithKey : requestProcessorConfigs) {
            for (Map.Entry<String, Object> entry : processorConfigWithKey.entrySet()) {
                String type = entry.getKey();
                if (!processorFactories.containsKey(type)) {
                    throw new IllegalArgumentException("Invalid processor type " + type);
                }
                Map<String, Object> config = (Map<String, Object>) entry.getValue();
                String tag = ConfigurationUtils.readOptionalStringProperty(null, null, config, TAG_KEY);
                boolean ignoreFailure = ConfigurationUtils.readBooleanProperty(null, null, config, IGNORE_FAILURE_KEY, false);
                String description = ConfigurationUtils.readOptionalStringProperty(null, tag, config, DESCRIPTION_KEY);
                processors.add(
                    processorFactories.get(type).create(processorFactories, tag, description, ignoreFailure, config, pipelineContext)
                );
                if (config.isEmpty() == false) {
                    String processorName = type;
                    if (tag != null) {
                        processorName = processorName + ":" + tag;
                    }
                    throw new OpenSearchParseException(
                        "processor ["
                            + processorName
                            + "] doesn't support one or more provided configuration parameters: "
                            + Arrays.toString(config.keySet().toArray())
                    );
                }
            }
        }
        return Collections.unmodifiableList(processors);
    }

    @Override
    protected void beforeTransformRequest() {
        super.beforeTransformRequest();
        totalRequestMetrics.before();
        pipelineRequestMetrics.before();
    }

    @Override
    protected void afterTransformRequest(long timeInNanos) {
        super.afterTransformRequest(timeInNanos);
        totalRequestMetrics.after(timeInNanos);
        pipelineRequestMetrics.after(timeInNanos);
    }

    @Override
    protected void onTransformRequestFailure() {
        super.onTransformRequestFailure();
        totalRequestMetrics.failed();
        pipelineRequestMetrics.failed();
    }

    protected void beforeRequestProcessor(Processor processor) {
        requestProcessorMetrics.get(getProcessorKey(processor)).before();
    }

    protected void afterRequestProcessor(Processor processor, long timeInNanos) {
        requestProcessorMetrics.get(getProcessorKey(processor)).after(timeInNanos);
    }

    protected void onRequestProcessorFailed(Processor processor) {
        requestProcessorMetrics.get(getProcessorKey(processor)).failed();
    }

    protected void beforeTransformResponse() {
        super.beforeTransformResponse();
        totalResponseMetrics.before();
        pipelineResponseMetrics.before();
    }

    protected void afterTransformResponse(long timeInNanos) {
        super.afterTransformResponse(timeInNanos);
        totalResponseMetrics.after(timeInNanos);
        pipelineResponseMetrics.after(timeInNanos);
    }

    protected void onTransformResponseFailure() {
        super.onTransformResponseFailure();
        totalResponseMetrics.failed();
        pipelineResponseMetrics.failed();
    }

    protected void beforeResponseProcessor(Processor processor) {
        responseProcessorMetrics.get(getProcessorKey(processor)).before();
    }

    protected void afterResponseProcessor(Processor processor, long timeInNanos) {
        responseProcessorMetrics.get(getProcessorKey(processor)).after(timeInNanos);
    }

    protected void onResponseProcessorFailed(Processor processor) {
        responseProcessorMetrics.get(getProcessorKey(processor)).failed();
    }

    @Override
    protected void beforeTransformPhaseResults() {
        super.beforeTransformPhaseResults();
        totalPhaseResultsMetrics.before();
        // Phase results processing is also tracked as part of individual pipeline request processing
        pipelineRequestMetrics.before();
    }

    @Override
    protected void afterTransformPhaseResults(long timeInNanos) {
        super.afterTransformPhaseResults(timeInNanos);
        totalPhaseResultsMetrics.after(timeInNanos);
        // Phase results processing is also tracked as part of individual pipeline request processing
        pipelineRequestMetrics.after(timeInNanos);
    }

    @Override
    protected void onTransformPhaseResultsFailure() {
        super.onTransformPhaseResultsFailure();
        totalPhaseResultsMetrics.failed();
        // Phase results processing failures are also tracked as part of individual pipeline request processing
        pipelineRequestMetrics.failed();
    }

    protected void beforePhaseResultsProcessor(Processor processor) {
        phaseResultsProcessorMetrics.get(getProcessorKey(processor)).before();
    }

    protected void afterPhaseResultsProcessor(Processor processor, long timeInNanos) {
        phaseResultsProcessorMetrics.get(getProcessorKey(processor)).after(timeInNanos);
    }

    protected void onPhaseResultsProcessorFailed(Processor processor) {
        phaseResultsProcessorMetrics.get(getProcessorKey(processor)).failed();
    }

    void copyMetrics(PipelineWithMetrics oldPipeline) {
        pipelineRequestMetrics.add(oldPipeline.pipelineRequestMetrics);
        pipelineResponseMetrics.add(oldPipeline.pipelineResponseMetrics);
        copyProcessorMetrics(requestProcessorMetrics, oldPipeline.requestProcessorMetrics);
        copyProcessorMetrics(responseProcessorMetrics, oldPipeline.responseProcessorMetrics);
        copyProcessorMetrics(phaseResultsProcessorMetrics, oldPipeline.phaseResultsProcessorMetrics);
    }

    private static <T extends Processor> void copyProcessorMetrics(
        Map<String, OperationMetrics> newProcessorMetrics,
        Map<String, OperationMetrics> oldProcessorMetrics
    ) {
        for (Map.Entry<String, OperationMetrics> oldProcessorMetric : oldProcessorMetrics.entrySet()) {
            if (newProcessorMetrics.containsKey(oldProcessorMetric.getKey())) {
                newProcessorMetrics.get(oldProcessorMetric.getKey()).add(oldProcessorMetric.getValue());
            }
        }
    }

    private static String getProcessorKey(Processor processor) {
        String key = processor.getType();
        if (processor.getTag() != null) {
            return key + ":" + processor.getTag();
        }
        return key;
    }

    void populateStats(SearchPipelineStats.Builder statsBuilder) {
        statsBuilder.addPipelineStats(getId(), pipelineRequestMetrics, pipelineResponseMetrics);
        for (Processor processor : getSearchRequestProcessors()) {
            String key = getProcessorKey(processor);
            statsBuilder.addRequestProcessorStats(getId(), key, processor.getType(), requestProcessorMetrics.get(key));
        }
        for (Processor processor : getSearchResponseProcessors()) {
            String key = getProcessorKey(processor);
            statsBuilder.addResponseProcessorStats(getId(), key, processor.getType(), responseProcessorMetrics.get(key));
        }
        for (Processor phaseResultsProcessor : getSearchPhaseResultsProcessors()) {
            String key = getProcessorKey(phaseResultsProcessor);
            statsBuilder.addPhaseResultsProcessorStats(
                getId(),
                key,
                phaseResultsProcessor.getType(),
                phaseResultsProcessorMetrics.get(key)
            );
        }
    }
}
