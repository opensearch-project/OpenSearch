/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.opensearch.plugins.SearchPipelinePlugin.SystemGeneratedSearchPipelineConfigKeys.SEARCH_REQUEST;
import static org.opensearch.search.pipeline.SystemGeneratedProcessor.ExecutionStage.POST_USER_DEFINED;
import static org.opensearch.search.pipeline.SystemGeneratedProcessor.ExecutionStage.PRE_USER_DEFINED;

class SystemGeneratedPipelineWithMetrics extends Pipeline {
    private final SystemGeneratedProcessorMetrics systemGeneratedProcessorMetrics;
    private static final String ID = "_system_generated_pipeline";
    private static final String PIPELINE_DESCRIPTION = "This is a system generated search pipeline.";
    private static final String TAG = "system_generated_processor";
    private static final String PROCESSOR_DESCRIPTION = "This is a system generated search processor.";
    private static final boolean IGNORE_FAILURE = false;
    private static final Processor.PipelineContext PIPELINE_CONTEXT = new Processor.PipelineContext(
        Processor.PipelineSource.SEARCH_REQUEST
    );

    SystemGeneratedPipelineWithMetrics(
        String id,
        String description,
        Integer version,
        List<SearchRequestProcessor> requestProcessors,
        List<SearchResponseProcessor> responseProcessors,
        List<SearchPhaseResultsProcessor> phaseResultsProcessors,
        NamedWriteableRegistry namedWriteableRegistry,
        LongSupplier relativeTimeSupplier,
        SystemGeneratedProcessorMetrics systemGeneratedProcessorMetrics
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
        this.systemGeneratedProcessorMetrics = systemGeneratedProcessorMetrics;
    }

    static SystemGeneratedPipelineHolder create(
        Map<String, Object> config,
        Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchRequestProcessor>> requestProcessorFactories,
        Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchResponseProcessor>> responseProcessorFactories,
        Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchPhaseResultsProcessor>> phaseResultsProcessorFactories,
        NamedWriteableRegistry namedWriteableRegistry,
        SystemGeneratedProcessorMetrics systemGeneratedProcessorMetrics,
        List<String> enabledSystemGeneratedFactories
    ) throws Exception {

        final LongSupplier relativeTimeSupplier = System::nanoTime;
        final ProcessorLists<SearchRequestProcessor> requestProcessorLists = generateProcessors(
            requestProcessorFactories,
            Processor.ProcessorType.SEARCH_REQUEST,
            config,
            relativeTimeSupplier,
            systemGeneratedProcessorMetrics,
            enabledSystemGeneratedFactories
        );

        final ProcessorLists<SearchPhaseResultsProcessor> phaseResultsProcessorLists = generateProcessors(
            phaseResultsProcessorFactories,
            Processor.ProcessorType.SEARCH_PHASE_RESULTS,
            config,
            relativeTimeSupplier,
            systemGeneratedProcessorMetrics,
            enabledSystemGeneratedFactories
        );

        final ProcessorLists<SearchResponseProcessor> responseProcessorLists = generateProcessors(
            responseProcessorFactories,
            Processor.ProcessorType.SEARCH_RESPONSE,
            config,
            relativeTimeSupplier,
            systemGeneratedProcessorMetrics,
            enabledSystemGeneratedFactories
        );

        final SystemGeneratedPipelineWithMetrics prePipeline = new SystemGeneratedPipelineWithMetrics(
            ID,
            PIPELINE_DESCRIPTION,
            null,
            requestProcessorLists.pre,
            responseProcessorLists.pre,
            phaseResultsProcessorLists.pre,
            namedWriteableRegistry,
            relativeTimeSupplier,
            systemGeneratedProcessorMetrics
        );

        final SystemGeneratedPipelineWithMetrics postPipeline = new SystemGeneratedPipelineWithMetrics(
            ID,
            PIPELINE_DESCRIPTION,
            null,
            requestProcessorLists.post,
            responseProcessorLists.post,
            phaseResultsProcessorLists.post,
            namedWriteableRegistry,
            relativeTimeSupplier,
            systemGeneratedProcessorMetrics
        );

        return new SystemGeneratedPipelineHolder(prePipeline, postPipeline);
    }

    static class ProcessorLists<T> {
        final List<T> pre = new ArrayList<>();
        final List<T> post = new ArrayList<>();
    }

    private static <T extends Processor> ProcessorLists<T> generateProcessors(
        Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<T>> factories,
        Processor.ProcessorType type,
        Map<String, Object> config,
        LongSupplier relativeTimeSupplier,
        SystemGeneratedProcessorMetrics systemGeneratedProcessorMetrics,
        List<String> enabledSystemGeneratedFactories
    ) throws Exception {
        ProcessorLists<T> lists = new ProcessorLists<>();
        if (enabledSystemGeneratedFactories.isEmpty()) {
            return lists;
        }
        boolean isAllEnabled = enabledSystemGeneratedFactories.contains("*");
        ProcessorGenerationContext context = new ProcessorGenerationContext((SearchRequest) config.get(SEARCH_REQUEST));

        for (Map.Entry<String, SystemGeneratedProcessor.SystemGeneratedFactory<T>> entry : factories.entrySet()) {
            String factoryType = entry.getKey();
            if (isAllEnabled == false && enabledSystemGeneratedFactories.contains(factoryType) == false) {
                continue;
            }
            SystemGeneratedProcessor.SystemGeneratedFactory<T> factory = entry.getValue();

            long start = relativeTimeSupplier.getAsLong();
            systemGeneratedProcessorMetrics.beforeEvaluation(factoryType, type);
            boolean shouldGenerate;
            try {
                shouldGenerate = factory.shouldGenerate(context);
                systemGeneratedProcessorMetrics.afterEvaluation(factoryType, type, relativeTimeSupplier.getAsLong() - start);
            } catch (Exception e) {
                systemGeneratedProcessorMetrics.onEvaluationFailure(factoryType, type);
                throw e;
            }

            if (!shouldGenerate) continue;

            start = relativeTimeSupplier.getAsLong();
            systemGeneratedProcessorMetrics.beforeGeneration(factoryType, type);
            try {
                T processor = factory.create(
                    Collections.unmodifiableMap(factories),
                    TAG,
                    PROCESSOR_DESCRIPTION,
                    IGNORE_FAILURE,
                    config,
                    PIPELINE_CONTEXT
                );
                systemGeneratedProcessorMetrics.afterGeneration(factoryType, type, relativeTimeSupplier.getAsLong() - start);

                SystemGeneratedProcessor systemGeneratedProcessor = (SystemGeneratedProcessor) processor;
                (PRE_USER_DEFINED.equals(systemGeneratedProcessor.getExecutionStage()) ? lists.pre : lists.post).add(processor);
            } catch (Exception e) {
                systemGeneratedProcessorMetrics.onGenerationFailure(factoryType, type);
                throw e;
            }
        }

        ensureSingleProcessor(type.name(), PRE_USER_DEFINED, lists.pre);
        ensureSingleProcessor(type.name(), POST_USER_DEFINED, lists.post);

        return lists;
    }

    private static <T extends Processor> void ensureSingleProcessor(
        String typeName,
        SystemGeneratedProcessor.ExecutionStage stage,
        List<T> processors
    ) {
        if (processors.size() > 1) {
            String processorTypes = processors.stream().map(Processor::getType).collect(Collectors.joining(","));
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Cannot support more than one system generated %s processor to be executed %s. Now we have [%s].",
                    typeName,
                    stage,
                    processorTypes
                )
            );
        }
    }

    /**
     * Evaluate if there is any conflict between processors
     * @param processorConflictEvaluationContext The context to evaluate the conflicts
     */
    void evaluateConflicts(ProcessorConflictEvaluationContext processorConflictEvaluationContext) {
        evaluateConflicts(getSearchRequestProcessors(), processorConflictEvaluationContext);
        evaluateConflicts(getSearchPhaseResultsProcessors(), processorConflictEvaluationContext);
        evaluateConflicts(getSearchResponseProcessors(), processorConflictEvaluationContext);
    }

    private <T extends Processor> void evaluateConflicts(
        List<T> processors,
        ProcessorConflictEvaluationContext processorConflictEvaluationContext
    ) {
        for (T processor : processors) {
            ((SystemGeneratedProcessor) processor).evaluateConflicts(processorConflictEvaluationContext);
        }
    }

    @Override
    protected void beforeRequestProcessor(Processor processor) {
        super.beforeRequestProcessor(processor);
        systemGeneratedProcessorMetrics.beforeProcess(processor.getType(), Processor.ProcessorType.SEARCH_REQUEST);
    }

    @Override
    protected void afterRequestProcessor(Processor processor, long timeInNanos) {
        super.afterRequestProcessor(processor, timeInNanos);
        systemGeneratedProcessorMetrics.afterProcess(processor.getType(), Processor.ProcessorType.SEARCH_REQUEST, timeInNanos);
    }

    @Override
    protected void onRequestProcessorFailed(Processor processor) {
        super.onRequestProcessorFailed(processor);
        systemGeneratedProcessorMetrics.onProcessFailure(processor.getType(), Processor.ProcessorType.SEARCH_REQUEST);
    }

    @Override
    protected void beforeResponseProcessor(Processor processor) {
        super.beforeResponseProcessor(processor);
        systemGeneratedProcessorMetrics.beforeProcess(processor.getType(), Processor.ProcessorType.SEARCH_RESPONSE);
    }

    @Override
    protected void afterResponseProcessor(Processor processor, long timeInNanos) {
        super.afterResponseProcessor(processor, timeInNanos);
        systemGeneratedProcessorMetrics.afterProcess(processor.getType(), Processor.ProcessorType.SEARCH_RESPONSE, timeInNanos);
    }

    @Override
    protected void onResponseProcessorFailed(Processor processor) {
        super.onResponseProcessorFailed(processor);
        systemGeneratedProcessorMetrics.onProcessFailure(processor.getType(), Processor.ProcessorType.SEARCH_RESPONSE);
    }
}
