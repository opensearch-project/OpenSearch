/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.common.metrics.OperationMetrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Metrics for the system generated processors
 */
class SystemGeneratedProcessorMetrics {
    private final Map<Processor.ProcessorType, Map<String, FactoryMetrics>> factoryMetrics = new ConcurrentHashMap<>();
    private final Map<Processor.ProcessorType, Map<String, OperationMetrics>> processorMetrics = new ConcurrentHashMap<>();

    SystemGeneratedProcessorMetrics() {
        for (Processor.ProcessorType type : Processor.ProcessorType.values()) {
            factoryMetrics.put(type, new ConcurrentHashMap<>());
            processorMetrics.put(type, new ConcurrentHashMap<>());
        }
    }

    void beforeEvaluation(String factoryType, Processor.ProcessorType processorType) {
        getOrCreateFactoryMetrics(factoryType, processorType).evaluation.before();
    }

    void afterEvaluation(String factoryType, Processor.ProcessorType processorType, long timeInNanos) {
        getOrCreateFactoryMetrics(factoryType, processorType).evaluation.after(timeInNanos);
    }

    void onEvaluationFailure(String factoryType, Processor.ProcessorType processorType) {
        getOrCreateFactoryMetrics(factoryType, processorType).evaluation.failed();
    }

    void beforeGeneration(String factoryType, Processor.ProcessorType processorType) {
        getOrCreateFactoryMetrics(factoryType, processorType).generation.before();
    }

    void afterGeneration(String factoryType, Processor.ProcessorType processorType, long timeInNanos) {
        getOrCreateFactoryMetrics(factoryType, processorType).generation.after(timeInNanos);
    }

    void onGenerationFailure(String factoryType, Processor.ProcessorType processorType) {
        getOrCreateFactoryMetrics(factoryType, processorType).generation.failed();
    }

    void beforeProcess(String processorKey, Processor.ProcessorType processorType) {
        getOrCreateProcessorMetrics(processorKey, processorType).before();
    }

    void afterProcess(String processorKey, Processor.ProcessorType processorType, long timeInNanos) {
        getOrCreateProcessorMetrics(processorKey, processorType).after(timeInNanos);
    }

    void onProcessFailure(String processorKey, Processor.ProcessorType processorType) {
        getOrCreateProcessorMetrics(processorKey, processorType).failed();
    }

    private FactoryMetrics getOrCreateFactoryMetrics(String factoryType, Processor.ProcessorType processorType) {
        return factoryMetrics.computeIfAbsent(processorType, k -> new HashMap<>()).computeIfAbsent(factoryType, k -> new FactoryMetrics());
    }

    private OperationMetrics getOrCreateProcessorMetrics(String processorKey, Processor.ProcessorType processorType) {
        return processorMetrics.computeIfAbsent(processorType, k -> new HashMap<>())
            .computeIfAbsent(processorKey, k -> new OperationMetrics());
    }

    SearchPipelineStats.FactoryDetailStats getFactoryStats() {
        List<SearchPipelineStats.FactoryStats> requestProcessorFactoryStats = buildFactoryStats(Processor.ProcessorType.SEARCH_REQUEST);

        List<SearchPipelineStats.FactoryStats> responseProcessorFactoryStats = buildFactoryStats(Processor.ProcessorType.SEARCH_RESPONSE);

        return new SearchPipelineStats.FactoryDetailStats(requestProcessorFactoryStats, responseProcessorFactoryStats);
    }

    private List<SearchPipelineStats.FactoryStats> buildFactoryStats(Processor.ProcessorType type) {
        List<SearchPipelineStats.FactoryStats> stats = new ArrayList<>();
        factoryMetrics.get(type).forEach((factoryType, metric) -> {
            stats.add(new SearchPipelineStats.FactoryStats(factoryType, metric.evaluation.createStats(), metric.generation.createStats()));
        });
        return stats;
    }

    SearchPipelineStats.PipelineDetailStats getProcessorStats() {
        List<SearchPipelineStats.ProcessorStats> requestProcessorStats = buildProcessorStats(Processor.ProcessorType.SEARCH_REQUEST);

        List<SearchPipelineStats.ProcessorStats> responseProcessorStats = buildProcessorStats(Processor.ProcessorType.SEARCH_RESPONSE);

        return new SearchPipelineStats.PipelineDetailStats(requestProcessorStats, responseProcessorStats);
    }

    private List<SearchPipelineStats.ProcessorStats> buildProcessorStats(Processor.ProcessorType type) {
        List<SearchPipelineStats.ProcessorStats> stats = new ArrayList<>();
        processorMetrics.get(type).forEach((processorType, metric) -> {
            stats.add(new SearchPipelineStats.ProcessorStats(processorType, processorType, metric.createStats()));
        });
        return stats;
    }

    /**
     * Metrics for a system generated factory
     */
    class FactoryMetrics {
        private final OperationMetrics evaluation = new OperationMetrics(TimeUnit.MICROSECONDS);
        private final OperationMetrics generation = new OperationMetrics(TimeUnit.MICROSECONDS);

        public OperationMetrics evaluation() {
            return evaluation;
        }

        public OperationMetrics generation() {
            return generation;
        }
    }
}
