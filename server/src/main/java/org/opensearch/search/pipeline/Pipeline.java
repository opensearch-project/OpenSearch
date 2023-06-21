/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.OpenSearchParseException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.metrics.OperationMetrics;
import org.opensearch.ingest.ConfigurationUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.opensearch.ingest.ConfigurationUtils.TAG_KEY;
import static org.opensearch.ingest.Pipeline.DESCRIPTION_KEY;
import static org.opensearch.ingest.Pipeline.VERSION_KEY;

/**
 * Concrete representation of a search pipeline, holding multiple processors.
 */
class Pipeline {

    public static final String REQUEST_PROCESSORS_KEY = "request_processors";
    public static final String RESPONSE_PROCESSORS_KEY = "response_processors";
    private final String id;
    private final String description;
    private final Integer version;

    // TODO: Refactor org.opensearch.ingest.CompoundProcessor to implement our generic Processor interface
    // Then these can be CompoundProcessors instead of lists.
    private final List<ProcessorWithMetrics<SearchRequestProcessor>> searchRequestProcessors;
    private final List<ProcessorWithMetrics<SearchResponseProcessor>> searchResponseProcessors;

    private final NamedWriteableRegistry namedWriteableRegistry;
    private final OperationMetrics totalRequestMetrics;
    private final OperationMetrics totalResponseMetrics;
    private final OperationMetrics pipelineRequestMetrics = new OperationMetrics();
    private final OperationMetrics pipelineResponseMetrics = new OperationMetrics();
    private final LongSupplier relativeTimeSupplier;

    private static class ProcessorWithMetrics<T extends Processor> {
        private final T processor;
        private final OperationMetrics metrics = new OperationMetrics();

        public ProcessorWithMetrics(T processor) {
            this.processor = processor;
        }
    }

    private Pipeline(
        String id,
        @Nullable String description,
        @Nullable Integer version,
        List<SearchRequestProcessor> requestProcessors,
        List<SearchResponseProcessor> responseProcessors,
        NamedWriteableRegistry namedWriteableRegistry,
        OperationMetrics totalRequestMetrics,
        OperationMetrics totalResponseMetrics,
        LongSupplier relativeTimeSupplier
    ) {
        this.id = id;
        this.description = description;
        this.version = version;
        this.searchRequestProcessors = requestProcessors.stream().map(ProcessorWithMetrics::new).collect(Collectors.toList());
        this.searchResponseProcessors = responseProcessors.stream().map(ProcessorWithMetrics::new).collect(Collectors.toList());
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.totalRequestMetrics = totalRequestMetrics;
        this.totalResponseMetrics = totalResponseMetrics;
        this.relativeTimeSupplier = relativeTimeSupplier;
    }

    static Pipeline create(
        String id,
        Map<String, Object> config,
        Map<String, Processor.Factory<SearchRequestProcessor>> requestProcessorFactories,
        Map<String, Processor.Factory<SearchResponseProcessor>> responseProcessorFactories,
        NamedWriteableRegistry namedWriteableRegistry,
        OperationMetrics totalRequestProcessingMetrics,
        OperationMetrics totalResponseProcessingMetrics
    ) throws Exception {
        String description = ConfigurationUtils.readOptionalStringProperty(null, null, config, DESCRIPTION_KEY);
        Integer version = ConfigurationUtils.readIntProperty(null, null, config, VERSION_KEY, null);
        List<Map<String, Object>> requestProcessorConfigs = ConfigurationUtils.readOptionalList(null, null, config, REQUEST_PROCESSORS_KEY);
        List<SearchRequestProcessor> requestProcessors = readProcessors(requestProcessorFactories, requestProcessorConfigs);
        List<Map<String, Object>> responseProcessorConfigs = ConfigurationUtils.readOptionalList(
            null,
            null,
            config,
            RESPONSE_PROCESSORS_KEY
        );
        List<SearchResponseProcessor> responseProcessors = readProcessors(responseProcessorFactories, responseProcessorConfigs);
        if (config.isEmpty() == false) {
            throw new OpenSearchParseException(
                "pipeline ["
                    + id
                    + "] doesn't support one or more provided configuration parameters "
                    + Arrays.toString(config.keySet().toArray())
            );
        }
        return new Pipeline(
            id,
            description,
            version,
            requestProcessors,
            responseProcessors,
            namedWriteableRegistry,
            totalRequestProcessingMetrics,
            totalResponseProcessingMetrics,
            System::nanoTime
        );
    }

    private static <T extends Processor> List<T> readProcessors(
        Map<String, Processor.Factory<T>> processorFactories,
        List<Map<String, Object>> requestProcessorConfigs
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
                String description = ConfigurationUtils.readOptionalStringProperty(null, tag, config, DESCRIPTION_KEY);
                processors.add(processorFactories.get(type).create(processorFactories, tag, description, config));
            }
        }
        return Collections.unmodifiableList(processors);
    }

    String getId() {
        return id;
    }

    String getDescription() {
        return description;
    }

    Integer getVersion() {
        return version;
    }

    List<SearchRequestProcessor> getSearchRequestProcessors() {
        return searchRequestProcessors.stream().map(p -> p.processor).collect(Collectors.toList());
    }

    List<SearchResponseProcessor> getSearchResponseProcessors() {
        return searchResponseProcessors.stream().map(p -> p.processor).collect(Collectors.toList());
    }

    SearchRequest transformRequest(SearchRequest request) throws SearchPipelineProcessingException {
        if (searchRequestProcessors.isEmpty() == false) {
            long pipelineStart = relativeTimeSupplier.getAsLong();
            totalRequestMetrics.before();
            pipelineRequestMetrics.before();
            try {
                try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
                    request.writeTo(bytesStreamOutput);
                    try (StreamInput in = bytesStreamOutput.bytes().streamInput()) {
                        try (StreamInput input = new NamedWriteableAwareStreamInput(in, namedWriteableRegistry)) {
                            request = new SearchRequest(input);
                        }
                    }
                }
                for (ProcessorWithMetrics<SearchRequestProcessor> processorWithMetrics : searchRequestProcessors) {
                    processorWithMetrics.metrics.before();
                    long start = relativeTimeSupplier.getAsLong();
                    try {
                        request = processorWithMetrics.processor.processRequest(request);
                    } catch (Exception e) {
                        processorWithMetrics.metrics.failed();
                        throw e;
                    } finally {
                        long took = TimeUnit.NANOSECONDS.toMillis(relativeTimeSupplier.getAsLong() - start);
                        processorWithMetrics.metrics.after(took);
                    }
                }
            } catch (Exception e) {
                totalRequestMetrics.failed();
                pipelineRequestMetrics.failed();
                throw new SearchPipelineProcessingException(e);
            } finally {
                long took = TimeUnit.NANOSECONDS.toMillis(relativeTimeSupplier.getAsLong() - pipelineStart);
                totalRequestMetrics.after(took);
                pipelineRequestMetrics.after(took);
            }
        }
        return request;
    }

    SearchResponse transformResponse(SearchRequest request, SearchResponse response) throws SearchPipelineProcessingException {
        if (searchResponseProcessors.isEmpty() == false) {
            long pipelineStart = relativeTimeSupplier.getAsLong();
            totalResponseMetrics.before();
            pipelineResponseMetrics.before();
            try {
                for (ProcessorWithMetrics<SearchResponseProcessor> processorWithMetrics : searchResponseProcessors) {
                    processorWithMetrics.metrics.before();
                    long start = relativeTimeSupplier.getAsLong();
                    try {
                        response = processorWithMetrics.processor.processResponse(request, response);
                    } catch (Exception e) {
                        processorWithMetrics.metrics.failed();
                        throw e;
                    } finally {
                        long took = TimeUnit.NANOSECONDS.toMillis(relativeTimeSupplier.getAsLong() - start);
                        processorWithMetrics.metrics.after(took);
                    }
                }
            } catch (Exception e) {
                totalResponseMetrics.failed();
                pipelineResponseMetrics.failed();
                throw new SearchPipelineProcessingException(e);
            } finally {
                long took = TimeUnit.NANOSECONDS.toMillis(relativeTimeSupplier.getAsLong() - pipelineStart);
                totalResponseMetrics.after(took);
                pipelineResponseMetrics.after(took);
            }
        }
        return response;
    }

    static final Pipeline NO_OP_PIPELINE = new Pipeline(
        SearchPipelineService.NOOP_PIPELINE_ID,
        "Pipeline that does not transform anything",
        0,
        Collections.emptyList(),
        Collections.emptyList(),
        null,
        new OperationMetrics(),
        new OperationMetrics(),
        () -> 0L
    );

    void copyMetrics(Pipeline oldPipeline) {
        pipelineRequestMetrics.add(oldPipeline.pipelineRequestMetrics);
        pipelineResponseMetrics.add(oldPipeline.pipelineResponseMetrics);
        copyProcessorMetrics(searchRequestProcessors, oldPipeline.searchRequestProcessors);
        copyProcessorMetrics(searchResponseProcessors, oldPipeline.searchResponseProcessors);
    }

    private static <T extends Processor> void copyProcessorMetrics(
        List<ProcessorWithMetrics<T>> newProcessorsWithMetrics,
        List<ProcessorWithMetrics<T>> oldProcessorsWithMetrics
    ) {
        Map<String, ProcessorWithMetrics<T>> requestProcessorsByKey = new HashMap<>();
        for (ProcessorWithMetrics<T> processorWithMetrics : newProcessorsWithMetrics) {
            requestProcessorsByKey.putIfAbsent(getProcessorKey(processorWithMetrics.processor), processorWithMetrics);
        }
        for (ProcessorWithMetrics<T> oldProcessorWithMetrics : oldProcessorsWithMetrics) {
            ProcessorWithMetrics<T> newProcessor = requestProcessorsByKey.get(getProcessorKey(oldProcessorWithMetrics.processor));
            if (newProcessor != null) {
                newProcessor.metrics.add(oldProcessorWithMetrics.metrics);
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
        for (ProcessorWithMetrics<SearchRequestProcessor> requestProcessorWithMetrics : searchRequestProcessors) {
            Processor processor = requestProcessorWithMetrics.processor;
            statsBuilder.addRequestProcessorStats(
                getId(),
                getProcessorKey(processor),
                processor.getType(),
                requestProcessorWithMetrics.metrics
            );
        }
        for (ProcessorWithMetrics<SearchResponseProcessor> responseProcessorWithMetrics : searchResponseProcessors) {
            Processor processor = responseProcessorWithMetrics.processor;
            statsBuilder.addResponseProcessorStats(
                getId(),
                getProcessorKey(processor),
                processor.getType(),
                responseProcessorWithMetrics.metrics
            );
        }
    }
}
