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

import org.opensearch.OpenSearchException;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.metrics.OperationMetrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * A Processor that executes a list of other "processors". It executes a separate list of
 * "onFailureProcessors" when any of the processors throw an {@link Exception}.
 *
 * @opensearch.internal
 */
public class CompoundProcessor implements Processor {
    public static final String ON_FAILURE_MESSAGE_FIELD = "on_failure_message";
    public static final String ON_FAILURE_PROCESSOR_TYPE_FIELD = "on_failure_processor_type";
    public static final String ON_FAILURE_PROCESSOR_TAG_FIELD = "on_failure_processor_tag";
    public static final String ON_FAILURE_PIPELINE_FIELD = "on_failure_pipeline";

    private final boolean ignoreFailure;
    private final List<Processor> processors;
    private final List<Processor> onFailureProcessors;
    private final List<Tuple<Processor, OperationMetrics>> processorsWithMetrics;
    private final LongSupplier relativeTimeProvider;

    CompoundProcessor(LongSupplier relativeTimeProvider, Processor... processor) {
        this(false, Arrays.asList(processor), Collections.emptyList(), relativeTimeProvider);
    }

    public CompoundProcessor(Processor... processor) {
        this(false, Arrays.asList(processor), Collections.emptyList());
    }

    public CompoundProcessor(boolean ignoreFailure, List<Processor> processors, List<Processor> onFailureProcessors) {
        this(ignoreFailure, processors, onFailureProcessors, System::nanoTime);
    }

    CompoundProcessor(
        boolean ignoreFailure,
        List<Processor> processors,
        List<Processor> onFailureProcessors,
        LongSupplier relativeTimeProvider
    ) {
        super();
        this.ignoreFailure = ignoreFailure;
        this.processors = processors;
        this.onFailureProcessors = onFailureProcessors;
        this.relativeTimeProvider = relativeTimeProvider;
        this.processorsWithMetrics = new ArrayList<>(processors.size());
        processors.forEach(p -> processorsWithMetrics.add(new Tuple<>(p, new OperationMetrics())));
    }

    List<Tuple<Processor, OperationMetrics>> getProcessorsWithMetrics() {
        return processorsWithMetrics;
    }

    public boolean isIgnoreFailure() {
        return ignoreFailure;
    }

    public List<Processor> getOnFailureProcessors() {
        return onFailureProcessors;
    }

    public List<Processor> getProcessors() {
        return processors;
    }

    public List<Processor> flattenProcessors() {
        List<Processor> allProcessors = new ArrayList<>(flattenProcessors(processors));
        allProcessors.addAll(flattenProcessors(onFailureProcessors));
        return allProcessors;
    }

    private static List<Processor> flattenProcessors(List<Processor> processors) {
        List<Processor> flattened = new ArrayList<>();
        for (Processor processor : processors) {
            if (processor instanceof CompoundProcessor) {
                flattened.addAll(((CompoundProcessor) processor).flattenProcessors());
            } else {
                flattened.add(processor);
            }
        }
        return flattened;
    }

    @Override
    public String getType() {
        return "compound";
    }

    @Override
    public String getTag() {
        return "CompoundProcessor-" + flattenProcessors().stream().map(Processor::getTag).collect(Collectors.joining("-"));
    }

    @Override
    public String getDescription() {
        return null;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        throw new UnsupportedOperationException("this method should not get executed");
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        innerExecute(0, ingestDocument, handler);
    }

    @Override
    public void batchExecute(List<IngestDocumentWrapper> ingestDocumentWrappers, Consumer<List<IngestDocumentWrapper>> handler) {
        innerBatchExecute(0, ingestDocumentWrappers, handler);
    }

    /**
     * Internal logic to process documents with current processor.
     *
     * @param currentProcessor index of processor to process batched documents
     * @param ingestDocumentWrappers batched documents to be processed
     * @param handler callback function
     */
    void innerBatchExecute(
        int currentProcessor,
        List<IngestDocumentWrapper> ingestDocumentWrappers,
        Consumer<List<IngestDocumentWrapper>> handler
    ) {
        if (currentProcessor == processorsWithMetrics.size()) {
            handler.accept(ingestDocumentWrappers);
            return;
        }
        Tuple<Processor, OperationMetrics> processorWithMetric = processorsWithMetrics.get(currentProcessor);
        final Processor processor = processorWithMetric.v1();
        final OperationMetrics metric = processorWithMetric.v2();
        final long startTimeInNanos = relativeTimeProvider.getAsLong();
        int size = ingestDocumentWrappers.size();
        metric.beforeN(size);
        // Use synchronization to ensure batches are processed by processors in sequential order
        AtomicInteger counter = new AtomicInteger(size);
        List<IngestDocumentWrapper> allResults = Collections.synchronizedList(new ArrayList<>());
        Map<Integer, IngestDocumentWrapper> slotToWrapperMap = createSlotIngestDocumentWrapperMap(ingestDocumentWrappers);
        processor.batchExecute(ingestDocumentWrappers, results -> {
            if (results.isEmpty()) return;
            allResults.addAll(results);
            // counter equals to 0 means all documents are processed and called back.
            if (counter.addAndGet(-results.size()) == 0) {
                long ingestTimeInMillis = TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - startTimeInNanos);
                metric.afterN(allResults.size(), ingestTimeInMillis);

                List<IngestDocumentWrapper> documentsDropped = new ArrayList<>();
                List<IngestDocumentWrapper> documentsWithException = new ArrayList<>();
                List<IngestDocumentWrapper> documentsToContinue = new ArrayList<>();
                int totalFailed = 0;
                // iterate all results to categorize them to: to continue, to drop, with exception
                for (IngestDocumentWrapper resultDocumentWrapper : allResults) {
                    IngestDocumentWrapper originalDocumentWrapper = slotToWrapperMap.get(resultDocumentWrapper.getSlot());
                    if (resultDocumentWrapper.getException() != null) {
                        ++totalFailed;
                        if (ignoreFailure) {
                            documentsToContinue.add(originalDocumentWrapper);
                        } else {
                            IngestProcessorException compoundProcessorException = newCompoundProcessorException(
                                resultDocumentWrapper.getException(),
                                processor,
                                originalDocumentWrapper.getIngestDocument()
                            );
                            documentsWithException.add(
                                new IngestDocumentWrapper(
                                    resultDocumentWrapper.getSlot(),
                                    originalDocumentWrapper.getIngestDocument(),
                                    compoundProcessorException
                                )
                            );
                        }
                    } else {
                        if (resultDocumentWrapper.getIngestDocument() == null) {
                            documentsDropped.add(resultDocumentWrapper);
                        } else {
                            documentsToContinue.add(resultDocumentWrapper);
                        }
                    }
                }
                if (totalFailed > 0) {
                    metric.failedN(totalFailed);
                }
                if (!documentsDropped.isEmpty()) {
                    handler.accept(documentsDropped);
                }
                if (!documentsToContinue.isEmpty()) {
                    innerBatchExecute(currentProcessor + 1, documentsToContinue, handler);
                }
                if (!documentsWithException.isEmpty()) {
                    if (onFailureProcessors.isEmpty()) {
                        handler.accept(documentsWithException);
                    } else {
                        documentsWithException.forEach(
                            doc -> executeOnFailureAsync(
                                0,
                                doc.getIngestDocument(),
                                (IngestProcessorException) doc.getException(),
                                (result, ex) -> {
                                    handler.accept(Collections.singletonList(new IngestDocumentWrapper(doc.getSlot(), result, ex)));
                                }
                            )
                        );
                    }
                }
            }
            assert counter.get() >= 0;
        });
    }

    void innerExecute(int currentProcessor, IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        if (currentProcessor == processorsWithMetrics.size()) {
            handler.accept(ingestDocument, null);
            return;
        }

        Tuple<Processor, OperationMetrics> processorWithMetric = processorsWithMetrics.get(currentProcessor);
        final Processor processor = processorWithMetric.v1();
        final OperationMetrics metric = processorWithMetric.v2();
        final long startTimeInNanos = relativeTimeProvider.getAsLong();
        metric.before();
        processor.execute(ingestDocument, (result, e) -> {
            long ingestTimeInMillis = TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - startTimeInNanos);
            metric.after(ingestTimeInMillis);

            if (e != null) {
                metric.failed();
                if (ignoreFailure) {
                    innerExecute(currentProcessor + 1, ingestDocument, handler);
                } else {
                    IngestProcessorException compoundProcessorException = newCompoundProcessorException(e, processor, ingestDocument);
                    if (onFailureProcessors.isEmpty()) {
                        handler.accept(null, compoundProcessorException);
                    } else {
                        executeOnFailureAsync(0, ingestDocument, compoundProcessorException, handler);
                    }
                }
            } else {
                if (result != null) {
                    innerExecute(currentProcessor + 1, result, handler);
                } else {
                    handler.accept(null, null);
                }
            }
        });
    }

    void executeOnFailureAsync(
        int currentOnFailureProcessor,
        IngestDocument ingestDocument,
        OpenSearchException exception,
        BiConsumer<IngestDocument, Exception> handler
    ) {
        if (currentOnFailureProcessor == 0) {
            putFailureMetadata(ingestDocument, exception);
        }

        if (currentOnFailureProcessor == onFailureProcessors.size()) {
            removeFailureMetadata(ingestDocument);
            handler.accept(ingestDocument, null);
            return;
        }

        final Processor onFailureProcessor = onFailureProcessors.get(currentOnFailureProcessor);
        onFailureProcessor.execute(ingestDocument, (result, e) -> {
            if (e != null) {
                removeFailureMetadata(ingestDocument);
                handler.accept(null, newCompoundProcessorException(e, onFailureProcessor, ingestDocument));
                return;
            }
            if (result == null) {
                removeFailureMetadata(ingestDocument);
                handler.accept(null, null);
                return;
            }
            executeOnFailureAsync(currentOnFailureProcessor + 1, ingestDocument, exception, handler);
        });
    }

    private void putFailureMetadata(IngestDocument ingestDocument, OpenSearchException cause) {
        List<String> processorTypeHeader = cause.getHeader("processor_type");
        List<String> processorTagHeader = cause.getHeader("processor_tag");
        List<String> processorOriginHeader = cause.getHeader("pipeline_origin");
        String failedProcessorType = (processorTypeHeader != null) ? processorTypeHeader.get(0) : null;
        String failedProcessorTag = (processorTagHeader != null) ? processorTagHeader.get(0) : null;
        String failedPipelineId = (processorOriginHeader != null) ? processorOriginHeader.get(0) : null;
        Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
        ingestMetadata.put(ON_FAILURE_MESSAGE_FIELD, cause.getRootCause().getMessage());
        ingestMetadata.put(ON_FAILURE_PROCESSOR_TYPE_FIELD, failedProcessorType);
        ingestMetadata.put(ON_FAILURE_PROCESSOR_TAG_FIELD, failedProcessorTag);
        if (failedPipelineId != null) {
            ingestMetadata.put(ON_FAILURE_PIPELINE_FIELD, failedPipelineId);
        }
    }

    private void removeFailureMetadata(IngestDocument ingestDocument) {
        Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
        ingestMetadata.remove(ON_FAILURE_MESSAGE_FIELD);
        ingestMetadata.remove(ON_FAILURE_PROCESSOR_TYPE_FIELD);
        ingestMetadata.remove(ON_FAILURE_PROCESSOR_TAG_FIELD);
        ingestMetadata.remove(ON_FAILURE_PIPELINE_FIELD);
    }

    static IngestProcessorException newCompoundProcessorException(Exception e, Processor processor, IngestDocument document) {
        if (e instanceof IngestProcessorException && ((IngestProcessorException) e).getHeader("processor_type") != null) {
            return (IngestProcessorException) e;
        }

        IngestProcessorException exception = new IngestProcessorException(e);

        String processorType = processor.getType();
        if (processorType != null) {
            exception.addHeader("processor_type", processorType);
        }
        String processorTag = processor.getTag();
        if (processorTag != null) {
            exception.addHeader("processor_tag", processorTag);
        }
        List<String> pipelineStack = document.getPipelineStack();
        if (pipelineStack.size() > 1) {
            exception.addHeader("pipeline_origin", pipelineStack);
        }

        return exception;
    }

    private Map<Integer, IngestDocumentWrapper> createSlotIngestDocumentWrapperMap(List<IngestDocumentWrapper> ingestDocumentWrappers) {
        Map<Integer, IngestDocumentWrapper> slotIngestDocumentWrapperMap = new HashMap<>();
        for (IngestDocumentWrapper ingestDocumentWrapper : ingestDocumentWrappers) {
            slotIngestDocumentWrapperMap.put(ingestDocumentWrapper.getSlot(), ingestDocumentWrapper);
        }
        return slotIngestDocumentWrapperMap;
    }

}
