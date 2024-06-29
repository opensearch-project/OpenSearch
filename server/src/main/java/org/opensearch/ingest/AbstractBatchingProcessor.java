/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.opensearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * Abstract base class for batch processors.
 *
 * @opensearch.internal
 */
public abstract class AbstractBatchingProcessor extends AbstractProcessor {

    public static final String BATCH_SIZE_FIELD = "batch_size";
    private static final int DEFAULT_BATCH_SIZE = 1;
    protected final int batchSize;

    protected AbstractBatchingProcessor(String tag, String description, int batchSize) {
        super(tag, description);
        this.batchSize = batchSize;
    }

    /**
     * Internal logic to process batched documents, must be implemented by concrete batch processors.
     *
     * @param ingestDocumentWrappers {@link List} of {@link IngestDocumentWrapper} to be processed.
     * @param handler                {@link Consumer} to be called with the results of the processing.
     */
    protected abstract void subBatchExecute(
        List<IngestDocumentWrapper> ingestDocumentWrappers,
        Consumer<List<IngestDocumentWrapper>> handler
    );

    @Override
    public void batchExecute(List<IngestDocumentWrapper> ingestDocumentWrappers, Consumer<List<IngestDocumentWrapper>> handler) {
        if (ingestDocumentWrappers.isEmpty()) {
            handler.accept(Collections.emptyList());
            return;
        }

        // if batch size is larger than document size, send one batch
        if (this.batchSize >= ingestDocumentWrappers.size()) {
            subBatchExecute(ingestDocumentWrappers, handler);
            return;
        }

        // split documents into multiple batches and send each batch to batch processors
        List<List<IngestDocumentWrapper>> batches = cutBatches(ingestDocumentWrappers);
        int size = ingestDocumentWrappers.size();
        AtomicInteger counter = new AtomicInteger(size);
        List<IngestDocumentWrapper> allResults = Collections.synchronizedList(new ArrayList<>());
        for (List<IngestDocumentWrapper> batch : batches) {
            this.subBatchExecute(batch, batchResults -> {
                allResults.addAll(batchResults);
                if (counter.addAndGet(-batchResults.size()) == 0) {
                    handler.accept(allResults);
                }
                assert counter.get() >= 0 : "counter is negative";
            });
        }
    }

    private List<List<IngestDocumentWrapper>> cutBatches(List<IngestDocumentWrapper> ingestDocumentWrappers) {
        List<List<IngestDocumentWrapper>> batches = new ArrayList<>();
        for (int i = 0; i < ingestDocumentWrappers.size(); i += this.batchSize) {
            batches.add(ingestDocumentWrappers.subList(i, Math.min(i + this.batchSize, ingestDocumentWrappers.size())));
        }
        return batches;
    }

    /**
     * Factory class for creating {@link AbstractBatchingProcessor} instances.
     *
     * @opensearch.internal
     */
    public abstract static class Factory implements Processor.Factory {
        final String processorType;

        protected Factory(String processorType) {
            this.processorType = processorType;
        }

        /**
         * Creates a new processor instance.
         *
         * @param processorFactories The processor factories.
         * @param tag                 The processor tag.
         * @param description         The processor description.
         * @param config              The processor configuration.
         * @return The new AbstractBatchProcessor instance.
         * @throws Exception If the processor could not be created.
         */
        @Override
        public AbstractBatchingProcessor create(
            Map<String, Processor.Factory> processorFactories,
            String tag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            int batchSize = ConfigurationUtils.readIntProperty(this.processorType, tag, config, BATCH_SIZE_FIELD, DEFAULT_BATCH_SIZE);
            if (batchSize < 1) {
                throw newConfigurationException(this.processorType, tag, BATCH_SIZE_FIELD, "batch size must be a positive integer");
            }
            return newProcessor(tag, description, batchSize, config);
        }

        /**
         * Returns a new processor instance.
         *
         * @param tag tag of the processor
         * @param description description of the processor
         * @param batchSize batch size of the processor
         * @param config configuration of the processor
         * @return a new batch processor instance
         */
        protected abstract AbstractBatchingProcessor newProcessor(
            String tag,
            String description,
            int batchSize,
            Map<String, Object> config
        );
    }
}
