/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest;

import java.util.Map;

/**
 * Abstract base class for batch system generated processors.
 *
 * @opensearch.internal
 */
public abstract class AbstractBatchingSystemProcessor extends AbstractBatchingProcessor {
    protected AbstractBatchingSystemProcessor(String tag, String description, int batchSize) {
        super(tag, description, batchSize);
    }

    @Override
    public boolean isSystemGenerated() {
        return true;
    }

    /**
     * Factory class for creating {@link AbstractBatchingProcessor} instances systematically.
     *
     * Since the processor config is generated based on the index config so the batch size info should also be defined
     * as part of it. And different processors can have their own logic to decide the batch size so let each
     * implementation of the newProcessor to handle it.
     *
     * @opensearch.internal
     */
    public abstract static class Factory implements Processor.Factory {
        final String processorType;

        protected Factory(String processorType) {
            this.processorType = processorType;
        }

        @Override
        public boolean isSystemGenerated() {
            return true;
        }

        /**
         * Creates a new processor instance. It will be invoked systematically.
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
            return newProcessor(tag, description, config);
        }

        /**
         * Returns a new processor instance. It will be invoked systematically.
         *
         * @param tag tag of the processor
         * @param description description of the processor
         * @param config configuration of the processor
         * @return a new batch processor instance
         */
        protected abstract AbstractBatchingProcessor newProcessor(String tag, String description, Map<String, Object> config);
    }
}
