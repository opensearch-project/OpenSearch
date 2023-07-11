/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import java.util.Map;

/**
 * A processor implementation may modify the request or response from a search call.
 * Whether changes are made and what exactly is modified is up to the implementation.
 * <p>
 * Processors may get called concurrently and thus need to be thread-safe.
 *
 * TODO: Refactor {@link org.opensearch.ingest.Processor} to extend this interface, and specialize to IngestProcessor.
 *
 * @opensearch.internal
 */
public interface Processor {
    /**
     * Processor configuration key to let the factory know the context for pipeline creation.
     * <p>
     * See {@link PipelineSource}.
     */
    String PIPELINE_SOURCE = "pipeline_source";

    /**
     * Gets the type of processor
     */
    String getType();

    /**
     * Gets the tag of a processor.
     */
    String getTag();

    /**
     * Gets the description of a processor.
     */
    String getDescription();

    /**
     * Gets the setting of ignoreFailure of a processor.
     */
    boolean isIgnoreFailure();

    /**
     * A factory that knows how to construct a processor based on a map of maps.
     */
    interface Factory<T extends Processor> {

        /**
         * Creates a processor based on the specified map of maps config.
         *
         * @param processorFactories Other processors which may be created inside this processor
         * @param tag                The tag for the processor
         * @param description        A short description of what this processor does
         * @param config             The configuration for the processor
         *                           <b>Note:</b> Implementations are responsible for removing the used configuration
         *                           keys, so that after creation the config map should be empty.
         * @param pipelineContext    Contextual information about the enclosing pipeline.
         */
        T create(
            Map<String, Factory<T>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws Exception;
    }

    /**
     * Contextual information about the enclosing pipeline. A processor factory may change processor initialization behavior or
     * pass this information to the created processor instance.
     */
    class PipelineContext {
        private final PipelineSource pipelineSource;

        public PipelineContext(PipelineSource pipelineSource) {
            this.pipelineSource = pipelineSource;
        }

        public PipelineSource getPipelineSource() {
            return pipelineSource;
        }
    }

    /**
     * A processor factory may change the processor initialization behavior based on the creation context (e.g. avoiding
     * creating expensive resources during validation or in a request-scoped pipeline.)
     */
    enum PipelineSource {
        // A named pipeline is being created or updated
        UPDATE_PIPELINE,
        // Pipeline is defined within a search request
        SEARCH_REQUEST,
        // A named pipeline is being validated before being written to cluster state
        VALIDATE_PIPELINE
    }
}
