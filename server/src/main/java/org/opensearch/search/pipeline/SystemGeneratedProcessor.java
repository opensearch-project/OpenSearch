/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

/**
 * Interface for a system generated search pipeline processor.
 */
public interface SystemGeneratedProcessor extends Processor {
    /**
     * @return when to execute the processor. Default to be executed post user defined processors.
     */
    default ExecutionStage getExecutionStage() {
        return ExecutionStage.POST_USER_DEFINED;
    }

    /**
     * Evaluate if this processor conflict with other processors
     * @param processorConflictEvaluationContext context to evaluate the conflicts
     * @throws IllegalArgumentException If there are conflicts.
     */
    default void evaluateConflicts(ProcessorConflictEvaluationContext processorConflictEvaluationContext) throws IllegalArgumentException {}

    /**
     * Factory to systematically generate the search processor.
     * @param <T>
     */
    interface SystemGeneratedFactory<T extends Processor> extends Factory<T> {
        /**
         * @param context context used to evaluate if we should use the factory to generate the system processor.
         * @return if we should use the factory to generate the system processor.
         */
        boolean shouldGenerate(ProcessorGenerationContext context);
    }

    /**
     * The stage to execute the processor
     */
    enum ExecutionStage {
        /**
         * Before user defined processors
         */
        PRE_USER_DEFINED,
        /**
         * After user defined processors
         */
        POST_USER_DEFINED,
    }
}
