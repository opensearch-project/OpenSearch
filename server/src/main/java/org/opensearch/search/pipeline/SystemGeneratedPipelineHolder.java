/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import reactor.util.annotation.NonNull;

/**
 * A holder for the system generated pipelines
 */
public record SystemGeneratedPipelineHolder(@NonNull SystemGeneratedPipelineWithMetrics prePipeline,
    @NonNull SystemGeneratedPipelineWithMetrics postPipeline) {

    boolean isNoOp() {
        return prePipeline.isNoOp() && postPipeline.isNoOp();
    }

    /**
     * Evaluate if there is any conflict between processors
     *
     * @param userDefinedPipeline user defined search pipeline
     */
    void evaluateConflict(Pipeline userDefinedPipeline) {
        final ProcessorConflictEvaluationContext context = new ProcessorConflictEvaluationContext(userDefinedPipeline, this);

        prePipeline.evaluateConflicts(context);
        postPipeline.evaluateConflicts(context);
    }
}
