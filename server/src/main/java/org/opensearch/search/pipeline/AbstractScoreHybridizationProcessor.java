/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.internal.SearchContext;

import java.util.Optional;

/**
 * Base class for all score hybridization processors. This class is responsible for executing the score hybridization process.
 * It is a pipeline processor that is executed after the query phase and before the fetch phase.
 */
public abstract class AbstractScoreHybridizationProcessor implements SearchPhaseResultsProcessor {
    /**
     * Method abstracts functional aspect of score normalization and score combination. Exact methods for each processing stage
     * are set as part of class constructor. This method is called when there is no pipeline context
     * @param searchPhaseResult {@link SearchPhaseResults} DTO that has query search results. Results will be mutated as part of this method execution
     * @param searchPhaseContext {@link SearchContext}
     */
    @Override
    public <Result extends SearchPhaseResult> void process(
        final SearchPhaseResults<Result> searchPhaseResult,
        final SearchPhaseContext searchPhaseContext
    ) {
        hybridizeScores(searchPhaseResult, searchPhaseContext, Optional.empty());
    }

    /**
     * Method abstracts functional aspect of score normalization and score combination. Exact methods for each processing stage
     * are set as part of class constructor. This method is called when there is pipeline context
     * @param searchPhaseResult {@link SearchPhaseResults} DTO that has query search results. Results will be mutated as part of this method execution
     * @param searchPhaseContext {@link SearchContext}
     * @param requestContext {@link PipelineProcessingContext} processing context of search pipeline
     * @param <Result>
     */
    @Override
    public <Result extends SearchPhaseResult> void process(
        final SearchPhaseResults<Result> searchPhaseResult,
        final SearchPhaseContext searchPhaseContext,
        final PipelineProcessingContext requestContext
    ) {
        hybridizeScores(searchPhaseResult, searchPhaseContext, Optional.ofNullable(requestContext));
    }

    /**
     * Method abstracts functional aspect of score normalization and score combination. Exact methods for each processing stage
     * are set as part of class constructor
     * @param searchPhaseResult
     * @param searchPhaseContext
     * @param requestContextOptional
     * @param <Result>
     */
    abstract <Result extends SearchPhaseResult> void hybridizeScores(
        SearchPhaseResults<Result> searchPhaseResult,
        SearchPhaseContext searchPhaseContext,
        Optional<PipelineProcessingContext> requestContextOptional
    );
}
