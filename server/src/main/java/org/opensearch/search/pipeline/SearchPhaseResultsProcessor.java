/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.internal.SearchContext;

/**
 * Creates a processor that runs between Phases of the Search.
 * @opensearch.api
 */
public interface SearchPhaseResultsProcessor extends Processor {

    /**
     * Processes the {@link SearchPhaseResults} obtained from a SearchPhase which will be returned to next
     * SearchPhase.
     * @param searchPhaseResult {@link SearchPhaseResults}
     * @param searchPhaseContext {@link SearchContext}
     * @param <Result> {@link SearchPhaseResult}
     */
    <Result extends SearchPhaseResult> void process(
        final SearchPhaseResults<Result> searchPhaseResult,
        final SearchPhaseContext searchPhaseContext
    );

    /**
     * The phase which should have run before, this processor can start executing.
     * @return {@link SearchPhaseName}
     */
    SearchPhaseName getBeforePhase();

    /**
     * The phase which should run after, this processor execution.
     * @return {@link SearchPhaseName}
     */
    SearchPhaseName getAfterPhase();

}
