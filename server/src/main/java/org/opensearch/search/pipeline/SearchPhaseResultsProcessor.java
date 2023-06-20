/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.action.search.SearchPhase;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.search.SearchPhaseResult;

/**
 * Creates a processor that runs between Phases of the Search.
 */
public interface SearchPhaseResultsProcessor extends Processor {
    <Result extends SearchPhaseResult> SearchPhaseResults<Result> process(
        final SearchPhaseResults<Result> searchPhaseResult,
        final SearchPhaseContext searchPhaseContext
    );

    /**
     * The phase which should have run before, this processor can start executing.
     * @return {@link SearchPhase.SearchPhaseName}
     */
    SearchPhase.SearchPhaseName getBeforePhase();

    /**
     * The phase which should run after, this processor execution.
     * @return {@link SearchPhase.SearchPhaseName}
     */
    SearchPhase.SearchPhaseName getAfterPhase();

}
