/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import java.util.List;

/**
 * A DTO to hold the context used for a system generated factory to evaluate should we generate the processor
 */
public class ProcessorConflictEvaluationContext {
    private final List<SearchRequestProcessor> userDefinedSearchRequestProcessors;
    private final List<SearchResponseProcessor> userDefinedSearchResponseProcessors;
    private final List<SearchPhaseResultsProcessor> userDefinedSearchPhaseResultsProcessors;

    private final List<SearchRequestProcessor> systemGeneratedPreSearchRequestProcessors;
    private final List<SearchResponseProcessor> systemGeneratedPreSearchResponseProcessors;
    private final List<SearchPhaseResultsProcessor> systemGeneratedPreSearchPhaseResultsProcessors;
    private final List<SearchRequestProcessor> systemGeneratedPostSearchRequestProcessors;
    private final List<SearchResponseProcessor> systemGeneratedPostSearchResponseProcessors;
    private final List<SearchPhaseResultsProcessor> systemGeneratedPostSearchPhaseResultsProcessors;

    ProcessorConflictEvaluationContext(Pipeline userDefinedPipeline, SystemGeneratedPipelineHolder systemGeneratedPipelineHolder) {
        userDefinedSearchRequestProcessors = userDefinedPipeline.getSearchRequestProcessors();
        userDefinedSearchResponseProcessors = userDefinedPipeline.getSearchResponseProcessors();
        userDefinedSearchPhaseResultsProcessors = userDefinedPipeline.getSearchPhaseResultsProcessors();
        systemGeneratedPreSearchRequestProcessors = systemGeneratedPipelineHolder.prePipeline().getSearchRequestProcessors();
        systemGeneratedPreSearchResponseProcessors = systemGeneratedPipelineHolder.prePipeline().getSearchResponseProcessors();
        systemGeneratedPreSearchPhaseResultsProcessors = systemGeneratedPipelineHolder.prePipeline().getSearchPhaseResultsProcessors();
        systemGeneratedPostSearchRequestProcessors = systemGeneratedPipelineHolder.postPipeline().getSearchRequestProcessors();
        systemGeneratedPostSearchResponseProcessors = systemGeneratedPipelineHolder.postPipeline().getSearchResponseProcessors();
        systemGeneratedPostSearchPhaseResultsProcessors = systemGeneratedPipelineHolder.postPipeline().getSearchPhaseResultsProcessors();
    }

    public List<SearchRequestProcessor> getUserDefinedSearchRequestProcessors() {
        return userDefinedSearchRequestProcessors;
    }

    public List<SearchResponseProcessor> getUserDefinedSearchResponseProcessors() {
        return userDefinedSearchResponseProcessors;
    }

    public List<SearchPhaseResultsProcessor> getUserDefinedSearchPhaseResultsProcessors() {
        return userDefinedSearchPhaseResultsProcessors;
    }

    public List<SearchRequestProcessor> getSystemGeneratedPreSearchRequestProcessors() {
        return systemGeneratedPreSearchRequestProcessors;
    }

    public List<SearchResponseProcessor> getSystemGeneratedPreSearchResponseProcessors() {
        return systemGeneratedPreSearchResponseProcessors;
    }

    public List<SearchPhaseResultsProcessor> getSystemGeneratedPreSearchPhaseResultsProcessors() {
        return systemGeneratedPreSearchPhaseResultsProcessors;
    }

    public List<SearchRequestProcessor> getSystemGeneratedPostSearchRequestProcessors() {
        return systemGeneratedPostSearchRequestProcessors;
    }

    public List<SearchResponseProcessor> getSystemGeneratedPostSearchResponseProcessors() {
        return systemGeneratedPostSearchResponseProcessors;
    }

    public List<SearchPhaseResultsProcessor> getSystemGeneratedPostSearchPhaseResultsProcessors() {
        return systemGeneratedPostSearchPhaseResultsProcessors;
    }
}
