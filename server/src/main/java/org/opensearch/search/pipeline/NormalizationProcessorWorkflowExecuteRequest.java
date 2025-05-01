/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.query.QuerySearchResult;

import java.util.List;
import java.util.Optional;

/**
 * NormalizationProcessor Workflow class
 */
public class NormalizationProcessorWorkflowExecuteRequest {
    final List<QuerySearchResult> querySearchResults;
    final Optional<FetchSearchResult> fetchSearchResultOptional;
    final ScoreNormalizationTechnique normalizationTechnique;
    final ScoreCombinationTechnique combinationTechnique;
    boolean explain;
    final PipelineProcessingContext pipelineProcessingContext;
    final SearchPhaseContext searchPhaseContext;

    public NormalizationProcessorWorkflowExecuteRequest(
        List<QuerySearchResult> querySearchResults,
        Optional<FetchSearchResult> fetchSearchResultOptional,
        ScoreNormalizationTechnique normalizationTechnique,
        ScoreCombinationTechnique combinationTechnique,
        boolean explain,
        PipelineProcessingContext pipelineProcessingContext,
        SearchPhaseContext searchPhaseContext
    ) {
        this.querySearchResults = querySearchResults;
        this.fetchSearchResultOptional = fetchSearchResultOptional;
        this.normalizationTechnique = normalizationTechnique;
        this.combinationTechnique = combinationTechnique;
        this.explain = explain;
        this.pipelineProcessingContext = pipelineProcessingContext;
        this.searchPhaseContext = searchPhaseContext;
    }

    public List<QuerySearchResult> getQuerySearchResults() {
        return querySearchResults;
    }

    public Optional<FetchSearchResult> getFetchSearchResultOptional() {
        return fetchSearchResultOptional;
    }

    public ScoreNormalizationTechnique getNormalizationTechnique() {
        return normalizationTechnique;
    }

    public ScoreCombinationTechnique getCombinationTechnique() {
        return combinationTechnique;
    }

    public boolean isExplain() {
        return explain;
    }

    public PipelineProcessingContext getPipelineProcessingContext() {
        return pipelineProcessingContext;
    }

    public SearchPhaseContext getSearchPhaseContext() {
        return searchPhaseContext;
    }

    /**
     * Builder class
     * @return NormalizationProcessorWorkflowExecuteRequestBuilder
     */
    public static NormalizationProcessorWorkflowExecuteRequestBuilder builder() {
        return new NormalizationProcessorWorkflowExecuteRequestBuilder();
    }

    /**
     * NormalizationProcessorWorkflowExecuteRequestBuilder
     */
    public static class NormalizationProcessorWorkflowExecuteRequestBuilder {
        private List<QuerySearchResult> querySearchResults;
        private Optional<FetchSearchResult> fetchSearchResultOptional;
        private ScoreNormalizationTechnique normalizationTechnique;
        private ScoreCombinationTechnique combinationTechnique;
        private boolean explain;
        private PipelineProcessingContext pipelineProcessingContext;
        private SearchPhaseContext searchPhaseContext;

        public NormalizationProcessorWorkflowExecuteRequestBuilder querySearchResults(List<QuerySearchResult> querySearchResults) {
            this.querySearchResults = querySearchResults;
            return this;
        }

        public NormalizationProcessorWorkflowExecuteRequestBuilder fetchSearchResultOptional(
            Optional<FetchSearchResult> fetchSearchResultOptional
        ) {
            this.fetchSearchResultOptional = fetchSearchResultOptional;
            return this;
        }

        public NormalizationProcessorWorkflowExecuteRequestBuilder normalizationTechnique(
            ScoreNormalizationTechnique normalizationTechnique
        ) {
            this.normalizationTechnique = normalizationTechnique;
            return this;
        }

        public NormalizationProcessorWorkflowExecuteRequestBuilder combinationTechnique(ScoreCombinationTechnique combinationTechnique) {
            this.combinationTechnique = combinationTechnique;
            return this;
        }

        public NormalizationProcessorWorkflowExecuteRequestBuilder explain(boolean explain) {
            this.explain = explain;
            return this;
        }

        public NormalizationProcessorWorkflowExecuteRequestBuilder pipelineProcessingContext(
            PipelineProcessingContext pipelineProcessingContext
        ) {
            this.pipelineProcessingContext = pipelineProcessingContext;
            return this;
        }

        public NormalizationProcessorWorkflowExecuteRequestBuilder searchPhaseContext(SearchPhaseContext searchPhaseContext) {
            this.searchPhaseContext = searchPhaseContext;
            return this;
        }

        public NormalizationProcessorWorkflowExecuteRequest build() {
            return new NormalizationProcessorWorkflowExecuteRequest(
                querySearchResults,
                fetchSearchResultOptional,
                normalizationTechnique,
                combinationTechnique,
                explain,
                pipelineProcessingContext,
                searchPhaseContext
            );
        }
    }
}
