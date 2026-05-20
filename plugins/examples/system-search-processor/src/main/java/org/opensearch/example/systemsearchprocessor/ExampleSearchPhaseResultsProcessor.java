/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.systemsearchprocessor;

import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.ProcessorGenerationContext;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.search.pipeline.SystemGeneratedProcessor;

import java.util.Map;

/**
 * An example system generated search phase results processor that will be executed after the user defined processor
 */
public class ExampleSearchPhaseResultsProcessor implements SystemGeneratedProcessor, SearchPhaseResultsProcessor {
    private static final String TYPE = "example-search-phase-results-processor";
    private static final String DESCRIPTION = "This is a system generated search phase results processor which will be"
        + "executed after the user defined search request. It will set the max score as 10.";
    private final String tag;
    private final boolean ignoreFailure;

    /**
     * ExampleSearchPhaseResultsProcessor constructor
     * @param tag processor tag
     * @param ignoreFailure should processor ignore the failure
     */
    public ExampleSearchPhaseResultsProcessor(String tag, boolean ignoreFailure) {
        this.tag = tag;
        this.ignoreFailure = ignoreFailure;
    }

    @Override
    public <Result extends SearchPhaseResult> void process(
        SearchPhaseResults<Result> searchPhaseResult,
        SearchPhaseContext searchPhaseContext
    ) {
        searchPhaseResult.getAtomicArray().asList().forEach(searchResult -> { searchResult.queryResult().topDocs().maxScore = 10; });
    }

    @Override
    public SearchPhaseName getBeforePhase() {
        return SearchPhaseName.QUERY;
    }

    @Override
    public SearchPhaseName getAfterPhase() {
        return SearchPhaseName.FETCH;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String getTag() {
        return this.tag;
    }

    @Override
    public String getDescription() {
        return DESCRIPTION;
    }

    @Override
    public boolean isIgnoreFailure() {
        return this.ignoreFailure;
    }

    static class Factory implements SystemGeneratedProcessor.SystemGeneratedFactory<SearchPhaseResultsProcessor> {
        public static final String TYPE = "example-search-phase-results-processor-factory";

        @Override
        public boolean shouldGenerate(ProcessorGenerationContext context) {
            return true;
        }

        @Override
        public SearchPhaseResultsProcessor create(
            Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws Exception {
            return new ExampleSearchPhaseResultsProcessor(tag, ignoreFailure);
        }
    }
}
