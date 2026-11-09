/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.systemsearchprocessor;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.ProcessorConflictEvaluationContext;
import org.opensearch.search.pipeline.ProcessorGenerationContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.search.pipeline.SystemGeneratedProcessor;
import org.opensearch.search.profile.SearchProfileShardResults;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opensearch.example.systemsearchprocessor.ExampleSearchRequestPreProcessor.ORIGINAL_QUERY_SIZE_KEY;

/**
 * An example system generated search response processor that will be executed before the user defined processor
 */
public class ExampleSearchResponseProcessor implements SearchResponseProcessor, SystemGeneratedProcessor {
    /**
     * type of the processor
     */
    public static final String TYPE = "example-search-response-processor";
    private static final String DESCRIPTION = "This is a system generated search response processor which will be"
        + "executed before the user defined search request. It will truncate the hits in the response.";
    private static final String TRIGGER_FIELD = "trigger_field";
    private static final String CONFLICT_PROCESSOR_TYPE = "truncate_hits";
    private final String tag;
    private final boolean ignoreFailure;

    /**
     * ExampleSearchResponseProcessor constructor
     * @param tag tag of the processor
     * @param ignoreFailure should processor ignore the failure
     */
    public ExampleSearchResponseProcessor(String tag, boolean ignoreFailure) {
        this.tag = tag;
        this.ignoreFailure = ignoreFailure;
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) throws Exception {
        throw new UnsupportedOperationException(
            String.format(Locale.ROOT, " [%s] should process the search response with PipelineProcessingContext.", TYPE)
        );
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse searchResponse, PipelineProcessingContext requestContext)
        throws Exception {
        long startTimeNanos = System.nanoTime();
        Object originalQuerySizeObj = requestContext.getAttribute(ORIGINAL_QUERY_SIZE_KEY);
        int originalQuerySize = originalQuerySizeObj == null ? 1 : Integer.parseInt(originalQuerySizeObj.toString());

        // Select subset of hits
        SearchHit[] allHits = searchResponse.getHits().getHits();
        if (originalQuerySize > allHits.length) {
            return searchResponse;
        }

        List<SearchHit> selected = Arrays.asList(Arrays.copyOf(allHits, originalQuerySize));

        // Build new SearchHits
        final SearchHits newHits = new SearchHits(
            selected.toArray(new SearchHit[0]),
            searchResponse.getHits().getTotalHits(),
            searchResponse.getHits().getMaxScore(),
            searchResponse.getHits().getSortFields(),
            searchResponse.getHits().getCollapseField(),
            searchResponse.getHits().getCollapseValues()
        );

        // Build new SearchResponseSections
        final SearchResponseSections newSections = new SearchResponseSections(
            newHits,
            searchResponse.getAggregations(),
            searchResponse.getSuggest(),
            searchResponse.isTimedOut(),
            searchResponse.isTerminatedEarly(),
            new SearchProfileShardResults(searchResponse.getProfileResults()),
            searchResponse.getNumReducePhases(),
            searchResponse.getInternalResponse().getSearchExtBuilders()
        );

        // Adjust timing
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);
        long newTookMillis = searchResponse.getTook().millis() + elapsedMillis;

        // Build final SearchResponse
        return new SearchResponse(
            newSections,
            searchResponse.getScrollId(),
            searchResponse.getTotalShards(),
            searchResponse.getSuccessfulShards(),
            searchResponse.getSkippedShards(),
            newTookMillis,
            searchResponse.getPhaseTook(),
            searchResponse.getShardFailures(),
            searchResponse.getClusters(),
            searchResponse.pointInTimeId()
        );
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
        return ignoreFailure;
    }

    @Override
    public ExecutionStage getExecutionStage() {
        return ExecutionStage.PRE_USER_DEFINED;
    }

    @Override
    public void evaluateConflicts(ProcessorConflictEvaluationContext context) throws IllegalArgumentException {
        boolean hasTruncateHitsProcessor = context.getUserDefinedSearchResponseProcessors()
            .stream()
            .anyMatch(processor -> CONFLICT_PROCESSOR_TYPE.equals(processor.getType()));

        if (hasTruncateHitsProcessor) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "The [%s] processor cannot be used in a search pipeline because it conflicts with the [%s] processor, "
                        + "which is automatically generated when executing a match query against [%s].",
                    CONFLICT_PROCESSOR_TYPE,
                    TYPE,
                    TRIGGER_FIELD
                )
            );
        }
    }

    static class Factory implements SystemGeneratedProcessor.SystemGeneratedFactory<SearchResponseProcessor> {
        public static final String TYPE = "example-search-response-processor-factory";

        // auto generate the processor if we do match query against the trigger_field
        @Override
        public boolean shouldGenerate(ProcessorGenerationContext context) {
            SearchRequest searchRequest = context.searchRequest();
            if (searchRequest == null || searchRequest.source() == null || searchRequest.source().query() == null) {
                return false;
            }
            QueryBuilder queryBuilder = searchRequest.source().query();
            if (queryBuilder instanceof MatchQueryBuilder matchQueryBuilder) {
                String fieldName = matchQueryBuilder.fieldName();
                return TRIGGER_FIELD.equals(fieldName);
            }
            return false;
        }

        @Override
        public SearchResponseProcessor create(
            Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws Exception {
            return new ExampleSearchResponseProcessor(tag, ignoreFailure);
        }
    }
}
