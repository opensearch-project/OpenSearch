/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.systemsearchprocessor;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.ProcessorGenerationContext;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.pipeline.SystemGeneratedProcessor;

import java.util.Locale;
import java.util.Map;

/**
 * An example system generated search request processor that will be executed before the user defined processor
 */
public class ExampleSearchRequestPreProcessor implements SearchRequestProcessor, SystemGeneratedProcessor {
    /**
     * type of the processor
     */
    public static final String TYPE = "example-search-request-pre-processor";
    /**
     * description of the processor
     */
    public static final String DESCRIPTION = "This is a system generated search request processor which will be"
        + "executed before the user defined search request. It will increase the query size by 1.";
    /**
     * original query size attribute key
     */
    public static final String ORIGINAL_QUERY_SIZE_KEY = "example-original-query-size";
    private final String tag;
    private final boolean ignoreFailure;

    /**
     * ExampleSearchRequestPreProcessor constructore
     * @param tag tag of the processor
     * @param ignoreFailure should processor ignore the failure
     */
    public ExampleSearchRequestPreProcessor(String tag, boolean ignoreFailure) {
        this.tag = tag;
        this.ignoreFailure = ignoreFailure;
    }

    @Override
    public SearchRequest processRequest(SearchRequest request) {
        throw new UnsupportedOperationException(
            String.format(Locale.ROOT, " [%s] should process the search request with PipelineProcessingContext.", TYPE)
        );
    }

    @Override
    public SearchRequest processRequest(SearchRequest request, PipelineProcessingContext requestContext) throws Exception {
        if (request == null || request.source() == null) {
            return request;
        }
        int size = request.source().size();
        // store the original query size so that later the response processor can use it
        requestContext.setAttribute(ORIGINAL_QUERY_SIZE_KEY, size);
        request.source().size(size + 1);
        return request;
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

    @Override
    public SystemGeneratedProcessor.ExecutionStage getExecutionStage() {
        // This processor will be executed before the user defined search request processor
        return ExecutionStage.PRE_USER_DEFINED;
    }

    static class Factory implements SystemGeneratedProcessor.SystemGeneratedFactory<SearchRequestProcessor> {
        public static final String TYPE = "example-search-request-pre-processor-factory";

        // We auto generate the processor if the original query size is less than 5.
        @Override
        public boolean shouldGenerate(ProcessorGenerationContext context) {
            SearchRequest searchRequest = context.searchRequest();
            if (searchRequest == null || searchRequest.source() == null) {
                return false;
            }
            int size = searchRequest.source().size();
            return size < 5;
        }

        @Override
        public SearchRequestProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws Exception {
            return new ExampleSearchRequestPreProcessor(tag, ignoreFailure);
        }
    }
}
