/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.systemsearchprocessor;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.ProcessorGenerationContext;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.pipeline.SystemGeneratedProcessor;

import java.util.Map;

/**
 * An example system generated search request processor that will be executed before the user defined processor
 */
public class ExampleSearchRequestPostProcessor implements SearchRequestProcessor, SystemGeneratedProcessor {
    /**
     * type of the processor
     */
    public static final String TYPE = "example-search-request-post-processor";
    /**
     * description of the processor
     */
    public static final String DESCRIPTION = "This is a system generated search request processor which will be"
        + "executed after the user defined search request. It will increase the query size by 2.";
    private final String tag;
    private final boolean ignoreFailure;

    /**
     * ExampleSearchRequestPostProcessor constructor
     * @param tag tag of the processor
     * @param ignoreFailure should processor ignore the failure
     */
    public ExampleSearchRequestPostProcessor(String tag, boolean ignoreFailure) {
        this.tag = tag;
        this.ignoreFailure = ignoreFailure;
    }

    @Override
    public SearchRequest processRequest(SearchRequest request) {
        if (request == null || request.source() == null) {
            return request;
        }
        int size = request.source().size();
        request.source().size(size + 2);
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
    public ExecutionStage getExecutionStage() {
        // This processor will be executed after the user defined search request processor
        return ExecutionStage.POST_USER_DEFINED;
    }

    static class Factory implements SystemGeneratedFactory<SearchRequestProcessor> {
        public static final String TYPE = "example-search-request-post-processor-factory";

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
            return new ExampleSearchRequestPostProcessor(tag, ignoreFailure);
        }
    }
}
