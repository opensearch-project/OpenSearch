/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;

public class TrackingSearchRequestProcessorWrapperTests extends OpenSearchTestCase {
    private SearchRequestProcessor mockProcessor;
    private TrackingSearchRequestProcessorWrapper wrapper;
    private PipelineProcessingContext context;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mockProcessor = Mockito.mock(SearchRequestProcessor.class);
        wrapper = new TrackingSearchRequestProcessorWrapper(mockProcessor);
        context = new PipelineProcessingContext();
    }

    public void testProcessRequestAsyncSuccess() {
        SearchRequest inputRequest = new SearchRequest();
        inputRequest.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));

        SearchRequest outputRequest = new SearchRequest();
        outputRequest.source(new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")));

        doAnswer(invocation -> {
            ActionListener<SearchRequest> listener = invocation.getArgument(2);
            listener.onResponse(outputRequest);
            return null;
        }).when(mockProcessor).processRequestAsync(any(SearchRequest.class), eq(context), any());

        ActionListener<SearchRequest> listener = ActionListener.wrap(response -> {
            assertEquals(outputRequest, response);
            ProcessorExecutionDetail detail = context.getProcessorExecutionDetails().get(0);
            assertEquals(wrapper.getType(), detail.getProcessorName());
            assertEquals(ProcessorExecutionDetail.ProcessorStatus.SUCCESS, detail.getStatus());
        }, e -> fail("Unexpected exception: " + e.getMessage()));

        wrapper.processRequestAsync(inputRequest, context, listener);
    }

}
