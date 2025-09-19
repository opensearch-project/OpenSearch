/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.opensearch.action.IndicesRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.plugins.SearchPipelinePlugin;
import org.opensearch.search.pipeline.PipelinedRequest;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryCoordinatorContextTests extends OpenSearchTestCase {

    private IndexNameExpressionResolver indexNameExpressionResolver;

    @Before
    public void setup() {
        indexNameExpressionResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
    }

    public void testGetContextVariables_whenPipelinedSearchRequest_thenReturnVariables() throws Exception {
        final PipelinedRequest searchRequest = createDummyPipelinedRequest();
        searchRequest.getPipelineProcessingContext().setAttribute("key", "value");

        final QueryCoordinatorContext queryCoordinatorContext = new QueryCoordinatorContext(mock(QueryRewriteContext.class), searchRequest);

        assertEquals(Map.of("key", "value"), queryCoordinatorContext.getContextVariables());
    }

    private PipelinedRequest createDummyPipelinedRequest() throws Exception {
        final Client client = mock(Client.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        final ExecutorService executorService = OpenSearchExecutors.newDirectExecutorService();
        when(threadPool.generic()).thenReturn(executorService);
        when(threadPool.executor(anyString())).thenReturn(executorService);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        final SearchPipelineService searchPipelineService = new SearchPipelineService(
            clusterService,
            threadPool,
            null,
            null,
            null,
            null,
            this.writableRegistry(),
            Collections.singletonList(new SearchPipelinePlugin() {
                @Override
                public Map<String, Processor.Factory<SearchRequestProcessor>> getRequestProcessors(Parameters parameters) {
                    return Collections.emptyMap();
                }

                @Override
                public Map<String, Processor.Factory<SearchResponseProcessor>> getResponseProcessors(Parameters parameters) {
                    return Collections.emptyMap();
                }

                @Override
                public Map<String, Processor.Factory<SearchPhaseResultsProcessor>> getSearchPhaseResultsProcessors(Parameters parameters) {
                    return Collections.emptyMap();
                }

            }),
            client
        );
        final SearchRequest searchRequest = new SearchRequest();
        return searchPipelineService.resolvePipeline(searchRequest, indexNameExpressionResolver);
    }

    public void testGetContextVariables_whenNotPipelinedSearchRequest_thenReturnEmpty() {
        final IndicesRequest searchRequest = mock(IndicesRequest.class);

        final QueryCoordinatorContext queryCoordinatorContext = new QueryCoordinatorContext(mock(QueryRewriteContext.class), searchRequest);

        assertTrue(queryCoordinatorContext.getContextVariables().isEmpty());
    }

    public void testGetSearchRequest() {
        final IndicesRequest searchRequest = mock(IndicesRequest.class);

        final QueryCoordinatorContext queryCoordinatorContext = new QueryCoordinatorContext(mock(QueryRewriteContext.class), searchRequest);

        assertEquals(searchRequest, queryCoordinatorContext.getSearchRequest());
    }
}
