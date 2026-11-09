/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.reindex;

import org.opensearch.action.index.IndexAction;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.TransportSearchAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.script.ScriptService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportReindexActionTests extends OpenSearchTestCase {
    public void testResolveIndices() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        TransportSearchAction transportSearchAction = mock(TransportSearchAction.class);
        when(transportSearchAction.resolveIndices(any())).thenAnswer(i -> ResolvedIndices.of(((SearchRequest) i.getArgument(0)).indices()));
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(threadContext);

        TransportReindexAction action = new TransportReindexAction(
            Settings.EMPTY,
            threadPool,
            mock(ActionFilters.class),
            indexNameExpressionResolver,
            mock(ClusterService.class),
            mock(ScriptService.class),
            null,
            mock(Client.class),
            mock(TransportService.class),
            mock(ReindexSslConfig.class),
            transportSearchAction
        );

        {
            ResolvedIndices resolvedIndices = action.resolveIndices(
                new ReindexRequest(new SearchRequest("searched-index"), new IndexRequest("target-index"))
            );
            assertEquals(
                ResolvedIndices.of("searched-index").withLocalSubActions(IndexAction.INSTANCE, ResolvedIndices.Local.of("target-index")),
                resolvedIndices
            );
        }
    }
}
