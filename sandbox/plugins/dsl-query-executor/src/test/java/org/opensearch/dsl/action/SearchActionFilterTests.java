/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.Index;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class SearchActionFilterTests extends OpenSearchTestCase {

    private final NodeClient client = mock(NodeClient.class);
    private final ClusterService clusterService = mock(ClusterService.class);
    private final IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
    private final Task task = mock(Task.class);
    private final ActionListener<ActionResponse> listener = mock(ActionListener.class);
    private final ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
    private final ActionRequestMetadata<ActionRequest, ActionResponse> metadata = mock(ActionRequestMetadata.class);
    private final SearchActionFilter filter = new SearchActionFilter(client, clusterService, indexNameExpressionResolver);

    private static final Settings PLUGGABLE_SETTINGS = Settings.builder()
        .put("index.pluggable.dataformat.enabled", true)
        .build();

    public void testOrderRunsAfterSecurityFilter() {
        assertEquals(SearchActionFilter.FILTER_ORDER, filter.order());
    }

    public void testPassesThroughNonSearchAction() {
        BulkRequest request = new BulkRequest();

        filter.apply(task, BulkAction.NAME, request, metadata, listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, request, listener);
        verify(client, never()).execute(any(), any(), any());
    }

    public void testReroutesSearchActionWhenNoPluggableDataFormat() {
        SearchRequest request = new SearchRequest("test-index");
        mockIndices(request, indexWithSettings("test-index", Settings.EMPTY));

        filter.apply(task, SearchAction.NAME, request, this.metadata, listener, chain);

        verify(client).execute(eq(DslExecuteAction.INSTANCE), eq(request), any());
        verify(chain, never()).proceed(any(), any(), any(), any());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testBypassesDslWhenAllIndicesHavePluggableDataFormat() {
        SearchRequest request = new SearchRequest("idx1", "idx2");
        mockIndices(request, indexWithSettings("idx1", PLUGGABLE_SETTINGS), indexWithSettings("idx2", PLUGGABLE_SETTINGS));

        filter.apply(task, SearchAction.NAME, request, this.metadata, listener, chain);

        verify(chain).proceed(task, SearchAction.NAME, request, listener);
        verify(client, never()).execute(any(), any(), any());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testFailsOnMixedPluggableDataFormatIndices() {
        SearchRequest request = new SearchRequest("idx1", "idx2");
        mockIndices(request, indexWithSettings("idx1", PLUGGABLE_SETTINGS), indexWithSettings("idx2", Settings.EMPTY));

        filter.apply(task, SearchAction.NAME, request, this.metadata, listener, chain);

        verify(listener).onFailure(argThat(e -> e instanceof IllegalArgumentException && e.getMessage().contains("mix of indices")));
        verify(chain, never()).proceed(any(), any(), any(), any());
        verify(client, never()).execute(any(), any(), any());
    }

    private IndexEntry indexWithSettings(String name, Settings settings) {
        return new IndexEntry(new Index(name, name + "-uuid"), settings);
    }

    private void mockIndices(SearchRequest request, IndexEntry... entries) {
        ClusterState state = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metadata()).thenReturn(metadata);

        Index[] indices = new Index[entries.length];
        for (int i = 0; i < entries.length; i++) {
            indices[i] = entries[i].index;
            IndexMetadata indexMetadata = mock(IndexMetadata.class);
            when(indexMetadata.getSettings()).thenReturn(entries[i].settings);
            when(metadata.index(entries[i].index)).thenReturn(indexMetadata);
        }
        when(indexNameExpressionResolver.concreteIndices(state, request)).thenReturn(indices);
    }

    private record IndexEntry(Index index, Settings settings) {}
}
