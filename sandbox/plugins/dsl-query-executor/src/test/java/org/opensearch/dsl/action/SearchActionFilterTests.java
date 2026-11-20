/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexSettings;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class SearchActionFilterTests extends OpenSearchTestCase {

    private final NodeClient client = mock(NodeClient.class);
    private final Task task = mock(Task.class);
    private final ActionListener<ActionResponse> listener = mock(ActionListener.class);
    private final ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
    private final ActionRequestMetadata<ActionRequest, ActionResponse> metadata = mock(ActionRequestMetadata.class);
    private final ClusterService clusterService = mock(ClusterService.class);
    private final IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
    private final SearchActionFilter filter = new SearchActionFilter(client, clusterService, resolver);

    public void testOrderRunsAfterSecurityFilter() {
        assertEquals(SearchActionFilter.FILTER_ORDER, filter.order());
    }

    public void testPassesThroughNonSearchAction() {
        BulkRequest request = new BulkRequest();

        filter.apply(task, BulkAction.NAME, request, metadata, listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, request, listener);
        verify(client, never()).execute(any(), any(), any());
    }

    public void testReroutesSearchAction() {
        SearchRequest request = new SearchRequest("test-index");
        configureIndices(request, true);

        filter.apply(task, SearchAction.NAME, request, metadata, listener, chain);

        verify(client).execute(eq(ExecuteAction.INSTANCE), eq(request), any());
        verify(chain, never()).proceed(any(), any(), any(), any());
    }

    public void testReroutesAliasResolvingToPluggableIndex() {
        SearchRequest request = new SearchRequest("test-alias");
        configureIndices(request, true);

        filter.apply(task, SearchAction.NAME, request, metadata, listener, chain);

        verify(client).execute(eq(ExecuteAction.INSTANCE), eq(request), any());
        verify(chain, never()).proceed(any(), any(), any(), any());
    }

    public void testReroutesMultiplePluggableIndices() {
        SearchRequest request = new SearchRequest("test-*");
        configureIndices(request, true, true);

        filter.apply(task, SearchAction.NAME, request, metadata, listener, chain);

        verify(client).execute(eq(ExecuteAction.INSTANCE), eq(request), any());
        verify(chain, never()).proceed(any(), any(), any(), any());
    }

    public void testPassesThroughStandardIndex() {
        SearchRequest request = new SearchRequest("test-index");
        configureIndices(request, false);

        filter.apply(task, SearchAction.NAME, request, metadata, listener, chain);

        verify(chain).proceed(task, SearchAction.NAME, request, listener);
        verify(client, never()).execute(any(), any(), any());
    }

    public void testPassesThroughMultipleStandardIndices() {
        SearchRequest request = new SearchRequest("test-*");
        configureIndices(request, false, false);

        filter.apply(task, SearchAction.NAME, request, metadata, listener, chain);

        verify(chain).proceed(task, SearchAction.NAME, request, listener);
        verify(client, never()).execute(any(), any(), any());
    }

    public void testPassesThroughMixedIndices() {
        SearchRequest request = new SearchRequest("test-*");
        configureIndices(request, true, false);

        filter.apply(task, SearchAction.NAME, request, metadata, listener, chain);

        verify(chain).proceed(task, SearchAction.NAME, request, listener);
        verify(client, never()).execute(any(), any(), any());
    }

    public void testPassesThroughEmptyResolution() {
        SearchRequest request = new SearchRequest("missing-*");
        configureIndices(request);

        filter.apply(task, SearchAction.NAME, request, metadata, listener, chain);

        verify(chain).proceed(task, SearchAction.NAME, request, listener);
        verify(client, never()).execute(any(), any(), any());
    }

    public void testPassesThroughResolutionFailure() {
        SearchRequest request = new SearchRequest("missing-index");
        ClusterState state = ClusterState.builder(new ClusterName("test")).build();
        when(clusterService.state()).thenReturn(state);
        when(resolver.concreteIndices(state, request)).thenThrow(new IllegalArgumentException("invalid index expression"));

        filter.apply(task, SearchAction.NAME, request, metadata, listener, chain);

        verify(chain).proceed(task, SearchAction.NAME, request, listener);
        verify(client, never()).execute(any(), any(), any());
    }

    public void testReroutesValidateQueryAction() {
        ValidateQueryRequest request = new ValidateQueryRequest("test-index");
        configureIndices(request, true);

        filter.apply(task, ValidateQueryAction.NAME, request, metadata, listener, chain);

        verify(client).execute(eq(ValidateAction.INSTANCE), eq(request), any());
        verify(chain, never()).proceed(any(), any(), any(), any());
    }

    public void testPassesThroughValidateQueryForStandardIndex() {
        ValidateQueryRequest request = new ValidateQueryRequest("test-index");
        configureIndices(request, false);

        filter.apply(task, ValidateQueryAction.NAME, request, metadata, listener, chain);

        verify(chain).proceed(task, ValidateQueryAction.NAME, request, listener);
        verify(client, never()).execute(any(), any(), any());
    }

    private void configureIndices(IndicesRequest request, boolean... pluggableDataFormatEnabled) {
        Metadata.Builder metadataBuilder = Metadata.builder();
        Index[] concreteIndices = new Index[pluggableDataFormatEnabled.length];
        for (int i = 0; i < pluggableDataFormatEnabled.length; i++) {
            String indexName = pluggableDataFormatEnabled.length == 1 ? "test-index" : "test-index-" + i;
            IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                        .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), pluggableDataFormatEnabled[i])
                )
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
            metadataBuilder.put(indexMetadata, true);
            concreteIndices[i] = indexMetadata.getIndex();
        }
        ClusterState state = ClusterState.builder(new ClusterName("test")).metadata(metadataBuilder).build();
        when(clusterService.state()).thenReturn(state);
        when(resolver.concreteIndices(state, request)).thenReturn(concreteIndices);
    }
}
