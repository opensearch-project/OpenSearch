/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ubi;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.ubi.ext.UbiParameters;
import org.opensearch.ubi.ext.UbiParametersExtBuilder;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UbiActionFilterTests extends OpenSearchTestCase {

    @SuppressWarnings("unchecked")
    public void testApplyWithoutUbiBlock() {

        final Client client = mock(Client.class);
        final AdminClient adminClient = mock(AdminClient.class);
        final IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);

        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);

        final ActionFuture<IndicesExistsResponse> actionFuture = mock(ActionFuture.class);
        when(indicesAdminClient.exists(any(IndicesExistsRequest.class))).thenReturn(actionFuture);

        final UbiActionFilter ubiActionFilter = new UbiActionFilter(client);
        final ActionListener<SearchResponse> listener = mock(ActionListener.class);

        final SearchRequest request = mock(SearchRequest.class);
        SearchHit[] searchHit = {};
        final SearchHits searchHits = new SearchHits(searchHit, new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0);

        final SearchResponse response = mock(SearchResponse.class);
        when(response.getHits()).thenReturn(searchHits);

        final Task task = mock(Task.class);

        final ActionFilterChain<SearchRequest, SearchResponse> chain = mock(ActionFilterChain.class);

        doAnswer(invocation -> {
            ActionListener<SearchResponse> actionListener = invocation.getArgument(3);
            actionListener.onResponse(response);
            return null;
        }).when(chain).proceed(eq(task), anyString(), eq(request), any());

        UbiParametersExtBuilder builder = mock(UbiParametersExtBuilder.class);
        final List<SearchExtBuilder> builders = new ArrayList<>();
        builders.add(builder);

        when(builder.getWriteableName()).thenReturn(UbiParametersExtBuilder.UBI_PARAMETER_NAME);
        when(builder.getParams()).thenReturn(null);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.ext(builders);

        when(request.source()).thenReturn(searchSourceBuilder);

        ubiActionFilter.apply(task, "ubi", request, listener, chain);

        verify(client, never()).index(any(), any());

    }

    @SuppressWarnings("unchecked")
    public void testApplyWithUbiBlockWithoutQueryId() {

        final Client client = mock(Client.class);
        final AdminClient adminClient = mock(AdminClient.class);
        final IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);

        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);

        final ActionFuture<IndicesExistsResponse> actionFuture = mock(ActionFuture.class);
        when(indicesAdminClient.exists(any(IndicesExistsRequest.class))).thenReturn(actionFuture);

        final UbiActionFilter ubiActionFilter = new UbiActionFilter(client);
        final ActionListener<SearchResponse> listener = mock(ActionListener.class);

        final SearchRequest request = mock(SearchRequest.class);
        SearchHit[] searchHit = {};
        final SearchHits searchHits = new SearchHits(searchHit, new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0);

        final SearchResponse response = mock(SearchResponse.class);
        when(response.getHits()).thenReturn(searchHits);

        final Task task = mock(Task.class);

        final ActionFilterChain<SearchRequest, SearchResponse> chain = mock(ActionFilterChain.class);

        doAnswer(invocation -> {
            ActionListener<SearchResponse> actionListener = invocation.getArgument(3);
            actionListener.onResponse(response);
            return null;
        }).when(chain).proceed(eq(task), anyString(), eq(request), any());

        UbiParametersExtBuilder builder = mock(UbiParametersExtBuilder.class);
        final List<SearchExtBuilder> builders = new ArrayList<>();
        builders.add(builder);

        final UbiParameters ubiParameters = new UbiParameters();

        when(builder.getWriteableName()).thenReturn(UbiParametersExtBuilder.UBI_PARAMETER_NAME);
        when(builder.getParams()).thenReturn(ubiParameters);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.ext(builders);

        when(request.source()).thenReturn(searchSourceBuilder);

        ubiActionFilter.apply(task, "ubi", request, listener, chain);

        verify(client, never()).index(any(), any());

    }

}
