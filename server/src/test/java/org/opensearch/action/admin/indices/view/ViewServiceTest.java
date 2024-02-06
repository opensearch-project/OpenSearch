/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.view;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.search.SearchAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.View;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class ViewServiceTest {

    private final View.Target typicalTarget = new View.Target("my-index-*");
    private final View typicalView = new View("actualView", "actual description", -1L, -1L, List.of(typicalTarget));

    private ClusterService clusterService;
    private NodeClient nodeClient;
    private final AtomicLong currentTime = new AtomicLong(0);
    private LongSupplier timeProvider = currentTime::longValue;
    private ViewService viewService;

    @Before
    public void before() {
        clusterService = mock(ClusterService.class);
        nodeClient = mock(NodeClient.class);
        timeProvider = mock(LongSupplier.class);
        doAnswer(invocation -> currentTime.get()).when(timeProvider).getAsLong();
        viewService = spy(new ViewService(clusterService, nodeClient, timeProvider));
    }

    @After
    public void after() {
        verifyNoMoreInteractions(timeProvider, clusterService, nodeClient);
    }

    @Test
    public void createView() {
        final var request = new CreateViewAction.Request("a", "b", List.of(new CreateViewAction.Request.Target("my-index-*")));
        final var listener = mock(ActionListener.class);
        setGetViewOrThrowExceptionToReturnTypicalView();

        viewService.createView(request, listener);

        verify(clusterService).submitStateUpdateTask(eq("create_view_task"), any());
        verify(timeProvider).getAsLong();
    }

    @Test
    public void updateView() {
        final var request = new CreateViewAction.Request("a", "b", List.of(new CreateViewAction.Request.Target("my-index-*")));
        final var listener = mock(ActionListener.class);
        setGetViewOrThrowExceptionToReturnTypicalView();

        viewService.updateView(request, listener);

        verify(clusterService).submitStateUpdateTask(eq("update_view_task"), any());
        verify(timeProvider).getAsLong();
    }

    @Test
    public void updateView_doesNotExist() {
        final var request = new CreateViewAction.Request("a", "b", List.of(new CreateViewAction.Request.Target("my-index-*")));
        final var listener = mock(ActionListener.class);
        doThrow(new ResourceNotFoundException("abc")).when(viewService).getViewOrThrowException(anyString());

        final Exception ex = assertThrows(ResourceNotFoundException.class, () -> viewService.updateView(request, listener));
        MatcherAssert.assertThat(ex.getMessage(), equalTo("abc"));
    }

    @Test
    public void deleteView() {
        final var request = new DeleteViewAction.Request("viewName");
        final var listener = mock(ActionListener.class);
        setGetViewOrThrowExceptionToReturnTypicalView();

        viewService.deleteView(request, listener);

        verify(clusterService).submitStateUpdateTask(eq("delete_view_task"), any());
    }

    @Test
    public void deleteView_doesNotExist() {
        final var request = new DeleteViewAction.Request("viewName");
        final var listener = mock(ActionListener.class);
        doThrow(new ResourceNotFoundException("abc")).when(viewService).getViewOrThrowException(anyString());

        final ResourceNotFoundException ex = assertThrows(ResourceNotFoundException.class, () -> viewService.deleteView(request, listener));

        MatcherAssert.assertThat(ex.getMessage(), equalTo("abc"));
    }

    @Test
    public void getView() {
        final var request = new GetViewAction.Request("viewName");
        final var listener = mock(ActionListener.class);
        setGetViewOrThrowExceptionToReturnTypicalView();

        viewService.getView(request, listener);

        verify(listener).onResponse(any());
    }

    @Test
    public void getView_doesNotExist() {
        final var request = new GetViewAction.Request("viewName");
        final var listener = mock(ActionListener.class);
        doThrow(new ResourceNotFoundException("abc")).when(viewService).getViewOrThrowException(anyString());

        final ResourceNotFoundException ex = assertThrows(ResourceNotFoundException.class, () -> viewService.getView(request, listener));

        MatcherAssert.assertThat(ex.getMessage(), equalTo("abc"));
    }

    @Test
    public void listViewNames() {
        final var clusterState = new ClusterState.Builder(new ClusterName("MyCluster")).metadata(
            new Metadata.Builder().views(Map.of(typicalView.getName(), typicalView)).build()
        ).build();
        final var listener = mock(ActionListener.class);
        when(clusterService.state()).thenReturn(clusterState);

        viewService.listViewNames(listener);

        verify(clusterService).state();
        verify(listener).onResponse(any());
    }

    @Test
    public void listViewNames_noViews() {
        final var clusterState = new ClusterState.Builder(new ClusterName("MyCluster")).build();
        final var listener = mock(ActionListener.class);
        when(clusterService.state()).thenReturn(clusterState);

        viewService.listViewNames(listener);

        verify(clusterService).state();
        verify(listener).onResponse(any());
    }

    @Test
    public void searchView() {
        final var request = spy(new SearchViewAction.Request("view"));
        final var listener = mock(ActionListener.class);
        setGetViewOrThrowExceptionToReturnTypicalView();

        viewService.searchView(request, listener);

        verify(nodeClient).executeLocally(eq(SearchAction.INSTANCE), any(), any(ActionListener.class));
        verify(request).indices(typicalTarget.getIndexPattern());
    }

    private void setGetViewOrThrowExceptionToReturnTypicalView() {
        doAnswer(invocation -> typicalView).when(viewService).getViewOrThrowException(anyString());
    }
}
