/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.view;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.View;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.client.node.NodeClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/** Service to interact with views, create, retrieve, update, and delete */
@ExperimentalApi
public class ViewService {

    private final static Logger LOG = LogManager.getLogger(ViewService.class);
    private final ClusterService clusterService;
    private final NodeClient client;
    private final LongSupplier timeProvider;

    public ViewService(final ClusterService clusterService, final NodeClient client, final LongSupplier timeProvider) {
        this.clusterService = clusterService;
        this.client = client;
        this.timeProvider = Optional.ofNullable(timeProvider).orElse(System::currentTimeMillis);
    }

    public void createView(final CreateViewAction.Request request, final ActionListener<GetViewAction.Response> listener) {
        final long currentTime = timeProvider.getAsLong();

        final List<View.Target> targets = request.getTargets()
            .stream()
            .map(target -> new View.Target(target.getIndexPattern()))
            .collect(Collectors.toList());
        final View view = new View(request.getName(), request.getDescription(), currentTime, currentTime, new TreeSet<>(targets));

        createOrUpdateView(Operation.CreateView, view, listener);
    }

    public void updateView(final CreateViewAction.Request request, final ActionListener<GetViewAction.Response> listener) {
        final View originalView = getViewOrThrowException(request.getName());

        final long currentTime = timeProvider.getAsLong();
        final List<View.Target> targets = request.getTargets()
            .stream()
            .map(target -> new View.Target(target.getIndexPattern()))
            .collect(Collectors.toList());
        final View updatedView = new View(
            request.getName(),
            request.getDescription(),
            originalView.getCreatedAt(),
            currentTime,
            new TreeSet<>(targets)
        );

        createOrUpdateView(Operation.UpdateView, updatedView, listener);
    }

    public void deleteView(final DeleteViewAction.Request request, final ActionListener<AcknowledgedResponse> listener) {
        getViewOrThrowException(request.getName());

        clusterService.submitStateUpdateTask("delete_view_task", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(final ClusterState currentState) throws Exception {
                return new ClusterState.Builder(clusterService.state()).metadata(
                    Metadata.builder(currentState.metadata()).removeView(request.getName())
                ).build();
            }

            @Override
            public void onFailure(final String source, final Exception e) {
                LOG.error("Unable to delete view, from " + source, e);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
                listener.onResponse(new AcknowledgedResponse(true));
            }
        });
    }

    public void getView(final GetViewAction.Request request, final ActionListener<GetViewAction.Response> listener) {
        final View view = getViewOrThrowException(request.getName());

        listener.onResponse(new GetViewAction.Response(view));
    }

    public void listViewNames(final ActionListener<ListViewNamesAction.Response> listener) {
        final List<String> viewNames = new ArrayList<>(
            Optional.ofNullable(clusterService)
                .map(ClusterService::state)
                .map(ClusterState::metadata)
                .map(Metadata::views)
                .map(Map::keySet)
                .orElseThrow()
        );

        listener.onResponse(new ListViewNamesAction.Response(viewNames));
    }

    public void searchView(final SearchViewAction.Request request, final ActionListener<SearchResponse> listener) {
        final View view = getViewOrThrowException(request.getView());

        final String[] indices = view.getTargets().stream().map(View.Target::getIndexPattern).toArray(String[]::new);
        request.indices(indices);

        client.executeLocally(SearchAction.INSTANCE, request, listener);
    }

    View getViewOrThrowException(final String viewName) {
        return Optional.ofNullable(clusterService)
            .map(ClusterService::state)
            .map(ClusterState::metadata)
            .map(Metadata::views)
            .map(views -> views.get(viewName))
            .orElseThrow(() -> new ViewNotFoundException(viewName));
    }

    private enum Operation {
        CreateView("create", false),
        UpdateView("update", true);

        private final String name;
        private final boolean allowOverriding;

        Operation(final String name, final boolean allowOverriding) {
            this.name = name;
            this.allowOverriding = allowOverriding;
        }
    }

    private void createOrUpdateView(final Operation operation, final View view, final ActionListener<GetViewAction.Response> listener) {
        clusterService.submitStateUpdateTask(operation.name + "_view_task", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(final ClusterState currentState) throws Exception {
                if (!operation.allowOverriding && currentState.metadata().views().containsKey(view.getName())) {
                    throw new ViewAlreadyExistsException(view.getName());
                }
                return new ClusterState.Builder(clusterService.state()).metadata(Metadata.builder(currentState.metadata()).put(view))
                    .build();
            }

            @Override
            public void onFailure(final String source, final Exception e) {
                LOG.error("Unable to " + operation.name + " view, from " + source, e);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
                final View createdView = newState.getMetadata().views().get(view.getName());
                final GetViewAction.Response response = new GetViewAction.Response(createdView);
                listener.onResponse(response);
            }
        });
    }
}
