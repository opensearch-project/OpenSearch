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
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.View;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Service to interact with views, create, retrieve, update, and delete */
public class ViewService {

    private final static Logger LOG = LogManager.getLogger(ViewService.class);
    private final ClusterService clusterService;
    private final NodeClient client;

    public ViewService(final ClusterService clusterService, NodeClient client) {
        this.clusterService = clusterService;
        this.client = client;
    }

    public void createView(final CreateViewAction.Request request, final ActionListener<CreateViewAction.Response> listener) {
        final long currentTime = System.currentTimeMillis();

        final List<View.Target> targets = request.getTargets()
            .stream()
            .map(target -> new View.Target(target.getIndexPattern()))
            .collect(Collectors.toList());
        final View view = new View(request.getName(), request.getDescription(), currentTime, currentTime, targets);

        clusterService.submitStateUpdateTask("create_view_task", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(final ClusterState currentState) throws Exception {
                return new ClusterState.Builder(clusterService.state()).metadata(Metadata.builder(currentState.metadata()).put(view))
                    .build();
            }

            @Override
            public void onFailure(final String source, final Exception e) {
                LOG.error("Unable to create view, due to {}", source, e);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
                final View createdView = newState.getMetadata().views().get(request.getName());
                final CreateViewAction.Response response = new CreateViewAction.Response(createdView);
                listener.onResponse(response);
            }
        });
    }

    public void searchView(final SearchViewAction.Request request, final ActionListener<SearchResponse> listener) {
        final Optional<View> optView = Optional.ofNullable(clusterService)
            .map(ClusterService::state)
            .map(ClusterState::metadata)
            .map(m -> m.views())
            .map(views -> views.get(request.getView()));

        if (optView.isEmpty()) {
            throw new ResourceNotFoundException("no such view [" + request.getView() + "]");
        }
        final View view = optView.get();

        final String[] indices = view.getTargets()
            .stream()
            .map(View.Target::getIndexPattern)
            .collect(Collectors.toList())
            .toArray(new String[0]);
        request.indices(indices);

        client.executeLocally(SearchAction.INSTANCE, request, listener);
    }
}
