package org.opensearch.cluster.metadata;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.view.CreateViewAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;

/** Service to interact with views, create, retrieve, update, and delete */
public class ViewService {

    private final static Logger LOG = LogManager.getLogger(ViewService.class);
    private final ClusterService clusterService;

    public ViewService(final ClusterService clusterService) {
        this.clusterService = clusterService;
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
}
