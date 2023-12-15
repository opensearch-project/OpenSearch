/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import joptsimple.internal.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.View;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.NamedRoute;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.opensearch.rest.RestRequest.Method.DELETE;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

public class RestViewAction extends BaseRestHandler {

    private final static Logger LOG = LogManager.getLogger(RestViewAction.class);

    private static final String VIEW_ID = "view_id";

    private final ClusterService clusterService;

    @Inject
    public RestViewAction(final ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public List<Route> routes() {
        final String viewIdParameter = "{" + VIEW_ID + "}";

        return List.of(
            new NamedRoute.Builder().path("/views").method(GET).uniqueName("cluster.views.list").build(),
            new NamedRoute.Builder().path("/views").method(POST).uniqueName("cluster.views.create").build(),
            new NamedRoute.Builder().path("/views/" + viewIdParameter).method(GET).uniqueName("cluster.views.get").build(),
            new NamedRoute.Builder().path("/views/" + viewIdParameter).method(DELETE).uniqueName("cluster.views.delete").build(),
            new NamedRoute.Builder().path("/views/" + viewIdParameter).method(PUT).uniqueName("cluster.views.update").build()
        );
    }

    @Override
    public String getName() {
        return "refresh_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        return channel -> {};
    }

    public RestResponse handleGet(final RestRequest r, final XContentBuilder builder) throws IOException {
        final List<View> views = Optional.ofNullable(clusterService.state().getMetadata())
            .map(m -> m.views())
            .map(v -> v.values())
            .map(v -> v.stream().collect(Collectors.toList()))
            .orElse(List.of());

        return new BytesRestResponse(RestStatus.OK, builder.startObject().array("views", views).endObject());
    }

    public RestResponse handlePost(final RestRequest r, final XContentBuilder builder) throws IOException {
        final View view;
        try (final XContentParser parser = r.contentParser()) {
            view = View.fromXContent(parser);
        }

        var task = new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(final ClusterState currentState) throws Exception {
                return new ClusterState.Builder(clusterService.state()).metadata(Metadata.builder(currentState.metadata()).put(view))
                    .build();
            }

            @Override
            public void onFailure(final String source, final Exception e) {
                LOG.error("Unable to create view, due to {}", source, e);
            }
        };

        clusterService.submitStateUpdateTask("create_view_task", task);

        // TODO: How to unasync?
        // TODO: Handle CREATED vs UPDATED
        return null;
    }

    public RestResponse handleSingleGet(final RestRequest r, final XContentBuilder builder) throws IOException {
        final String viewId = r.param(VIEW_ID);

        if (Strings.isNullOrEmpty(viewId)) {
            return new BytesRestResponse(RestStatus.NOT_FOUND, "");
        }

        final Optional<View> view = Optional.ofNullable(clusterService.state().getMetadata())
            .map(m -> m.views())
            .map(views -> views.get(viewId));

        if (view.isEmpty()) {
            return new BytesRestResponse(RestStatus.NOT_FOUND, "");
        }

        return new BytesRestResponse(RestStatus.OK, builder.startObject().value(view).endObject());
    }

    public RestResponse handleSinglePut(final RestRequest r) {
        return null;
    }

    public RestResponse handleSingleDelete(final RestRequest r) {
        return null;
    }

}
