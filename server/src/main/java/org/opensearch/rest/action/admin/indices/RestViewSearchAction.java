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
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.ViewSearchRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.metadata.View;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.NamedRoute;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestCancellableNodeClient;
import org.opensearch.rest.action.RestStatusToXContentListener;
import org.opensearch.rest.action.search.RestSearchAction;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

/** TODO */
public class RestViewSearchAction extends BaseRestHandler {

    private final static Logger LOG = LogManager.getLogger(RestViewSearchAction.class);

    private static final String VIEW_ID = "view_id";

    private final ClusterService clusterService;

    @Inject
    public RestViewSearchAction(final ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public List<Route> routes() {
        final String viewIdParameter = "{" + VIEW_ID + "}";

        return List.of(
            new NamedRoute.Builder().path("/views/" + viewIdParameter + "/_search").method(GET).uniqueName("cluster:views:search").build(),
            new NamedRoute.Builder().path("/views/" + viewIdParameter + "/_search").method(POST).uniqueName("cluster:views:search").build()
        );
    }

    @Override
    public String getName() {
        return "view_search_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String viewId = request.param(VIEW_ID);
        return channel -> {

            if (Strings.isNullOrEmpty(viewId)) {
                channel.sendResponse(new BytesRestResponse(RestStatus.NOT_FOUND, ""));
            }

            final Optional<View> optView = Optional.ofNullable(clusterService.state().getMetadata())
                .map(m -> m.views())
                .map(views -> views.get(viewId));

            if (optView.isEmpty()) {
                channel.sendResponse(new BytesRestResponse(RestStatus.NOT_FOUND, ""));
            }
            final View view = optView.get();

            final ViewSearchRequest viewSearchRequest = new ViewSearchRequest(view);
            final IntConsumer setSize = size -> viewSearchRequest.source().size(size);

            request.withContentOrSourceParamParserOrNull(
                parser -> RestSearchAction.parseSearchRequest(
                    viewSearchRequest,
                    request,
                    parser,
                    client.getNamedWriteableRegistry(),
                    setSize
                )
            );

            // TODO: Only allow operations that are supported

            final String[] indices = view.targets.stream()
                .map(target -> target.indexPattern)
                .collect(Collectors.toList())
                .toArray(new String[0]);
            viewSearchRequest.indices(indices);

            // TODO: Look into resource leak on cancelClient? Note; is already leaking in
            // server/src/main/java/org/opensearch/rest/action/search/RestSearchAction.java
            final RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancelClient.execute(SearchAction.INSTANCE, viewSearchRequest, new RestStatusToXContentListener<>(channel));
        };
    }
}
