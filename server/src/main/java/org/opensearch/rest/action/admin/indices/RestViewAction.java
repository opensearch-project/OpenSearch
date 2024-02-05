/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.view.CreateViewAction;
import org.opensearch.action.admin.indices.view.SearchViewAction;
import org.opensearch.action.search.SearchAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.ValidationException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.NamedRoute;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestCancellableNodeClient;
import org.opensearch.rest.action.RestStatusToXContentListener;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.rest.action.search.RestSearchAction;

import java.io.IOException;
import java.util.List;
import java.util.function.IntConsumer;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

/** All rest handlers for view actions */
public class RestViewAction {

    private final static Logger LOG = LogManager.getLogger(RestViewAction.class);

    public static final String VIEW_ID = "view_id";
    public static final String VIEW_ID_PARAMETER = "{" + VIEW_ID + "}";

    /** Handler for create view */
    public static class CreateViewHandler extends BaseRestHandler {

        @Override
        public List<Route> routes() {
            return List.of(new NamedRoute.Builder().path("/views").method(POST).uniqueName(CreateViewAction.NAME).build());
        }

        @Override
        public String getName() {
            return CreateViewAction.NAME;
        }

        @Override
        protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
            try (final XContentParser parser = request.contentParser()) {
                final CreateViewAction.Request createViewAction = CreateViewAction.Request.fromXContent(parser);
                return channel -> client.admin().indices().createView(createViewAction, new RestToXContentListener<>(channel));
            }
        }
    }

    public static class SearchViewHandler extends BaseRestHandler {
        @Override
        public List<Route> routes() {
            return List.of(
                new NamedRoute.Builder().path("/views/" + VIEW_ID_PARAMETER + "/_search")
                    .method(GET)
                    .uniqueName("cluster:views:search")
                    .build(),
                new NamedRoute.Builder().path("/views/" + VIEW_ID_PARAMETER + "/_search")
                    .method(POST)
                    .uniqueName("cluster:views:search")
                    .build()
            );
        }

        @Override
        public String getName() {
            return "view_search_action";
        }

        @Override
        public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
            final String viewId = request.param(VIEW_ID);

            final SearchViewAction.Request viewSearchRequest = new SearchViewAction.Request(viewId);
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

            final ValidationException validationResult = viewSearchRequest.validate();
            if (validationResult != null) {
                throw validationResult;
            }

            return channel -> {
                final RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
                cancelClient.execute(SearchAction.INSTANCE, viewSearchRequest, new RestStatusToXContentListener<>(channel));
            };
        }
    }

    // public List<Route> routes() {

    // return List.of(
    // new NamedRoute.Builder().path("/views").method(GET).uniqueName("cluster:views:list").build(),
    // new NamedRoute.Builder().path("/views/" + viewIdParameter).method(GET).uniqueName("cluster:views:get").build(),
    // new NamedRoute.Builder().path("/views/" + viewIdParameter).method(DELETE).uniqueName("cluster:views:delete").build(),
    // new NamedRoute.Builder().path("/views/" + viewIdParameter).method(PUT).uniqueName("cluster:views:update").build()
    // );
    // }

    // public RestResponse handlePost(final RestRequest r, final RestChannel channel) throws IOException {
    // final View inputView;
    // try (final XContentParser parser = r.contentParser()) {
    // inputView = View.fromXContent(parser);
    // }

    // final long currentTime = System.currentTimeMillis();
    // final View view = new View(inputView.name, inputView.description, currentTime, currentTime, inputView.targets);

    // clusterService.submitStateUpdateTask("create_view_task", new ClusterStateUpdateTask() {
    // @Override
    // public ClusterState execute(final ClusterState currentState) throws Exception {
    // return new ClusterState.Builder(clusterService.state()).metadata(Metadata.builder(currentState.metadata()).put(view))
    // .build();
    // }

    // @Override
    // public void onFailure(final String source, final Exception e) {
    // LOG.error("Unable to create view, due to {}", source, e);
    // channel.sendResponse(
    // new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, "Unknown error occurred, see the log for details.")
    // );
    // }

    // @Override
    // public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
    // try {
    // channel.sendResponse(
    // new BytesRestResponse(RestStatus.CREATED, channel.newBuilder().startObject().field(view.name, view).endObject())
    // );
    // } catch (final IOException e) {
    // // TODO?
    // LOG.error(e);
    // }
    // }
    // });
    // // TODO: Handle CREATED vs UPDATED
    // return null;
    // }

    // public RestResponse handleSingleGet(final RestRequest r, final XContentBuilder builder) throws IOException {
    // final String viewId = r.param(VIEW_ID);

    // if (Strings.isNullOrEmpty(viewId)) {
    // return new BytesRestResponse(RestStatus.NOT_FOUND, "");
    // }

    // final Optional<View> view = Optional.ofNullable(clusterService.state().getMetadata())
    // .map(m -> m.views())
    // .map(views -> views.get(viewId));

    // if (view.isEmpty()) {
    // return new BytesRestResponse(RestStatus.NOT_FOUND, "");
    // }

    // return new BytesRestResponse(RestStatus.OK, builder.startObject().value(view).endObject());
    // }

    // public RestResponse handleSinglePut(final RestRequest r) {
    // return new BytesRestResponse(RestStatus.NOT_IMPLEMENTED, "");
    // }

    // public RestResponse handleSingleDelete(final RestRequest r) {
    // return new BytesRestResponse(RestStatus.NOT_IMPLEMENTED, "");
    // }

}
