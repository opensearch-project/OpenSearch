/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.admin.indices.view.CreateViewAction;
import org.opensearch.action.admin.indices.view.DeleteViewAction;
import org.opensearch.action.admin.indices.view.GetViewAction;
import org.opensearch.action.admin.indices.view.ListViewNamesAction;
import org.opensearch.action.admin.indices.view.SearchViewAction;
import org.opensearch.action.admin.indices.view.UpdateViewAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.ValidationException;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.NamedRoute;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestCancellableNodeClient;
import org.opensearch.rest.action.RestStatusToXContentListener;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.rest.action.search.RestSearchAction;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.function.IntConsumer;

import static org.opensearch.rest.RestRequest.Method.DELETE;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

/** All rest handlers for view actions */
@ExperimentalApi
public class RestViewAction {

    public static final String VIEW_NAME = "view_name";
    public static final String VIEW_NAME_PARAMETER = "{" + VIEW_NAME + "}";

    /** Handler for create view */
    @ExperimentalApi
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

                final ValidationException validationResult = createViewAction.validate();
                if (validationResult != null) {
                    throw validationResult;
                }

                return channel -> client.admin().indices().createView(createViewAction, new RestToXContentListener<>(channel));
            }
        }
    }

    /** Handler for delete view */
    @ExperimentalApi
    public static class DeleteViewHandler extends BaseRestHandler {

        @Override
        public List<Route> routes() {
            return List.of(
                new NamedRoute.Builder().path("/views/" + VIEW_NAME_PARAMETER).method(DELETE).uniqueName(DeleteViewAction.NAME).build()
            );
        }

        @Override
        public String getName() {
            return DeleteViewAction.NAME;
        }

        @Override
        protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
            final String viewId = request.param(VIEW_NAME);

            final DeleteViewAction.Request deleteRequest = new DeleteViewAction.Request(viewId);

            final ValidationException validationResult = deleteRequest.validate();
            if (validationResult != null) {
                throw validationResult;
            }

            return channel -> client.admin().indices().deleteView(deleteRequest, new RestToXContentListener<>(channel));
        }
    }

    /** Handler for update view */
    @ExperimentalApi
    public static class UpdateViewHandler extends BaseRestHandler {

        @Override
        public List<Route> routes() {
            return List.of(
                new NamedRoute.Builder().path("/views/" + VIEW_NAME_PARAMETER).method(PUT).uniqueName(UpdateViewAction.NAME).build()
            );
        }

        @Override
        public String getName() {
            return UpdateViewAction.NAME;
        }

        @Override
        protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
            final String viewId = request.param(VIEW_NAME);

            try (final XContentParser parser = request.contentParser()) {
                final CreateViewAction.Request updateRequest = UpdateViewAction.Request.fromXContent(parser, viewId);

                final ValidationException validationResult = updateRequest.validate();
                if (validationResult != null) {
                    throw validationResult;
                }

                return channel -> client.admin().indices().updateView(updateRequest, new RestToXContentListener<>(channel));
            }
        }
    }

    /** Handler for get view */
    @ExperimentalApi
    public static class GetViewHandler extends BaseRestHandler {

        @Override
        public List<Route> routes() {
            return List.of(
                new NamedRoute.Builder().path("/views/" + VIEW_NAME_PARAMETER).method(GET).uniqueName(GetViewAction.NAME).build()
            );
        }

        @Override
        public String getName() {
            return GetViewAction.NAME;
        }

        @Override
        protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
            final String viewId = request.param(VIEW_NAME);

            final GetViewAction.Request getRequest = new GetViewAction.Request(viewId);

            final ValidationException validationResult = getRequest.validate();
            if (validationResult != null) {
                throw validationResult;
            }

            return channel -> client.admin().indices().getView(getRequest, new RestToXContentListener<>(channel));
        }
    }

    /** Handler for get view */
    @ExperimentalApi
    public static class ListViewNamesHandler extends BaseRestHandler {

        @Override
        public List<Route> routes() {
            return List.of(new NamedRoute.Builder().path("/views/").method(GET).uniqueName(ListViewNamesAction.NAME).build());
        }

        @Override
        public String getName() {
            return ListViewNamesAction.NAME;
        }

        @Override
        protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
            return channel -> client.listViewNames(new ListViewNamesAction.Request(), new RestToXContentListener<>(channel));
        }
    }

    /** Handler for search view */
    @ExperimentalApi
    public static class SearchViewHandler extends BaseRestHandler {
        @Override
        public List<Route> routes() {
            return List.of(
                new NamedRoute.Builder().path("/views/" + VIEW_NAME_PARAMETER + "/_search")
                    .method(GET)
                    .uniqueName(SearchViewAction.NAME)
                    .build(),
                new NamedRoute.Builder().path("/views/" + VIEW_NAME_PARAMETER + "/_search")
                    .method(POST)
                    .uniqueName(SearchViewAction.NAME)
                    .build()
            );
        }

        @Override
        public String getName() {
            return SearchViewAction.NAME;
        }

        @Override
        public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
            final String viewId = request.param(VIEW_NAME);

            final SearchViewAction.Request viewSearchRequest = new SearchViewAction.Request(viewId, new SearchRequest());
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
                cancelClient.execute(SearchViewAction.INSTANCE, viewSearchRequest, new RestStatusToXContentListener<>(channel));
            };
        }
    }
}
