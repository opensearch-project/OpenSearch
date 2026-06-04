/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.view;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/** Action to list a view names */
@ExperimentalApi
public class ListViewNamesAction extends ActionType<ListViewNamesAction.Response> {

    public static final ListViewNamesAction INSTANCE = new ListViewNamesAction();
    public static final String NAME = "views:data/read/list";

    public ListViewNamesAction() {
        super(NAME, ListViewNamesAction.Response::new);
    }

    /** Request for list view names */
    @ExperimentalApi
    public static class Request extends ActionRequest {
        public Request() {}

        public Request(final StreamInput in) {}

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Request that = (Request) o;
            return true;
        }

        @Override
        public int hashCode() {
            return 1;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    /** Response for list view names */
    @ExperimentalApi
    public static class Response extends ActionResponse implements ToXContentObject {

        private final List<String> views;

        public Response(final List<String> views) {
            this.views = views;
        }

        public Response(final StreamInput in) throws IOException {
            views = in.readStringList();
        }

        public List<String> getViewNames() {
            return views;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Response that = (Response) o;
            return views.equals(that.views);
        }

        @Override
        public int hashCode() {
            return Objects.hash(views);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeStringCollection(views);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            builder.field("views", views);
            builder.endObject();
            return builder;
        }
    }

    /**
     * Transport Action for getting a View
     */
    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final ViewService viewService;

        @Inject
        public TransportAction(final TransportService transportService, final ActionFilters actionFilters, final ViewService viewService) {
            super(NAME, transportService, actionFilters, Request::new);
            this.viewService = viewService;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            viewService.listViewNames(listener);
        }

    }

}
