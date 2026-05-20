/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.view;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

import static org.opensearch.action.ValidateActions.addValidationError;

/** Action to create a view */
@ExperimentalApi
public class SearchViewAction extends ActionType<SearchResponse> {

    public static final SearchViewAction INSTANCE = new SearchViewAction();
    public static final String NAME = "views:data/read/search";

    private SearchViewAction() {
        super(NAME, SearchResponse::new);
    }

    /**
     * Wraps the functionality of search requests and tailors for what is available
     * when searching through views
     */
    @ExperimentalApi
    public static class Request extends SearchRequest {

        private final String view;

        public Request(final String view, final SearchRequest searchRequest) {
            super(searchRequest);
            this.view = view;
        }

        public Request(final StreamInput in) throws IOException {
            super(in);
            view = in.readString();
        }

        public String getView() {
            return view;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request that = (Request) o;
            return view.equals(that.view) && super.equals(that);
        }

        @Override
        public int hashCode() {
            return Objects.hash(view, super.hashCode());
        }

        @Override
        public ActionRequestValidationException validate() {
            final Function<String, String> unsupported = (String x) -> x + " is not supported when searching views";
            ActionRequestValidationException validationException = super.validate();

            if (scroll() != null) {
                validationException = addValidationError(unsupported.apply("Scroll"), validationException);
            }

            // TODO: Filter out any additional search features that are not supported.
            // Required before removing @ExperimentalApi annotations.

            if (Strings.isNullOrEmpty(view)) {
                validationException = addValidationError("View is required", validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(view);
        }

        @Override
        public String toString() {
            return super.toString().replace("SearchRequest{", "SearchViewAction.Request{view=" + view + ",");
        }
    }

    /**
     * Transport Action for searching a View
     */
    public static class TransportAction extends HandledTransportAction<Request, SearchResponse> {

        private final ViewService viewService;

        @Inject
        public TransportAction(final TransportService transportService, final ActionFilters actionFilters, final ViewService viewService) {
            super(NAME, transportService, actionFilters, Request::new);
            this.viewService = viewService;
        }

        @Override
        protected void doExecute(final Task task, final Request request, final ActionListener<SearchResponse> listener) {
            viewService.searchView(request, listener);
        }
    }
}
