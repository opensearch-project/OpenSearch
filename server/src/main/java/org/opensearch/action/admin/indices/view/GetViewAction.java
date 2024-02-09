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
import org.opensearch.action.ValidateActions;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.View;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ToXContent.Params;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

/** Action to get a view */
@ExperimentalApi
public class GetViewAction extends ActionType<GetViewAction.Response> {

    public static final GetViewAction INSTANCE = new GetViewAction();
    public static final String NAME = "views:data/read/get";

    public GetViewAction() {
        super(NAME, GetViewAction.Response::new);
    }

    /** Request for get view */
    @ExperimentalApi
    public static class Request extends ClusterManagerNodeRequest<Request> {
        private final String name;

        public Request(final String name) {
            this.name = name;
        }

        public Request(final StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Request that = (Request) o;
            return name.equals(that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.isNullOrEmpty(name)) {
                validationException = ValidateActions.addValidationError("name cannot be empty or null", validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "get_view_request",
            args -> new Request((String) args[0])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), View.NAME_FIELD);
        }

        public static Request fromXContent(final XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    /** Response with a view */
    @ExperimentalApi
    public static class Response extends ActionResponse implements ToXContentObject {

        private final View view;

        public Response(final View view) {
            this.view = view;
        }

        public Response(final StreamInput in) throws IOException {
            super(in);
            this.view = new View(in);
        }

        public View getView() {
            return view;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Response that = (Response) o;
            return getView().equals(that.getView());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getView());
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            this.view.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            builder.field("view", view);
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<Response, Void> PARSER = new ConstructingObjectParser<>(
            "view_response",
            args -> new Response((View) args[0])
        );
        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), View.PARSER, new ParseField("view"));
        }

        public static Response fromXContent(final XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    /**
     * Transport Action for getting a View
     */
    public static class TransportAction extends TransportClusterManagerNodeAction<Request, Response> {

        private final ViewService viewService;

        @Inject
        public TransportAction(
            final TransportService transportService,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final ActionFilters actionFilters,
            final IndexNameExpressionResolver indexNameExpressionResolver,
            final ViewService viewService
        ) {
            super(NAME, transportService, clusterService, threadPool, actionFilters, Request::new, indexNameExpressionResolver);
            this.viewService = viewService;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.MANAGEMENT;
        }

        @Override
        protected Response read(final StreamInput in) throws IOException {
            return new Response(in);
        }

        @Override
        protected void clusterManagerOperation(final Request request, final ClusterState state, final ActionListener<Response> listener)
            throws Exception {
            viewService.getView(request, listener);
        }

        @Override
        protected ClusterBlockException checkBlock(final Request request, final ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        }
    }
}
