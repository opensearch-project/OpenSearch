package org.opensearch.action.admin.indices.view;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.opensearch.cluster.metadata.ViewService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/** Action to create a view */
public class CreateViewAction extends ActionType<CreateViewAction.Response> {

    public static final CreateViewAction INSTANCE = new CreateViewAction();
    public static final String NAME = "cluster:views:create";

    private CreateViewAction() {
        super(NAME, CreateViewAction.Response::new);
    }


    /** View target representation for create requests */
    public static class ViewTarget implements Writeable {
        public final String indexPattern;

        public ViewTarget(final String indexPattern) {
            this.indexPattern = indexPattern;
        }

        public ViewTarget(final StreamInput in) throws IOException {
            this.indexPattern = in.readString();
        }

        public String getIndexPattern() {
            return indexPattern;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(indexPattern);
        }

        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(indexPattern)) {
                validationException = ValidateActions.addValidationError("index pattern cannot be empty or null", validationException);
            }

            return validationException;
        }

    }

    /**
     * Request for Creating View
     */
    public static class Request extends ClusterManagerNodeRequest<Request> {
        private final String name;
        private final String description;
        private final List<ViewTarget> targets;

        public Request(final String name, final String description, final List<ViewTarget> targets) {
            this.name = name;
            this.description = description;
            this.targets = targets;
        }

        public String getName() {
            return name;
        }

        public String getDescription() {
            return description;
        }

        public List<ViewTarget> getTargets() {
            return new ArrayList<>(targets);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.isNullOrEmpty(name)) {
                validationException = ValidateActions.addValidationError("Name is cannot be empty or null", validationException);
            }
            if (targets.isEmpty()) {
                validationException = ValidateActions.addValidationError("targets cannot be empty", validationException);
            }

            for (final ViewTarget target : targets) {
                validationException = target.validate();
            }

            return validationException;
        }

        public Request(final StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
            this.description = in.readString();
            this.targets = in.readList(ViewTarget::new);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            out.writeString(description);
            out.writeList(targets);
        }
    }

    /** Response after view is created */
    public static class Response extends ActionResponse {

        private final org.opensearch.cluster.metadata.View createdView; 

        public Response(final org.opensearch.cluster.metadata.View createdView) {
            this.createdView = createdView;
        }

        public Response(final StreamInput in) throws IOException {
            super(in);
            this.createdView = new org.opensearch.cluster.metadata.View(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            this.createdView.writeTo(out);
        }
    }

    /**
     * Transport Action for creating a View
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
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Response read(StreamInput in) throws IOException {
            return new Response(in);
        }

        @Override
        protected void clusterManagerOperation(Request request, ClusterState state, ActionListener<Response> listener)
            throws Exception {
            viewService.createView(request, listener);
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }
}
