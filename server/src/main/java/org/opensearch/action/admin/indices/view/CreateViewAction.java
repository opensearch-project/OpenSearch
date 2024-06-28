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
import org.opensearch.common.ValidationException;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Action to create a view */
@ExperimentalApi
public class CreateViewAction extends ActionType<GetViewAction.Response> {

    private static final int MAX_NAME_LENGTH = 64;
    private static final int MAX_DESCRIPTION_LENGTH = 256;
    private static final int MAX_TARGET_COUNT = 25;
    private static final int MAX_TARGET_INDEX_PATTERN_LENGTH = 64;

    public static final CreateViewAction INSTANCE = new CreateViewAction();
    public static final String NAME = "cluster:admin/views/create";

    private CreateViewAction() {
        super(NAME, GetViewAction.Response::new);
    }

    /**
     * Request for Creating View
     */
    @ExperimentalApi
    public static class Request extends ClusterManagerNodeRequest<Request> {
        private final String name;
        private final String description;
        private final List<Target> targets;

        public Request(final String name, final String description, final List<Target> targets) {
            this.name = name;
            this.description = Objects.requireNonNullElse(description, "");
            this.targets = targets;
        }

        public Request(final StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
            this.description = in.readString();
            this.targets = in.readList(Target::new);
        }

        public String getName() {
            return name;
        }

        public String getDescription() {
            return description;
        }

        public List<Target> getTargets() {
            return new ArrayList<>(targets);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request that = (Request) o;
            return name.equals(that.name) && description.equals(that.description) && targets.equals(that.targets);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, description, targets);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.isNullOrEmpty(name)) {
                validationException = ValidateActions.addValidationError("name cannot be empty or null", validationException);
            }
            if (name != null && name.length() > MAX_NAME_LENGTH) {
                validationException = ValidateActions.addValidationError(
                    "name must be less than " + MAX_NAME_LENGTH + " characters in length",
                    validationException
                );
            }
            if (description != null && description.length() > MAX_DESCRIPTION_LENGTH) {
                validationException = ValidateActions.addValidationError(
                    "description must be less than " + MAX_DESCRIPTION_LENGTH + " characters in length",
                    validationException
                );
            }
            if (CollectionUtils.isEmpty(targets)) {
                validationException = ValidateActions.addValidationError("targets cannot be empty", validationException);
            } else {
                if (targets.size() > MAX_TARGET_COUNT) {
                    validationException = ValidateActions.addValidationError(
                        "view cannot have more than " + MAX_TARGET_COUNT + " targets",
                        validationException
                    );
                }
                for (final Target target : targets) {
                    final var validationMessages = Optional.ofNullable(target.validate())
                        .map(ValidationException::validationErrors)
                        .orElse(List.of());
                    for (final String validationMessage : validationMessages) {
                        validationException = ValidateActions.addValidationError(validationMessage, validationException);
                    }
                }
            }

            return validationException;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            out.writeString(description);
            out.writeList(targets);
        }

        /** View target representation for create requests */
        @ExperimentalApi
        public static class Target implements Writeable {
            public final String indexPattern;

            public Target(final String indexPattern) {
                this.indexPattern = indexPattern;
            }

            public Target(final StreamInput in) throws IOException {
                this.indexPattern = in.readString();
            }

            public String getIndexPattern() {
                return indexPattern;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Target that = (Target) o;
                return indexPattern.equals(that.indexPattern);
            }

            @Override
            public int hashCode() {
                return Objects.hash(indexPattern);
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
                if (indexPattern != null && indexPattern.length() > MAX_TARGET_INDEX_PATTERN_LENGTH) {
                    validationException = ValidateActions.addValidationError(
                        "target index pattern must be less than " + MAX_TARGET_INDEX_PATTERN_LENGTH + " characters in length",
                        validationException
                    );
                }

                return validationException;
            }

            private static final ConstructingObjectParser<Target, Void> PARSER = new ConstructingObjectParser<>(
                "target",
                args -> new Target((String) args[0])
            );
            static {
                PARSER.declareString(ConstructingObjectParser.constructorArg(), View.Target.INDEX_PATTERN_FIELD);
            }

            public static Target fromXContent(final XContentParser parser) throws IOException {
                return PARSER.parse(parser, null);
            }
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "create_view_request",
            args -> new Request((String) args[0], (String) args[1], (List<Target>) args[2])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), View.NAME_FIELD);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), View.DESCRIPTION_FIELD);
            PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> Target.fromXContent(p), View.TARGETS_FIELD);
        }

        public static Request fromXContent(final XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    /**
     * Transport Action for creating a View
     */
    public static class TransportAction extends TransportClusterManagerNodeAction<Request, GetViewAction.Response> {

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
        protected GetViewAction.Response read(final StreamInput in) throws IOException {
            return new GetViewAction.Response(in);
        }

        @Override
        protected void clusterManagerOperation(
            final Request request,
            final ClusterState state,
            final ActionListener<GetViewAction.Response> listener
        ) throws Exception {
            viewService.createView(request, listener);
        }

        @Override
        protected ClusterBlockException checkBlock(final Request request, final ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }
}
