/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.view;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/** Action to update a view */
@ExperimentalApi
public class UpdateViewAction extends ActionType<GetViewAction.Response> {

    public static final UpdateViewAction INSTANCE = new UpdateViewAction();
    public static final String NAME = "cluster:admin/views/update";

    public UpdateViewAction() {
        super(NAME, GetViewAction.Response::new);
    }

    /**
     * Transport Action for updating a View
     */
    public static class TransportAction extends TransportClusterManagerNodeAction<CreateViewAction.Request, GetViewAction.Response> {

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
            super(NAME, transportService, clusterService, threadPool, actionFilters, CreateViewAction.Request::new, indexNameExpressionResolver);
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
        protected void clusterManagerOperation(final CreateViewAction.Request request, final ClusterState state, final ActionListener<GetViewAction.Response> listener)
            throws Exception {
            viewService.updateView(request, listener);
        }

        @Override
        protected ClusterBlockException checkBlock(final CreateViewAction.Request request, final ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }

}
