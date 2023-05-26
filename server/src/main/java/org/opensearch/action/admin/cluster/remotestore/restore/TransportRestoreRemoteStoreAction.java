/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.restore;

import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreClusterStateListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.snapshots.RestoreService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for restore remote store operation
 *
 * @opensearch.internal
 */
public final class TransportRestoreRemoteStoreAction extends TransportClusterManagerNodeAction<
    RestoreRemoteStoreRequest,
    RestoreRemoteStoreResponse> {
    private final RestoreService restoreService;

    @Inject
    public TransportRestoreRemoteStoreAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        RestoreService restoreService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            RestoreRemoteStoreAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            RestoreRemoteStoreRequest::new,
            indexNameExpressionResolver
        );
        this.restoreService = restoreService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected RestoreRemoteStoreResponse read(StreamInput in) throws IOException {
        return new RestoreRemoteStoreResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(RestoreRemoteStoreRequest request, ClusterState state) {
        // Restoring a remote store might change the global state and create/change an index,
        // so we need to check for METADATA_WRITE and WRITE blocks
        ClusterBlockException blockException = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        if (blockException != null) {
            return blockException;
        }
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);

    }

    @Override
    protected void clusterManagerOperation(
        final RestoreRemoteStoreRequest request,
        final ClusterState state,
        final ActionListener<RestoreRemoteStoreResponse> listener
    ) {
        restoreService.restoreFromRemoteStore(
            request,
            ActionListener.delegateFailure(listener, (delegatedListener, restoreCompletionResponse) -> {
                if (restoreCompletionResponse.getRestoreInfo() == null && request.waitForCompletion()) {
                    RestoreClusterStateListener.createAndRegisterListener(
                        clusterService,
                        restoreCompletionResponse,
                        delegatedListener,
                        RestoreRemoteStoreResponse::new
                    );
                } else {
                    delegatedListener.onResponse(new RestoreRemoteStoreResponse(restoreCompletionResponse.getRestoreInfo()));
                }
            })
        );
    }
}
