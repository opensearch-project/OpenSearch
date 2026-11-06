/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.readonly;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.DestructiveOperations;
import org.opensearch.action.support.TransportIndicesResolvingAction;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MetadataIndexStateService;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;

/**
 * Removes a single index level block from a given set of indices. Unlike adding a block, removing one needs no verification on the
 * shards: a single cluster state update removes the cluster block and sets the matching {@code index.blocks.*} setting to
 * {@code false}, so that metadata and settings stay consistent. See {@link MetadataIndexStateService#removeIndexBlock} for the details.
 *
 * @opensearch.internal
 */
public class TransportRemoveIndexBlockAction extends TransportClusterManagerNodeAction<RemoveIndexBlockRequest, RemoveIndexBlockResponse>
    implements
        TransportIndicesResolvingAction<RemoveIndexBlockRequest> {

    private static final Logger logger = LogManager.getLogger(TransportRemoveIndexBlockAction.class);

    private final MetadataIndexStateService indexStateService;
    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportRemoveIndexBlockAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataIndexStateService indexStateService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DestructiveOperations destructiveOperations
    ) {
        super(
            RemoveIndexBlockAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            RemoveIndexBlockRequest::new,
            indexNameExpressionResolver
        );
        this.indexStateService = indexStateService;
        this.destructiveOperations = destructiveOperations;
    }

    @Override
    protected String executor() {
        // no need to use a thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected RemoveIndexBlockResponse read(StreamInput in) throws IOException {
        return new RemoveIndexBlockResponse(in);
    }

    @Override
    protected void doExecute(Task task, RemoveIndexBlockRequest request, ActionListener<RemoveIndexBlockResponse> listener) {
        // fail before the request is forwarded: an older cluster manager node does not know the action
        ensureAllNodesSupportRemoveIndexBlock(clusterService.state().nodes());
        destructiveOperations.failDestructive(request.indices());
        super.doExecute(task, request, listener);
    }

    /**
     * Fails the request unless every node in the cluster is on the version that introduced this action. The action is handled by the
     * elected cluster manager, which, on an older version, would answer with an obscure "action not found" transport error.
     */
    static void ensureAllNodesSupportRemoveIndexBlock(DiscoveryNodes nodes) {
        final Version minNodeVersion = nodes.getMinNodeVersion();
        if (minNodeVersion.before(Version.V_3_10_0)) {
            throw new IllegalArgumentException(
                "action ["
                    + RemoveIndexBlockAction.NAME
                    + "] requires all nodes in the cluster to be on version ["
                    + Version.V_3_10_0
                    + "] or later, but the oldest node is on version ["
                    + minNodeVersion
                    + "]"
            );
        }
    }

    /**
     * Removing a block has to work on an index that is blocked itself, otherwise an index with a metadata blocking block (for example
     * {@code read_only}) could never be unblocked, which is what makes clearing {@code read_only} and {@code write} through the settings
     * API fail. Only cluster level blocks stop the request.
     */
    @Override
    protected ClusterBlockException checkBlock(RemoveIndexBlockRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void clusterManagerOperation(
        final RemoveIndexBlockRequest request,
        final ClusterState state,
        final ActionListener<RemoveIndexBlockResponse> listener
    ) throws Exception {
        final Index[] concreteIndices = resolveIndices(state, request).concreteIndicesAsArray();
        if (concreteIndices == null || concreteIndices.length == 0) {
            listener.onResponse(new RemoveIndexBlockResponse(true, Collections.emptyList()));
            return;
        }

        final RemoveIndexBlockClusterStateUpdateRequest removeBlockRequest = new RemoveIndexBlockClusterStateUpdateRequest(
            request.getBlock()
        ).ackTimeout(request.timeout()).clusterManagerNodeTimeout(request.clusterManagerNodeTimeout()).indices(concreteIndices);
        indexStateService.removeIndexBlock(removeBlockRequest, ActionListener.delegateResponse(listener, (delegatedListener, t) -> {
            logger.debug(() -> new ParameterizedMessage("failed to remove block from indices [{}]", (Object) concreteIndices), t);
            delegatedListener.onFailure(t);
        }));
    }

    private ResolvedIndices.Local.Concrete resolveIndices(ClusterState state, RemoveIndexBlockRequest request) {
        return indexNameExpressionResolver.concreteResolvedIndices(state, request);
    }

    @Override
    public ResolvedIndices resolveIndices(RemoveIndexBlockRequest request) {
        return ResolvedIndices.of(resolveIndices(this.clusterService.state(), request));
    }
}
