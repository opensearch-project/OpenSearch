/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.state;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ProtobufActionFilters;
import org.opensearch.action.support.clustermanager.ProtobufTransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.node.NodeClosedException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.function.Predicate;

/**
 * Transport action for obtaining cluster state
 *
 * @opensearch.internal
 */
public class ProtobufTransportClusterStateAction extends ProtobufTransportClusterManagerNodeReadAction<
    ProtobufClusterStateRequest,
    ProtobufClusterStateResponse> {

    private final Logger logger = LogManager.getLogger(getClass());

    static {
        final String property = System.getProperty("opensearch.cluster_state.size");
        if (property != null) {
            throw new IllegalArgumentException("opensearch.cluster_state.size is no longer respected but was [" + property + "]");
        }
    }

    @Inject
    public ProtobufTransportClusterStateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ProtobufActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterStateAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ProtobufClusterStateRequest::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        // very lightweight operation in memory, no need to fork to a thread
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(ProtobufClusterStateRequest request, ClusterState state) {
        // cluster state calls are done also on a fully blocked cluster to figure out what is going
        // on in the cluster. For example, which nodes have joined yet the recovery has not yet kicked
        // in, we need to make sure we allow those calls
        // return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
        return null;
    }

    @Override
    protected void clusterManagerOperation(
        final ProtobufClusterStateRequest request,
        final ClusterState state,
        final ActionListener<ProtobufClusterStateResponse> listener
    ) throws IOException {
        final Predicate<ClusterState> acceptableClusterStatePredicate = request.waitForMetadataVersion() == null
            ? clusterState -> true
            : clusterState -> clusterState.metadata().version() >= request.waitForMetadataVersion();

        final Predicate<ClusterState> acceptableClusterStateOrNotMasterPredicate = request.local()
            ? acceptableClusterStatePredicate
            : acceptableClusterStatePredicate.or(clusterState -> clusterState.nodes().isLocalNodeElectedClusterManager() == false);

        if (acceptableClusterStatePredicate.test(state)) {
            ActionListener.completeWith(listener, () -> buildResponse(request, state));
        } else {
            assert acceptableClusterStateOrNotMasterPredicate.test(state) == false;
            new ClusterStateObserver(state, clusterService, request.waitForTimeout(), logger, threadPool.getThreadContext())
                .waitForNextChange(new ClusterStateObserver.Listener() {

                    @Override
                    public void onNewClusterState(ClusterState newState) {
                        if (acceptableClusterStatePredicate.test(newState)) {
                            ActionListener.completeWith(listener, () -> buildResponse(request, newState));
                        } else {
                            listener.onFailure(
                                new NotClusterManagerException(
                                    "cluster-manager stepped down waiting for metadata version " + request.waitForMetadataVersion()
                                )
                            );
                        }
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(new NodeClosedException(clusterService.localNode()));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        try {
                            listener.onResponse(new ProtobufClusterStateResponse(state.getClusterName().value(), null, 1, null, true));
                        } catch (Exception e) {
                            listener.onFailure(e);
                        }
                    }
                }, acceptableClusterStateOrNotMasterPredicate);
        }
    }

    private ProtobufClusterStateResponse buildResponse(final ProtobufClusterStateRequest request, final ClusterState currentState) {
        logger.trace("Serving cluster state request using version {}", currentState.version());
        return new ProtobufClusterStateResponse(
            currentState.getClusterName().value(),
            currentState.nodes(),
            currentState.version(),
            currentState.stateUUID(),
            false
        );
    }

    @Override
    protected ProtobufClusterStateResponse read(byte[] in) throws IOException {
        return new ProtobufClusterStateResponse(in);
    }

}
