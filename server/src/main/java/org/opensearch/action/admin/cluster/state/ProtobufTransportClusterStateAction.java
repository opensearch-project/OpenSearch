/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.state;

import com.google.protobuf.CodedInputStream;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ProtobufActionFilters;
import org.opensearch.action.support.clustermanager.ProtobufTransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ProtobufClusterState;
import org.opensearch.cluster.ProtobufClusterStateObserver;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.ProtobufIndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.Metadata.Custom;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.node.NodeClosedException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ProtobufTransportService;

import java.io.IOException;
import java.util.function.Predicate;
import java.util.Map;

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
        ProtobufTransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ProtobufActionFilters actionFilters,
        ProtobufIndexNameExpressionResolver indexNameExpressionResolver
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
    protected ProtobufClusterStateResponse read(CodedInputStream in) throws IOException {
        return new ProtobufClusterStateResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(ProtobufClusterStateRequest request, ProtobufClusterState state) {
        // cluster state calls are done also on a fully blocked cluster to figure out what is going
        // on in the cluster. For example, which nodes have joined yet the recovery has not yet kicked
        // in, we need to make sure we allow those calls
        // return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
        return null;
    }

    @Override
    protected void clusterManagerOperation(
        final ProtobufClusterStateRequest request,
        final ProtobufClusterState state,
        final ActionListener<ProtobufClusterStateResponse> listener
    ) throws IOException {

        final Predicate<ProtobufClusterState> acceptableClusterStatePredicate = request.waitForMetadataVersion() == null
            ? clusterState -> true
            : clusterState -> clusterState.metadata().version() >= request.waitForMetadataVersion();

        final Predicate<ProtobufClusterState> acceptableClusterStateOrNotMasterPredicate = request.local()
            ? acceptableClusterStatePredicate
            : acceptableClusterStatePredicate.or(clusterState -> clusterState.nodes().isLocalNodeElectedClusterManager() == false);

        if (acceptableClusterStatePredicate.test(state)) {
            ActionListener.completeWith(listener, () -> buildResponse(request, state));
        } else {
            assert acceptableClusterStateOrNotMasterPredicate.test(state) == false;
            new ProtobufClusterStateObserver(state, clusterService, request.waitForTimeout(), logger, threadPool.getThreadContext())
                .waitForNextChange(new ProtobufClusterStateObserver.Listener() {

                    @Override
                    public void onNewClusterState(ProtobufClusterState newState) {
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
                            listener.onResponse(new ProtobufClusterStateResponse(state.getClusterName(), null, true));
                        } catch (Exception e) {
                            listener.onFailure(e);
                        }
                    }
                }, acceptableClusterStateOrNotMasterPredicate);
        }
    }

    private ProtobufClusterStateResponse buildResponse(final ProtobufClusterStateRequest request, final ProtobufClusterState currentState) {
        logger.trace("Serving cluster state request using version {}", currentState.version());
        ProtobufClusterState.Builder builder = ProtobufClusterState.builder(currentState.getClusterName());
        builder.version(currentState.version());
        builder.stateUUID(currentState.stateUUID());

        if (request.nodes()) {
            builder.nodes(currentState.nodes());
        }
        if (request.routingTable()) {
            if (request.indices().length > 0) {
                RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
                String[] indices = indexNameExpressionResolver.concreteIndexNames(currentState, request);
                for (String filteredIndex : indices) {
                    if (currentState.routingTable().getIndicesRouting().containsKey(filteredIndex)) {
                        routingTableBuilder.add(currentState.routingTable().getIndicesRouting().get(filteredIndex));
                    }
                }
                builder.routingTable(routingTableBuilder.build());
            } else {
                builder.routingTable(currentState.routingTable());
            }
        }
        if (request.blocks()) {
            builder.blocks(currentState.blocks());
        }

        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.clusterUUID(currentState.metadata().clusterUUID());
        mdBuilder.coordinationMetadata(currentState.coordinationMetadata());

        if (request.metadata()) {
            if (request.indices().length > 0) {
                mdBuilder.version(currentState.metadata().version());
                String[] indices = indexNameExpressionResolver.concreteIndexNames(currentState, request);
                for (String filteredIndex : indices) {
                    IndexMetadata indexMetadata = currentState.metadata().index(filteredIndex);
                    if (indexMetadata != null) {
                        mdBuilder.put(indexMetadata, false);
                    }
                }
            } else {
                mdBuilder = Metadata.builder(currentState.metadata());
            }

            // filter out metadata that shouldn't be returned by the API
            for (final Map.Entry<String, Custom> custom : currentState.metadata().customs().entrySet()) {
                if (custom.getValue().context().contains(Metadata.XContentContext.API) == false) {
                    mdBuilder.removeCustom(custom.getKey());
                }
            }
        }
        builder.metadata(mdBuilder);

        if (request.customs()) {
            for (ObjectObjectCursor<String, ProtobufClusterState.Custom> custom : currentState.customs()) {
                if (custom.value.isPrivate() == false) {
                    builder.putCustom(custom.key, custom.value);
                }
            }
        }

        return new ProtobufClusterStateResponse(currentState.getClusterName(), builder.build(), false);
    }

}
