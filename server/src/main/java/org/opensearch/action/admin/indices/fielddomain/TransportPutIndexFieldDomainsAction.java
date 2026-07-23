/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.fielddomain;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportIndicesResolvingAction;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MetadataIndexFieldDomainService;
import org.opensearch.cluster.metadata.OptionallyResolvedIndices;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for publishing field-domain metadata to one concrete index.
 */
public class TransportPutIndexFieldDomainsAction extends TransportClusterManagerNodeAction<
    PutIndexFieldDomainsRequest,
    AcknowledgedResponse> implements TransportIndicesResolvingAction<PutIndexFieldDomainsRequest> {
    private static final Logger logger = LogManager.getLogger(TransportPutIndexFieldDomainsAction.class);

    private final MetadataIndexFieldDomainService fieldDomainService;

    @Inject
    public TransportPutIndexFieldDomainsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataIndexFieldDomainService fieldDomainService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            PutIndexFieldDomainsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutIndexFieldDomainsRequest::new,
            indexNameExpressionResolver
        );
        this.fieldDomainService = fieldDomainService;
    }

    @Override
    protected String executor() {
        // The transport action only submits a cluster state update task.
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(PutIndexFieldDomainsRequest request, ClusterState state) {
        ClusterBlockException globalBlock = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        if (globalBlock != null) {
            return globalBlock;
        }
        if (request.targetIndex() == null) {
            return null;
        }
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void clusterManagerOperation(
        final PutIndexFieldDomainsRequest request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        PutIndexFieldDomainsClusterStateUpdateRequest clusterStateUpdateRequest = new PutIndexFieldDomainsClusterStateUpdateRequest()
            .targetIndex(request.targetIndex())
            .fieldDomainCustomData(request.fieldDomainCustomData())
            .ackTimeout(request.timeout())
            .clusterManagerNodeTimeout(request.clusterManagerNodeTimeout());

        fieldDomainService.putFieldDomains(clusterStateUpdateRequest, new ActionListener<>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                listener.onResponse(new AcknowledgedResponse(response.isAcknowledged()));
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug(() -> new ParameterizedMessage("failed to publish field domains for index [{}]", request.targetIndex()), e);
                listener.onFailure(e);
            }
        });
    }

    @Override
    public OptionallyResolvedIndices resolveIndices(PutIndexFieldDomainsRequest request) {
        if (request.targetIndex() == null) {
            return ResolvedIndices.unknown();
        }
        return ResolvedIndices.of(request.targetIndex());
    }
}
