/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.deployment;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.deployment.DeploymentManagerService;
import org.opensearch.cluster.deployment.DeploymentState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for transitioning a deployment (drain/finish).
 *
 * @opensearch.internal
 */
public class TransportTransitionDeploymentAction extends TransportClusterManagerNodeAction<
    TransitionDeploymentRequest,
    AcknowledgedResponse> {

    private final DeploymentManagerService deploymentManagerService;

    @Inject
    public TransportTransitionDeploymentAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DeploymentManagerService deploymentManagerService
    ) {
        super(
            TransitionDeploymentAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            TransitionDeploymentRequest::new,
            indexNameExpressionResolver
        );
        this.deploymentManagerService = deploymentManagerService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        TransitionDeploymentRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        if (request.targetState() == DeploymentState.DRAIN) {
            deploymentManagerService.startDeployment(request.deploymentId(), request.attributes(), request, listener);
        } else {
            deploymentManagerService.finishDeployment(request.deploymentId(), request, listener);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(TransitionDeploymentRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
