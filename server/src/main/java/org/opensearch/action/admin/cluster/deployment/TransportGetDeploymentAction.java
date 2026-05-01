/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.deployment;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.deployment.Deployment;
import org.opensearch.cluster.deployment.DeploymentStateService;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Transport action for getting deployment status.
 *
 * @opensearch.internal
 */
public class TransportGetDeploymentAction extends TransportClusterManagerNodeReadAction<GetDeploymentRequest, GetDeploymentResponse> {

    private final DeploymentStateService deploymentStateService;

    @Inject
    public TransportGetDeploymentAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DeploymentStateService deploymentStateService
    ) {
        super(
            GetDeploymentAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetDeploymentRequest::new,
            indexNameExpressionResolver,
            true
        );
        this.deploymentStateService = deploymentStateService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetDeploymentResponse read(StreamInput in) throws IOException {
        return new GetDeploymentResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        GetDeploymentRequest request,
        ClusterState state,
        ActionListener<GetDeploymentResponse> listener
    ) {
        if (request.deploymentId() != null) {
            Deployment deployment = deploymentStateService.getDeployment(request.deploymentId());
            if (deployment != null) {
                listener.onResponse(new GetDeploymentResponse(Collections.singletonMap(request.deploymentId(), deployment)));
            } else {
                listener.onResponse(new GetDeploymentResponse(Collections.emptyMap()));
            }
        } else {
            Map<String, Deployment> deployments = deploymentStateService.getAllDeployments();
            listener.onResponse(new GetDeploymentResponse(deployments));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(GetDeploymentRequest request, ClusterState state) {
        return null;
    }
}
