/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.deployment;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request to get deployment status. If deploymentId is null, returns all deployments.
 *
 * @opensearch.internal
 */
public class GetDeploymentRequest extends ClusterManagerNodeReadRequest<GetDeploymentRequest> {

    private String deploymentId;

    public GetDeploymentRequest() {}

    public GetDeploymentRequest(String deploymentId) {
        this.deploymentId = deploymentId;
    }

    public GetDeploymentRequest(StreamInput in) throws IOException {
        super(in);
        this.deploymentId = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(deploymentId);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String deploymentId() {
        return deploymentId;
    }

    public GetDeploymentRequest deploymentId(String deploymentId) {
        this.deploymentId = deploymentId;
        return this;
    }
}
