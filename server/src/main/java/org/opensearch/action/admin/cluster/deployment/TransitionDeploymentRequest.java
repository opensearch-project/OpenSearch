/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.deployment;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.opensearch.cluster.deployment.DeploymentState;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Request to transition a deployment to a new state (drain or finish).
 *
 * @opensearch.internal
 */
public class TransitionDeploymentRequest extends AcknowledgedRequest<TransitionDeploymentRequest> {

    private String deploymentId;
    private DeploymentState targetState;
    private Map<String, String> attributes;

    public TransitionDeploymentRequest() {
        super();
        this.attributes = Collections.emptyMap();
    }

    public TransitionDeploymentRequest(StreamInput in) throws IOException {
        super(in);
        this.deploymentId = in.readString();
        this.targetState = DeploymentState.readFrom(in);
        this.attributes = in.readMap(StreamInput::readString, StreamInput::readString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(deploymentId);
        targetState.writeTo(out);
        out.writeMap(attributes, StreamOutput::writeString, StreamOutput::writeString);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (deploymentId == null || deploymentId.isEmpty()) {
            validationException = addValidationError("deployment ID is required", validationException);
        }
        if (targetState == null) {
            validationException = addValidationError("target state is required", validationException);
        }
        if (targetState == DeploymentState.DRAIN && (attributes == null || attributes.isEmpty())) {
            validationException = addValidationError("attributes are required for DRAIN state", validationException);
        }
        return validationException;
    }

    public String deploymentId() {
        return deploymentId;
    }

    public TransitionDeploymentRequest deploymentId(String deploymentId) {
        this.deploymentId = deploymentId;
        return this;
    }

    public DeploymentState targetState() {
        return targetState;
    }

    public TransitionDeploymentRequest targetState(DeploymentState targetState) {
        this.targetState = targetState;
        return this;
    }

    public Map<String, String> attributes() {
        return attributes;
    }

    public TransitionDeploymentRequest attributes(Map<String, String> attributes) {
        this.attributes = attributes != null ? attributes : Collections.emptyMap();
        return this;
    }
}
