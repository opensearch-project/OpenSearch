/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.deployment;

import org.opensearch.cluster.deployment.Deployment;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Response containing deployment status information.
 *
 * @opensearch.internal
 */
public class GetDeploymentResponse extends ActionResponse implements ToXContentObject {

    private final Map<String, Deployment> deployments;

    public GetDeploymentResponse(Map<String, Deployment> deployments) {
        this.deployments = Collections.unmodifiableMap(deployments);
    }

    public GetDeploymentResponse(StreamInput in) throws IOException {
        int size = in.readVInt();
        Map<String, Deployment> deployments = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String id = in.readString();
            Deployment deployment = new Deployment(in);
            deployments.put(id, deployment);
        }
        this.deployments = Collections.unmodifiableMap(deployments);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(deployments.size());
        for (Map.Entry<String, Deployment> entry : deployments.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    public Map<String, Deployment> getDeployments() {
        return deployments;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject("deployments");
        for (Map.Entry<String, Deployment> entry : deployments.entrySet()) {
            builder.field(entry.getKey());
            entry.getValue().toXContent(builder, params);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }
}
