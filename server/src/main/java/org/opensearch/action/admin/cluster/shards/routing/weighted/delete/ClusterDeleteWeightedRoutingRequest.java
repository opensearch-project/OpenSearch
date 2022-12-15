/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.weighted.delete;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Request to delete weights for weighted round-robin shard routing policy.
 *
 * @opensearch.internal
 */
public class ClusterDeleteWeightedRoutingRequest extends ClusterManagerNodeRequest<ClusterDeleteWeightedRoutingRequest> {
    String awarenessAttribute;

    public String getAwarenessAttribute() {
        return awarenessAttribute;
    }

    public void setAwarenessAttribute(String awarenessAttribute) {
        this.awarenessAttribute = awarenessAttribute;
    }

    public ClusterDeleteWeightedRoutingRequest(String awarenessAttribute) {
        this.awarenessAttribute = awarenessAttribute;
    }

    public ClusterDeleteWeightedRoutingRequest() {}

    public ClusterDeleteWeightedRoutingRequest(StreamInput in) throws IOException {
        super(in);
        awarenessAttribute = in.readString();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (awarenessAttribute == null || awarenessAttribute.isEmpty()) {
            validationException = addValidationError("Awareness attribute is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(awarenessAttribute);
    }

    @Override
    public String toString() {
        return "ClusterDeleteWeightedRoutingRequest";
    }
}
