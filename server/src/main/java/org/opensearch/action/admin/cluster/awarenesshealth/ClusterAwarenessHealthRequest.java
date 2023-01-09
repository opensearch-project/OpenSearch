/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.awarenesshealth;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Transport request for requesting cluster Awareness health
 *
 * @opensearch.internal
 */
public class ClusterAwarenessHealthRequest extends ClusterManagerNodeReadRequest<ClusterAwarenessHealthRequest> {

    private String awarenessAttributeName;

    public ClusterAwarenessHealthRequest() {}

    public ClusterAwarenessHealthRequest(StreamInput in) throws IOException {
        super(in);
        awarenessAttributeName = in.readString();
    }

    public String getAttributeName() {
        return awarenessAttributeName;
    }

    public void setAwarenessAttributeName(String awarenessAttributeName) {
        this.awarenessAttributeName = awarenessAttributeName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(awarenessAttributeName);
    }

    @Override
    public ActionRequestValidationException validate() {

        if (awarenessAttributeName == null || awarenessAttributeName.isBlank()) {
            return addValidationError("awareness attribute value seems to be missing", null);
        }
        return null;
    }
}
