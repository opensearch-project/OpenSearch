/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.transport;

import java.io.IOException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public  class RerouteActionRequest extends ActionRequest {

    public final ClusterState state;
    public final String reason;

    public RerouteActionRequest(ClusterState state, String reason) {this.state = state;
        this.reason = reason;
    }

    public RerouteActionRequest(StreamInput in) throws IOException {
        this.state = ClusterState.readFrom(in, null);
        this.reason =  in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        state.writeTo(out);
        out.writeString(reason);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
