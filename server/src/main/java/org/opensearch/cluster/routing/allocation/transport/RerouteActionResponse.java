/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.transport;

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import java.io.IOException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public  class RerouteActionResponse extends ActionResponse {

    public final ClusterState state;

    public RerouteActionResponse(ClusterState state) {this.state = state;}

    public RerouteActionResponse(StreamInput in) throws IOException {
        this.state = ClusterState.readFrom(in, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        state.writeTo(out);
    }

}
