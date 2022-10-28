/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.weighted.delete;

import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Response from deleting weights for weighted round-robin search routing policy.
 *
 * @opensearch.internal
 */
public class ClusterDeleteWeightedRoutingResponse extends AcknowledgedResponse {

    ClusterDeleteWeightedRoutingResponse(StreamInput in) throws IOException {
        super(in);
    }

    public ClusterDeleteWeightedRoutingResponse(boolean acknowledged) {
        super(acknowledged);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

    }
}
