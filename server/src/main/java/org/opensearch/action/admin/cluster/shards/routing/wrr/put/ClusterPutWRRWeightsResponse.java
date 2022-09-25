/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.wrr.put;

import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.xcontent.ToXContentObject;

import java.io.IOException;

/**
 * Response from updating weights for weighted round-robin search routing policy.
 *
 * @opensearch.internal
 */
public class ClusterPutWRRWeightsResponse extends AcknowledgedResponse implements ToXContentObject {
    public ClusterPutWRRWeightsResponse(boolean acknowledged) {
        super(acknowledged);
    }

    public ClusterPutWRRWeightsResponse(StreamInput in) throws IOException {
        super(in);
    }
}
