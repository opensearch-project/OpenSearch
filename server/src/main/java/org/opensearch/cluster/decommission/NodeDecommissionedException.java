/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission;

import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.rest.RestStatus;

import java.io.IOException;

/**
 * This exception is thrown if the node is decommissioned by @{@link DecommissionService}
 * and this nodes needs to be removed from the cluster
 *
 * @opensearch.internal
 */
public class NodeDecommissionedException extends OpenSearchException {

    public NodeDecommissionedException(String msg, Object... args) {
        super(msg, args);
    }

    public NodeDecommissionedException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.FAILED_DEPENDENCY;
    }
}
