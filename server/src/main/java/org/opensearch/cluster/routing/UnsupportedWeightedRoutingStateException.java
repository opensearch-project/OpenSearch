/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.rest.RestStatus;

import java.io.IOException;

/**
 * Thrown when failing to update the routing weight due to an unsupported state. See {@link WeightedRoutingService} for more details.
 *
 * @opensearch.internal
 */
public class UnsupportedWeightedRoutingStateException extends OpenSearchException {
    public UnsupportedWeightedRoutingStateException(StreamInput in) throws IOException {
        super(in);
    }

    public UnsupportedWeightedRoutingStateException(String msg, Object... args) {
        super(msg, args);
    }

    @Override
    public RestStatus status() {
        return RestStatus.CONFLICT;
    }
}
