/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.weighted;

import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.rest.RestStatus;

import java.io.IOException;

public class WeightedRoutingVersionMismatchException extends OpenSearchException {

    public WeightedRoutingVersionMismatchException() {
        super("");
    }

    public WeightedRoutingVersionMismatchException(Throwable cause) {
        super(cause);
    }

    public WeightedRoutingVersionMismatchException(String message) {
        super(message);
    }

    @Override
    public RestStatus status() {
        return RestStatus.CONFLICT;
    }

    public WeightedRoutingVersionMismatchException(StreamInput in) throws IOException {
        super(in);
    }
}
