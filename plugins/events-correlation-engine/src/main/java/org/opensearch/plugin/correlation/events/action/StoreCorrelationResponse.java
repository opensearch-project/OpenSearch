/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.events.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;

import java.io.IOException;

/**
 * Transport Response to store correlations
 *
 * @opensearch.internal
 */
public class StoreCorrelationResponse extends ActionResponse {

    private RestStatus status;

    /**
     * Parameterized ctor of StoreCorrelationResponse
     * @param status REST status of the request
     */
    public StoreCorrelationResponse(RestStatus status) {
        super();
        this.status = status;
    }

    /**
     * StreamInput ctor of StoreCorrelationResponse
     * @param sin StreamInput
     * @throws IOException IOException
     */
    public StoreCorrelationResponse(StreamInput sin) throws IOException {
        this(sin.readEnum(RestStatus.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(status);
    }

    /**
     * get REST status of request
     * @return REST status of request
     */
    public RestStatus getStatus() {
        return status;
    }
}
