/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.cache;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for pruning file cache.
 * Extends ActionRequest for direct node execution.
 *
 * @opensearch.internal
 */
public class PruneCacheRequest extends ActionRequest {

    private static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(30);
    private TimeValue timeout = DEFAULT_TIMEOUT;

    /**
     * Default constructor
     */
    public PruneCacheRequest() {}

    /**
     * Constructor for stream input
     *
     * @param in the stream input
     * @throws IOException if an I/O exception occurs
     */
    public PruneCacheRequest(StreamInput in) throws IOException {
        super(in);
        timeout = in.readTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeTimeValue(timeout);
    }

    /**
     * Sets the timeout for this request.
     * Note: Currently preserved for API compatibility but not actively enforced
     * in HandledTransportAction implementation.
     *
     * @param timeout operation timeout (passed through but not enforced by transport layer)
     * @return this request
     */
    public PruneCacheRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * Gets the timeout for this request.
     * Note: Currently preserved for API compatibility but not actively enforced
     * in HandledTransportAction implementation.
     *
     * @return operation timeout (passed through but not enforced by transport layer)
     */
    public TimeValue timeout() {
        return timeout;
    }

    @Override
    public ActionRequestValidationException validate() {
        // No validation needed for this stateless request
        return null;
    }
}
