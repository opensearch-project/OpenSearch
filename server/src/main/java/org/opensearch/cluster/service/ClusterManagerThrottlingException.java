/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.OpenSearchException;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Exception raised from cluster manager node due to task throttling.
 */
public class ClusterManagerThrottlingException extends OpenSearchException {

    public ClusterManagerThrottlingException(String msg, Object... args) {
        super(msg, args);
    }

    public ClusterManagerThrottlingException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Throwable fillInStackTrace() {
        // This is on the hot path; stack traces are expensive to compute and not very useful for this exception, so don't fill it.
        return this;
    }
}
