/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Exception thrown if replication fails that may be retried.
 *
 * @opensearch.internal
 */
public class RetryableReplicationException extends OpenSearchException {

    public RetryableReplicationException(String msg) {
        super(msg);
    }

    public RetryableReplicationException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public RetryableReplicationException(StreamInput in) throws IOException {
        super(in);
    }

}
