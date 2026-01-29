/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.OpenSearchException;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Exception thrown when a version conflict occurs during remote state operations.
 * This indicates optimistic concurrency control failure.
 *
 * @opensearch.internal
 */
public class RemoteStateVersionConflictException extends OpenSearchException {

    public RemoteStateVersionConflictException(String msg) {
        super(msg);
    }

    public RemoteStateVersionConflictException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public RemoteStateVersionConflictException(StreamInput in) throws IOException {
        super(in);
    }
}
