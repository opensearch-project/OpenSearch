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
 * Exception thrown when attempting to write a blob that already exists.
 * This indicates the blob was created by another process.
 *
 * @opensearch.internal
 */
public class RemoteStateBlobAlreadyExistsException extends OpenSearchException {

    public RemoteStateBlobAlreadyExistsException(String msg) {
        super(msg);
    }

    public RemoteStateBlobAlreadyExistsException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public RemoteStateBlobAlreadyExistsException(StreamInput in) throws IOException {
        super(in);
    }
}
