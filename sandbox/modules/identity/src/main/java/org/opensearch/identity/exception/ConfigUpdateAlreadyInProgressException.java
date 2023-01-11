/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.exception;

import java.io.IOException;

import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.StreamInput;

public class ConfigUpdateAlreadyInProgressException extends OpenSearchException {

    public ConfigUpdateAlreadyInProgressException(StreamInput in) throws IOException {
        super(in);
    }

    public ConfigUpdateAlreadyInProgressException(String msg, Object... args) {
        super(msg, args);
    }

    public ConfigUpdateAlreadyInProgressException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }

    public ConfigUpdateAlreadyInProgressException(Throwable cause) {
        super(cause);
    }

}
