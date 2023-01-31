/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.exception;

import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.StreamInput;

import java.io.IOException;

public class InvalidUserNameException extends OpenSearchException {

    private static final long serialVersionUID = 1L;

    public InvalidUserNameException(StreamInput in) throws IOException {
        super(in);
    }

    public InvalidUserNameException(String msg, Object... args) {
        super(msg, args);
    }

    public InvalidUserNameException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }

    public InvalidUserNameException(Throwable cause) {
        super(cause);
    }
}
