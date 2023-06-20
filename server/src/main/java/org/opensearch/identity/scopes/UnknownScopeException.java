/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.scopes;

import java.io.IOException;
import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.rest.RestStatus;

public class UnknownScopeException extends OpenSearchException {

    public UnknownScopeException(String scope) {
        super("Failed to find scope: " + scope);
    }

    public UnknownScopeException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnknownScopeException(StreamInput in) throws IOException {
        super(in);
    }

    public UnknownScopeException(String msg, Object... args) {
        super(msg, args);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
