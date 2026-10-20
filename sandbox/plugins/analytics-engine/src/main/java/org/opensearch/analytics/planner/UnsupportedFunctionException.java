/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.OpenSearchException;
import org.opensearch.core.rest.RestStatus;

/**
 * Thrown when a query uses a function that is not currently supported by the analytics engine.
 * Maps to HTTP 400 (Bad Request) since this is a user error, not a server error.
 */
public class UnsupportedFunctionException extends OpenSearchException {

    public UnsupportedFunctionException(String functionName, String context) {
        super("Function [" + functionName + "] is not currently supported" + (context != null ? " " + context : ""));
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
