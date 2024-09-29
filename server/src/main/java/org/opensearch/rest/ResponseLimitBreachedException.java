/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.opensearch.OpenSearchException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;

import java.io.IOException;

/**
 * Thrown when api response breaches threshold limit.
 *
 * @opensearch.internal
 */
public class ResponseLimitBreachedException extends OpenSearchException {

    public ResponseLimitBreachedException(String msg) {
        super(msg);
    }

    public ResponseLimitBreachedException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.TOO_MANY_REQUESTS;
    }
}
