/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch;

import org.opensearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Base exception for a missing action on a primary
 *
 * @opensearch.internal
 */
public class InvalidArgumentException extends OpenSearchException {
    public InvalidArgumentException(String message) {
        super(message);
    }

    public InvalidArgumentException(StreamInput in) throws IOException {
        super(in);
    }
}
