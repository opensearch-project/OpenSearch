/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.pool;

public class AlreadyClosedException extends IllegalStateException {
    public AlreadyClosedException(String message) {
        super(message);
    }

    public AlreadyClosedException(String message, Throwable cause) {
        super(message, cause);
    }
}
