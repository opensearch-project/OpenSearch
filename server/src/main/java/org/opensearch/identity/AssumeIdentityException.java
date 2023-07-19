/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

/**
 * An exception thrown when the subject is not authorized for the given permission
 *
 * @opensearch.experimental
 */
public class AssumeIdentityException extends RuntimeException {
    public AssumeIdentityException(final String message) {
        super(message);
    }
}