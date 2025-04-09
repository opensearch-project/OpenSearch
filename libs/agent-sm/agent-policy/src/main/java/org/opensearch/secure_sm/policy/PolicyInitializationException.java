/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.secure_sm.policy;

/**
 * Custom exception for failures during policy file parsing,
 */
public class PolicyInitializationException extends Exception {

    public PolicyInitializationException(String message) {
        super(message);
    }

    public PolicyInitializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public PolicyInitializationException(Throwable cause) {
        super(cause);
    }
}
