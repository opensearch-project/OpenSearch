/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.utils;

public enum ErrorType {
    BODY_NOT_PARSEABLE("Failed to parse body for request: "),
    IDENTITY_NOT_INITIALIZED("Identity index not initialized");

    private String message;

    private ErrorType(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
