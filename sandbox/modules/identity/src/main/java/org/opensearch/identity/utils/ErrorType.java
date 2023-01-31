/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.utils;

public enum ErrorType {
    NONE("ok"),
    INVALID_CONFIGURATION("Invalid configuration"),
    INVALID_PASSWORD("Invalid password"),
    WRONG_DATATYPE("Wrong datatype"),
    BODY_NOT_PARSEABLE("Could not parse content of request."),
    PAYLOAD_NOT_ALLOWED("Request body not allowed for this action."),
    PAYLOAD_MANDATORY("Request body required for this action."),
    IDENTITY_NOT_INITIALIZED("Identity index not initialized"),
    NULL_ARRAY_ELEMENT("`null` is not allowed as json array element"),
    HASH_OR_PASSWORD_MISSING("Please specify either 'hash' or 'password' when creating a new internal user."),
    RESTRICTED_CHARS_IN_USERNAME("Username has unpermitted restricted characters :");

    private String message;

    private ErrorType(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
