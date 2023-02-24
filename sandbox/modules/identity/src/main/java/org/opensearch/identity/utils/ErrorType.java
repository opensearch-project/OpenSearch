/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.utils;

public enum ErrorType {

    BODY_NOT_PARSEABLE("Failed to parse body for user request: "),
    IDENTITY_NOT_INITIALIZED("Identity index not initialized"),
    HASH_OR_PASSWORD_MISSING("Please specify either 'hash' or 'password' when creating a new internal user."),
    RESTRICTED_CHARS_IN_USERNAME("Username has unpermitted restricted characters: "),
    RESOURCE_NOT_FOUND_SUFFIX(" doesn't exist."),
    NEWPASSWORD_MISMATCHING("New passwords do not match."),
    OLDPASSWORD_MISMATCHING("Old passwords do not match."),
    USER_NOT_EXISTING("Failed to reset password, because target user does not exist."),
    NEWPASSWORD_MATCHING_OLDPASSWORD("New password is same as the current password, please create another new password.");

    private String message;

    private ErrorType(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
