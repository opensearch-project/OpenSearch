/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.azure;

import java.util.Arrays;

// Type of token credentials that the plugin supports
public enum TokenCredentialType {
    MANAGED_IDENTITY("managed");

    private final String type;

    TokenCredentialType(String type) {
        this.type = type;
    }

    public static String[] getTokenCredentialTypes() {
        return Arrays.stream(TokenCredentialType.values()).map(tokenCredentialType -> tokenCredentialType.type).toArray(String[]::new);
    }

    static TokenCredentialType valueOfType(String type) {
        for (TokenCredentialType value : values()) {
            if (value.type.equalsIgnoreCase(type) || value.name().equalsIgnoreCase(type)) {
                return value;
            }
        }
        throw new IllegalArgumentException(
            "The token credential type '"
                + type
                + "' is unsupported, please use one of the following values: "
                + String.join(", ", getTokenCredentialTypes())
        );
    }
}
