/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.identity;

import java.util.Base64;

/**
 * Util class to convert username to/from identifier
 * Helpful during stream writes and reads
 */
public class ExtensionIdentifierUtils {
    /**
     * Convert username to identifier
     * @param username input username to convert to id
     * @return uuid
     */
    public static String toIdentifier(String username) {
        return Base64.getEncoder().encodeToString(username.getBytes());
    }

    /**
     * Convert identifier to username
     * @param id identifier to converted to username
     * @return String
     */
    public static String toUsername(String id) {
        return new String(Base64.getDecoder().decode(id));
    }
}
