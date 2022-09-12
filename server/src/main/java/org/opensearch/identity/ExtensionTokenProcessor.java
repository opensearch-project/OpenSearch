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

/**
 * Token processor class to handle token encryption/decryption
 */
public class ExtensionTokenProcessor {
    private String extensionUniqueId;

    public ExtensionTokenProcessor(String extensionUniqueId) {
        this.extensionUniqueId = extensionUniqueId;
    }

    public String getExtensionUniqueId() {
        return extensionUniqueId;
    }

    /**
     * ENCRYPTION
     *
     * Create a two-way encrypted access token for given principal for this extension
     * @return token generated from principal
     */
    public PrincipalIdentifierToken generateToken(Principal principal) {
        String token = principal.getName() + ":" + extensionUniqueId;
        return new PrincipalIdentifierToken(token);
    }

    /**
     * DECRYPTION
     *
     * Convert token to Principal
     * @return Principal
     */
    public Principal extractPrincipal(PrincipalIdentifierToken token) {
        String[] parts = token.getToken().split(":");

        if (parts.length != 2) {
            return null;
        }

        return new Principal(parts[0]);
    }

}
