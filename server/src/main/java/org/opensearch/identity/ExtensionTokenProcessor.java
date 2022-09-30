/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import java.security.Principal;

/**
 * Token processor class to handle token encryption/decryption
 * This processor is will be instantiated for every extension
 */
public class ExtensionTokenProcessor {
    public static final String INVALID_TOKEN_MESSAGE = "Token must not be null and must be a colon-separated String";
    public static final String INVALID_EXTENSION_MESSAGE = "Token passed here is for a different extension";

    private final String extensionUniqueId;

    public ExtensionTokenProcessor(String extensionUniqueId) {
        this.extensionUniqueId = extensionUniqueId;
    }

    public String getExtensionUniqueId() {
        return extensionUniqueId;
    }

    /**
     * Create a two-way encrypted access token for given principal for this extension
     * @return token generated from principal
     */
    public PrincipalIdentifierToken generateToken(Principal principal) {
        // This is a placeholder implementation
        // More concrete implementation will be covered in https://github.com/opensearch-project/OpenSearch/issues/4485
        String token = principal.getName() + ":" + extensionUniqueId;

        return new PrincipalIdentifierToken(token);
    }

    /**
     * Decrypt the token and extract Principal
     * @param token the requester identity token, should not be null
     * @return Principal
     *
     * @opensearch.internal
     *
     * This method contains a placeholder implementation.
     * More concrete implementation will be covered in https://github.com/opensearch-project/OpenSearch/issues/4485
     */
    public Principal extractPrincipal(PrincipalIdentifierToken token) throws IllegalArgumentException {
        // check is token is valid, we don't do anything if it is valid
        // else we re-throw the thrown exception
        validateToken(token);

        String[] parts = token.getToken().split(":");
        final String principalName = parts[0];
        return () -> principalName;
    }

    /**
     * Checks validity of the requester identifier token
     * @param token The requester identifier token
     * @throws IllegalArgumentException when token is invalid
     *
     * This method contains a placeholder implementation.
     * More concrete implementation will be covered in https://github.com/opensearch-project/OpenSearch/issues/4485
     */
    public void validateToken(PrincipalIdentifierToken token) throws IllegalArgumentException {

        if (token == null || token.getToken() == null) {
            throw new IllegalArgumentException(INVALID_TOKEN_MESSAGE);
        }

        String[] parts = token.getToken().split(":");

        // check whether token is malformed
        if (parts.length != 2) {
            throw new IllegalArgumentException(INVALID_TOKEN_MESSAGE);
        }
        // check whether token is for this extension
        if (!parts[1].equals(extensionUniqueId)) {
            throw new IllegalArgumentException(INVALID_EXTENSION_MESSAGE);
        }
    }
}
