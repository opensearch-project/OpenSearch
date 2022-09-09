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
 * An identifier class to identify the owner for rest call to/from extensions
 */
public class ExtensionIdentifier {
    // TODO: This should be fetched from Shiro and will replace entityName
    // private Principal principal;
    private String entityName;

    // DiscoveryExtension.ID
    private String extensionUniqueId;

    public ExtensionIdentifier(String entityName, String extensionUniqueId) {
        this.entityName = entityName;
        this.extensionUniqueId = extensionUniqueId;
    }

    public String getEntityName() {
        return this.entityName;
    }

    public String getExtensionUniqueId() {
        return extensionUniqueId;
    }

    /**
     * ENCRYPTION
     *
     * Convert entityName to identifier
     * @return identifier generated from string
     */
    // TODO: re-write this method with some form of encryption
    public String generateToken() {
        return entityName + ":" + extensionUniqueId;
    }

    /**
     * DECRYPTION
     *
     * Convert identifier to entityName
     * @return String
     */
    // TODO: re-write this method with some form of decryption
    public String decryptToken() {
        return entityName + ":" + extensionUniqueId;
    }

    // TODO: is toString() required?
    @Override
    public String toString() {
        return "ExtensionIdentifier{" + "entityName='" + entityName + '\'' + ", extensionUniqueId='" + extensionUniqueId + '\'' + '}';
    }
}
