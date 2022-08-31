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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.UUID;

/**
 * Stores principal information about owners of incoming/outgoing requests to/from extensions
 */
public class ExtensionPrincipalsRegistry {
    private final Map<UUID, Principal> extensionRequestPrincipals;
    private final Logger logger = LogManager.getLogger(ExtensionPrincipalsRegistry.class);

    public ExtensionPrincipalsRegistry(Map<UUID, Principal> extensionRequestPrincipals) {
        this.extensionRequestPrincipals = extensionRequestPrincipals;
    }

    /**
     * TODO: should it return an immutable copy? i.e. Immutable.copyOf(extensionRequestPrincipals)
     * @return the current principal store
     */
    public Map<UUID, Principal> getExtensionRequestPrincipals() {
        return this.extensionRequestPrincipals;
    }

    public void addPrincipal(Principal principal){
        UUID key = UUID.randomUUID();
        while(extensionRequestPrincipals.containsKey(key))
            key = UUID.randomUUID();
        this.extensionRequestPrincipals.put(key, principal);
        logger.info("Successfully stored principal=" + principal);
    }

    public Principal getPrincipalById(UUID uuid){
        if(this.extensionRequestPrincipals.containsKey(uuid))
            return this.extensionRequestPrincipals.get(uuid);
        return null;
    }

    public boolean removePrincipalFromStore(UUID uuid){
        Principal deletedPrincipal = (Principal) this.extensionRequestPrincipals.remove(uuid);
        if (deletedPrincipal == null){
            logger.info("Unsuccessful: Invalid key uuid= " + uuid);
            return false;
        }
        logger.info("Successful: Deleted principal= " + deletedPrincipal);
        return true;
    }

}
