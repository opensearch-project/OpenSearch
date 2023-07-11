/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import java.security.Principal;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.identity.scopes.Scope;

/**
 * The ApplicationManager class handles the processing and resolution of multiple types of applications. Using the class, OpenSearch can
 * continue to resolve requests even when specific application types are disabled. For example, the ExtensionManager can be Noop in which case
 * the ApplicationManager is able to resolve requests for other application types still
 *
 * @opensearch.experimental
 */
public class ApplicationManager {

    AtomicReference<ExtensionsManager> extensionManager;
    public static ApplicationManager instance; // Required for access in static contexts

    public ApplicationManager() {
        instance = this;
        extensionManager = new AtomicReference<>();
    }

    public void register(ExtensionsManager manager) {
        if (this.extensionManager == null) {
            this.extensionManager = new AtomicReference<>(manager);
        } else {
            this.extensionManager.set(manager);
        }
    }

    public Set<Scope> getScopes(Principal principal) {
        if (this.extensionManager != null) {
            if (this.extensionManager.get().getExtensionIdMap().containsKey(principal.getName())) {
                return extensionManager.get().getExtensionIdMap().get(principal.getName()).getScopes();
            }
        }
        return Set.of();
    }

    /** Checks whether there is an application associated with the given principal or not
     * @param principal The principal for the application you are trying to find
     * @return Whether the application exists (TRUE) or not (FALSE)
     * */
    public boolean applicationExists(Principal principal) {
        return (this.extensionManager.get().getExtensionIdMap().containsKey(principal.getName()));
    }
}
