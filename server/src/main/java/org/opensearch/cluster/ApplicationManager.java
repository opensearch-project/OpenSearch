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
import java.util.stream.Collectors;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.identity.scopes.Scope;
import org.opensearch.identity.scopes.ScopeEnums;

/**
 * The ApplicationManager class handles the processing and resolution of multiple types of applications. Using the class, OpenSearch can
 * continue to resolve requests even when specific application types are disabled. For example, the ExtensionManager can be Noop in which case
 * the ApplicationManager is able to resolve requests for other application types still
 *
 * @opensearch.experimental
 */
public class ApplicationManager {

    ExtensionsManager extensionManager;
    public static ApplicationManager instance; // Required for access in static contexts

    public ApplicationManager(ExtensionsManager extensionsManager) {
        this.extensionManager = extensionsManager;
        instance = this;
    }

    public static ApplicationManager getInstance() {
        return instance;
    }

    /**
     * This method allows for checking the ApplicationScopes associated with an application
     * @param principal The principal of the application whose scopes you want to check
     * @return A set of all application scopes associated with that principal
     */
    public Set<String> getApplicationScopes(Principal principal) {

        if (extensionManager.getExtensionPrincipals().contains(principal)) {
            return extensionManager.getExtensionIdMap()
                .get(principal.getName())
                .getScopes()
                .stream()
                .filter(scope -> Scope.parseScopeFromString(scope).getNamespace() == ScopeEnums.ScopeNamespace.APPLICATION)
                .collect(Collectors.toSet());
        }

        return Set.of();
    }

    /**
     * This method allows for checking the ActionScopes associated with an application
     * @param principal The principal of the application whose scopes you want to check
     * @return A set of all action scopes associated with that principal
     */
    Set<String> getActionScopes(Principal principal) {

        if (extensionManager.getExtensionPrincipals().contains(principal)) {
            return extensionManager.getExtensionIdMap()
                .get(principal.getName())
                .getScopes()
                .stream()
                .filter(scope -> Scope.parseScopeFromString(scope).getNamespace() == ScopeEnums.ScopeNamespace.ACTION)
                .collect(Collectors.toSet());
        }

        return Set.of();
    }

    /**
     * This method allows for checking the ExtensionPointScopes associated with an application
     * @param principal The principal of the application whose scopes you want to check
     * @return A set of all extension point scopes associated with that principal
     */
    Set<String> getExtensionPointScopes(Principal principal) {

        if (extensionManager.getExtensionPrincipals().contains(principal)) {
            return extensionManager.getExtensionIdMap()
                .get(principal.getName())
                .getScopes()
                .stream()
                .filter(scope -> Scope.parseScopeFromString(scope).getNamespace() == ScopeEnums.ScopeNamespace.EXTENSION_POINT)
                .collect(Collectors.toSet());
        }

        return Set.of();
    }

    /**
     * Checks whether there is an application associated with the given principal or not
     * @param principal The principal for the application you are trying to find
     * @return Whether the application exists (TRUE) or not (FALSE)
     */
    public boolean associatedApplicationExists(Principal principal) {
        return (extensionManager.getExtensionPrincipals().contains(principal));
    }

    public ExtensionsManager getExtensionManager() {
        return extensionManager;
    }
}
