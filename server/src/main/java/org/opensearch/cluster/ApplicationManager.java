/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import java.security.Principal;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.identity.ApplicationAwareSubject;
import org.opensearch.identity.scopes.ApplicationScope;
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

    public static ApplicationManager getInstance() {
        if (instance == null) {
            new ApplicationManager();
        }
        return instance;
    }

    /**
     * Checks scopes of an application subject and determine if it is allowed to perform an operation based on the given scopes
     * @param subject The ApplicationSubject whose scopes should be evaluated
     * @param scopes The scopes to check against the subject
     * @return true if allowed, false if none of the scopes are allowed.
     */
    public boolean isAllowed(ApplicationAwareSubject subject, final List<Scope> scopes) {

        final Optional<Principal> optionalPrincipal = subject.getApplication();

        if (optionalPrincipal.isEmpty()) {
            // If there is no application, actions are allowed by default

            return true;
        }

        if (!subject.applicationExists()) {

            return false;
        }

        final Set<Scope> scopesOfApplication = subject.getScopes();

        boolean isApplicationSuperUser = scopesOfApplication.contains(ApplicationScope.SUPER_USER_ACCESS);

        if (isApplicationSuperUser) {

            return true;
        }

        Set<Scope> intersection = new HashSet<>(scopesOfApplication);

        // Retain only the elements present in the list
        intersection.retainAll(scopes);

        boolean isMatchingScopePresent = !intersection.isEmpty();

        return isMatchingScopePresent;
    }

    public Set<Scope> getScopes(Principal principal) {
        if (this.extensionManager != null) {
            if (this.extensionManager.get().getExtensionIdMap().containsKey(principal.getName())) {
                return extensionManager.get().getExtensionIdMap().get(principal.getName()).getScopes();
            }
        }
        return Set.of();
    }

    /**
     * Checks whether there is an application associated with the given principal or not
     * @param principal The principal for the application you are trying to find
     * @return Whether the application exists (TRUE) or not (FALSE)
     */
    public boolean associatedApplicationExists(Principal principal) {
        return (this.extensionManager.get().getExtensionIdMap().containsKey(principal.getName()));
    }

    /**
     * Allows for checking the ExtensionManager being used by the ApplicationManager in the case that there are multiple application providers
     * @return The ExtensionManager being queried by the ApplicationManager
     */
    public ExtensionsManager getExtensionManager() {
        return extensionManager.get();
    }
}
