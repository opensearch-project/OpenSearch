/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.identity.scopes.Scope;
import org.opensearch.identity.tokens.AuthToken;

import java.security.Principal;
import java.util.Set;
import java.util.List;
import java.util.Optional;

/**
 * An individual, process, or device that causes information to flow among objects or change to the system state.
 *
 * @opensearch.experimental
 */
public class ApplicationAwareSubject implements Subject {

    public boolean checkApplicationExists(Principal principal) {
        return (ExtensionsManager.getExtensionManager().getExtensionPrincipals().contains(principal));
    }

    private final Subject wrapped;

    public ApplicationAwareSubject(final Subject wrapped) {
        this.wrapped = wrapped;
    }

    /**
     * This wrapped call allows for testing
     * @param principal The principal of the target application
     * @return The set of all of the application's scopes
     */
    public Set<String> checkApplicationScopes(Principal principal) {
        return Scope.getApplicationScopes(principal);
    }

    // Passthroughs for wrapped subject
    public Principal getPrincipal() {
        return wrapped.getPrincipal();
    }

    public void authenticate(final AuthToken token) {
        wrapped.authenticate(token);
    }

    public Optional<Principal> getApplication() {
        return wrapped.getApplication();
    }
    // end Passthroughs for wrapped subject

    /**
     * Checks scopes of the current subject if they are allowed for any of the listed scopes
     * @param scopes The scopes to check against the subject
     * @return true if allowed, false if none of the scopes are allowed.
     */
    public boolean isAllowed(final List<Scope> scopes) {
        System.out.println("At top of is allowed with scopes: " + scopes);
        final Optional<Principal> optionalPrincipal = wrapped.getApplication();

        if (optionalPrincipal.isEmpty()) {
            System.out.println("Returning true because no application");
            // If there is no application, actions are allowed by default
            return true;
        }

        final Principal appPrincipal = optionalPrincipal.get();

        if (!this.checkApplicationExists(appPrincipal)) {
            System.out.println("Returning false because application does not exist");
            return false;
        }

        final Set<String> scopesOfApplication = checkApplicationScopes(appPrincipal);

        if (scopesOfApplication.stream()
            .map(Scope::parseScopeFromString)
            .anyMatch(parsedScope -> parsedScope.equals(ApplicationScope.SuperUserAccess))) {
            // Applications that are fully trusted automatically pass all checks
            System.out.println("Returning true because superuser");
            return true;
        }

        if (scopesOfApplication.stream()
            .map(Scope::parseScopeFromString)
            .anyMatch(parsedScope -> scopes.stream().anyMatch(parsedScope::equals))) { // Find any matches between scope list and subject's
            System.out.println("Returning true because matching scope");                                            // scopes
            return true;
        }

        System.out.println("Returning false because no matching scopes");
        return false;
    }
}
