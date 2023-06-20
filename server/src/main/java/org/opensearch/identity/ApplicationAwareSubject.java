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
import java.util.function.Function;

/**
 * An individual, process, or device that causes information to flow among objects or change to the system state.
 *
 * @opensearch.experimental
 */
public final class ApplicationAwareSubject implements Subject {

    private final Function<Principal, Boolean> checkApplicationExists() {

        return new Function<Principal, Boolean>() {
            @Override
            public Boolean apply(Principal principal) {

                return (ExtensionsManager.getExtensionManager().getExtensionPrincipals().contains(principal));
            }
        };
    }

    private final Subject wrapped;

    ApplicationAwareSubject(final Subject wrapped) {
        this.wrapped = wrapped;
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
     * @param scope The scopes to check against the subject
     * @return true if allowed, false if none of the scopes are allowed.
     */
    public boolean isAllowed(final List<Scope> scope) {
        final Optional<Principal> appPrincipal = wrapped.getApplication();
        if (appPrincipal.isEmpty()) {
            // If there is no application, actions are permitted by default
            return true;
        }

        if (!this.checkApplicationExists().apply(appPrincipal.get())) {
            return false;
        }

        final Set<String> scopesOfApplication = Scope.getApplicationScopes(appPrincipal.get());

        if (scopesOfApplication.stream()
            .map(Scope::parseScopeFromString)
            .anyMatch(parsedScope -> parsedScope.equals(ApplicationScope.SuperUserAccess))) {
            // Applications that are fully trusted automatically pass all checks
            return true;
        }

        // TODO: Decide how to handle resolution of scopes here.
        // For now returning FALSE as application is not SuperUser
        return false;
    }
}
