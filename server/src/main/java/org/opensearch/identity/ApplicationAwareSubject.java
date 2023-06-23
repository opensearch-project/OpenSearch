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

import java.security.Principal;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.opensearch.cluster.ApplicationManager;
import org.opensearch.identity.scopes.ApplicationScope;
import org.opensearch.identity.scopes.Scope;
import org.opensearch.identity.tokens.AuthToken;

/**
 *
 * @opensearch.experimental
 */
public class ApplicationAwareSubject implements Subject {

    public boolean checkApplicationExists(Principal principal) {
        return (ApplicationManager.getInstance().associatedApplicationExists(principal));
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
        return ApplicationManager.getInstance().getApplicationScopes(principal);
    }

    // Passthroughs for wrapped subject
    public Principal getPrincipal() {
        return wrapped.getPrincipal();
    }

    public void authenticate(final AuthToken token) {
        wrapped.authenticate(token);
    }

    public Optional<Principal> getApplication() {
        System.out.println("Returning optional in ApplicationAwareSubject");
        return wrapped.getApplication();
    }
    // end Passthroughs for wrapped subject

    /**
     * Checks scopes of the current subject if they are allowed for any of the listed scopes
     * @param scopes The scopes to check against the subject
     * @return true if allowed, false if none of the scopes are allowed.
     */
    public boolean isAllowed(final List<Scope> scopes) {
        System.out.println("At top of isAllowed");
        final Optional<Principal> optionalPrincipal = wrapped.getApplication();
        System.out.println("Optional of principal is: " + optionalPrincipal);
        if (optionalPrincipal.isEmpty()) {

            System.out.println("Optional is empty so returning false");
            // If there is no application, actions are allowed by default
            return true;
        }

        final Principal appPrincipal = optionalPrincipal.get();

        System.out.println("Principal from optional is: " + appPrincipal);
        if (!this.checkApplicationExists(appPrincipal)) {

            System.out.println("Application does not exist so returning false");
            return false;
        }

        final Set<String> scopesOfApplication = checkApplicationScopes(appPrincipal);

        System.out.println("Scopes of application are: " + scopesOfApplication);
        boolean isApplicationSuperUser = scopesOfApplication.stream()
            .map(Scope::parseScopeFromString)
            .anyMatch(parsedScope -> parsedScope.equals(ApplicationScope.SUPER_USER_ACCESS));

        if (isApplicationSuperUser) {

            System.out.println("Application is superuser so returning true");
            return true;
        }

        boolean isMatchingScopePresent = scopesOfApplication.stream()
            .map(Scope::parseScopeFromString)
            .anyMatch(parsedScope -> scopes.stream().anyMatch(parsedScope::equals));

        if (isMatchingScopePresent) {

            System.out.println("Application has match so returning true ");
            return true;
        }

        System.out.println("Application does not have matching scope so returning false");
        return false;
    }
}
