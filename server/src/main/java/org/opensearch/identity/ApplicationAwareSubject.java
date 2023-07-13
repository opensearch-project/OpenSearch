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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.identity.scopes.ApplicationScope;
import org.opensearch.identity.scopes.Scope;
import org.opensearch.identity.tokens.AuthToken;

/**
 * This class defines an ApplicationAwareSubject.
 *
 * An ApplicatioAwarenSubject is a type of Subject which wraps another basic Subject object.
 * With an ApplicationAwareSubject, we represent a system that interacts with the Identity access control system.
 *
 * @opensearch.experimental
 */
@SuppressWarnings("overrides")
public class ApplicationAwareSubject implements Subject {

    private final Subject wrapped;
    private final ExtensionsManager extensionsManager;

    /**
     * We wrap a basic Subject object to create an ApplicationAwareSubject -- this should come from the IdentityService
     * @param wrapped The Subject to be wrapped
     */
    public ApplicationAwareSubject(final Subject wrapped, ExtensionsManager extensionsManager) {
        this.wrapped = wrapped;
        this.extensionsManager = extensionsManager;
    }

    /**
     * Use the ApplicationManager to get the scopes associated with the principal of the wrapped Subject.
     * Because the wrapped subject is just a basic Subject, it may not know its own scopes. This circumvents this issue.
     * @return A set of Strings representing the scopes associated with the wrapped subject's principal
     */
    public Set<Scope> getScopes() {
        return extensionsManager.getScopes(wrapped.getPrincipal());
    }

    /**
     * Checks scopes of an application subject and determine if it is allowed to perform an operation based on the given scopes
     * @param scopes The scopes to check against the subject
     * @return true if allowed, false if none of the scopes are allowed.
     */
    public boolean isAllowed(final List<Scope> scopes) {

        final Optional<Principal> optionalPrincipal = this.getApplication();

        if (optionalPrincipal.isEmpty()) {
            // If there is no application, actions are allowed by default

            return true;
        }

        if (!extensionsManager.applicationExists(this.getPrincipal())) {

            return false;
        }

        final Set<Scope> scopesOfApplication = this.getScopes();

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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ApplicationAwareSubject)) {
            return false;
        }
        ApplicationAwareSubject other = (ApplicationAwareSubject) obj;
        return Objects.equals(this.getScopes(), other.getScopes());
    }
}
