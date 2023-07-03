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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.opensearch.cluster.ApplicationManager;
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

    /**
     * We wrap a basic Subject object to create an ApplicationAwareSubject -- this should come from the IdentityService
     * @param wrapped The Subject to be wrapped
     */
    public ApplicationAwareSubject(final Subject wrapped) {
        this.wrapped = wrapped;
    }

    /**
     * Call to the ApplicationManager to confirm there is an associated application matching the wrapped subject's principal.
     * @return There is an associated application known to the ApplicationManager (TRUE) or there is not (FALSE)
     */
    public boolean applicationExists() {
        return (ApplicationManager.getInstance().associatedApplicationExists(wrapped.getPrincipal()));
    }

    /**
     * Use the ApplicationManager to get the scopes associated with the principal of the wrapped Subject.
     * Because the wrapped subject is just a basic Subject, it may not know its own scopes. This circumvents this issue.
     * @return A set of Strings representing the scopes associated with the wrapped subject's principal
     */
    public Set<Scope> getScopes() {
        return ApplicationManager.getInstance().getScopes(wrapped.getPrincipal());
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
